"""Stage 1 — triage: peek every zip / AiiDA export, decide what to fetch, emit fetch units.

The Materials Cloud census (discover) hands over EVERY public record; triage turns that into a
keep-list with the evidence policy chosen for this harvest (user decision 2026-09-23):

* **Evidence.** Each ``.zip`` and ``.aiida`` file is peeked — its central directory read over HTTP
  Range (ZIP64-aware, :mod:`remote_zip`) — to list the VASP outputs inside without downloading.
  Tar-family archives cannot be peeked (no index; compressed streams are non-seekable).
* **Keep rule.** A record whose own text mentions VASP (``vasp_mention``) is kept FAIL-SAFE, as on
  Zenodo: dropped only when every archive was peeked successfully and none holds a VASP primary or
  a nested archive. Any OTHER record is kept only on POSITIVE evidence — a peek found a
  ``vasprun``/``OUTCAR``/``vaspout`` member — so VASP hidden in records that never say "VASP" is
  recovered while the ~1 TB of unpeekable tars in (overwhelmingly QE) records is not downloaded
  blind.
* **File pruning** (fetch touches only what can yield VASP): a zip proven to hold none is dropped;
  a **legacy-format AiiDA export** with VASP members is kept and marked ``archive_kind="zip"`` (the
  shared fetch then extracts it like any zip — its members keep real names); a sqlite_zip export
  (names hidden in ``db.sqlite3``) or an unreadable ``.aiida`` is dropped from the fetch list and
  reported as an evidence gap (the CSD3 probe inspects those). ``file_allowlist`` restricts a
  record to named files — by default the Bosoni ACWF verification record keeps only its two
  ``*_results_vasp.aiida`` exports (user decision).
* **Fetch units.** A kept record is split into one keep-list entry per archive (``recid =
  <record_id>~<key-derived tag>``, :func:`unit_id` — derived from the archive KEY, never its
  position, so a re-run of triage that keeps a different set of archives cannot renumber a unit
  onto another unit's terminal rejection); directly-exposed VASP outputs form one ``~loose`` unit.
  Each archive is extracted into its own subdir anyway, so calc units and calc_ids are IDENTICAL
  to a whole-record fetch (the calc_id uses ``provenance.record_id``, not ``recid``) — but the disk
  valve now paces per archive, so a 44 GB, 10-archive AIMD record never has to fit the staging
  budget at once.
* **Evidence = fetch's own rules.** A member counts as a VASP output iff the shared fetch would
  extract it AND seed a calc unit from it (``fetch._PARSE_RE`` + ``fetch._unit_role``), so triage
  can never prune an archive fetch would have parsed (``OUTCAR1``/``vasprun_1.xml`` included).

Peek results are cached in ``<out>.peeks.jsonl`` (keyed by record, file, size and checksum) so a
re-run — e.g. after a wallclock kill — does not repeat completed central-directory reads.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests

from zenodo_harvest.fetch import (
    _PARSE_RE,
    _PRIMARY_ROLES,
    _archive_subdir,
    _is_archive,
    _is_junk_member,
    _unit_role,
)
from zenodo_harvest.manifest import JsonlWriter, RejectionLogger, read_jsonl

from .client import new_session
from .records import AIIDA_EXT, classify_mc_files
from .remote_zip import DEFAULT_MAX_CD_BYTES, peek_archive

logger = logging.getLogger(__name__)

# Per-record file allowlists (fnmatch globs on the file key). The Bosoni et al. ACWF verification
# record ships 20 AiiDA exports for 10 codes (75.5 GB); only its two VASP exports are harvested.
DEFAULT_FILE_ALLOWLIST: dict[str, tuple[str, ...]] = {
    "yf0rj-w3r97": ("*_results_vasp.aiida",),
}
UNIT_SEP = "~"
# Bump when the evidence rules (remote_zip.zip_evidence / aiida_format) change, so cached peek
# verdicts computed under the old rules are re-evaluated instead of trusted.
EVIDENCE_RULES_VERSION = 2
# Peek outcomes that are NOT a property of the file and must be retried rather than cached.
_UNCACHED_STATUSES = ("peek_failed", "cd_too_large")


def _is_aiida(key: str) -> bool:
    return key.lower().endswith(AIIDA_EXT)


def _peekable(f: dict[str, Any]) -> bool:
    return f.get("ext") == ".zip" or _is_aiida(str(f.get("key", "")))


def _fetch_kind(f: dict[str, Any]) -> str | None:
    """The archive kind the shared fetch will use for this file (declared or sniffed)."""
    return f.get("archive_kind") or _is_archive(str(f.get("key", "")).rsplit("/", 1)[-1])


def _peek_key(recid: str, f: dict[str, Any]) -> str:
    return (f"v{EVIDENCE_RULES_VERSION}\t{recid}\t{f.get('key')}\t{f.get('size')}"
            f"\t{f.get('checksum')}")


def _is_loose_primary(f: dict[str, Any]) -> bool:
    """A directly-exposed file the shared fetch would download AND seed a calc unit from."""
    key = str(f.get("key", ""))
    base = key.rsplit("/", 1)[-1]
    return (not _is_junk_member(key) and bool(_PARSE_RE.search(base))
            and _unit_role(base) in _PRIMARY_ROLES)


def unit_id(record_id: str, key: str) -> str:
    """A fetch-unit id derived from the archive's own KEY (not its position), so re-running triage
    with a different surviving-archive set never renumbers units — a renumbered id would inherit
    another unit's terminal rejection and its archive would never be fetched. Filesystem-safe (it
    becomes the unit's staging dir) and ``:``-free (rejection ids use ``<recid>:<key>``)."""
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", key)[:40].strip("._-") or "file"
    return f"{record_id}{UNIT_SEP}{safe}-{hashlib.sha1(key.encode('utf-8')).hexdigest()[:8]}"


def _load_peek_cache(path: Path) -> dict[str, dict[str, Any]]:
    cache: dict[str, dict[str, Any]] = {}
    if path.is_file():
        for row in read_jsonl(path):
            if row.get("k"):
                cache[row["k"]] = row["v"]
    return cache


def split_units(cand: dict[str, Any], files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Split a kept record into fetch units: one per archive (id from the archive key, see
    :func:`unit_id`), plus one ``~loose`` unit for directly-exposed VASP outputs (with the other
    loose files beside them). Loose files without a VASP output are left out of the units: they
    extract to ``extracted/`` beside no calc, so they can neither form nor annotate a calc unit.
    Deterministic and stable across re-runs whatever else triage keeps or prunes."""
    rid = cand["recid"]
    archives = [f for f in files if _fetch_kind(f)]
    direct = [f for f in files if not _fetch_kind(f)]
    groups: list[tuple[str, list[dict[str, Any]]]] = [(unit_id(rid, str(a["key"])), [a])
                                                      for a in archives]
    if any(_is_loose_primary(f) for f in direct):
        groups.append((f"{rid}{UNIT_SEP}loose", direct))
    units = []
    for uid, grp in groups:
        u = dict(cand)
        u["recid"] = uid
        u["files"] = grp
        u["files_total"] = len(grp)
        u["bytes_total"] = sum(int(f.get("size") or 0) for f in grp)
        u["unit"] = {"record_id": rid, "files": [str(f["key"]) for f in grp], "of": len(groups)}
        units.append(u)
    return units


def _decide(cand: dict[str, Any], files: list[dict[str, Any]],
            ev: dict[str, dict[str, Any]]) -> tuple[bool, str, list[dict[str, Any]], list[str]]:
    """``(keep, reason, files_to_fetch, gaps)`` for one record (see the module docstring).

    Positive evidence = something the shared fetch would turn into a calc unit: a peeked archive
    member or a loose file whose name seeds a unit (``fetch._unit_role`` vasprun/vaspout/outcar).
    Loose INPUT files (POSCAR/INCAR/KPOINTS…) are not evidence of anything parseable."""
    positive = False
    unresolved = False
    gaps: list[str] = []
    fetch_files: list[dict[str, Any]] = []
    subdirs: dict[str, str] = {}
    for f in files:
        key = str(f["key"])
        base = key.rsplit("/", 1)[-1]
        aiida = _is_aiida(key)
        if not (aiida or _fetch_kind(f)):
            fetch_files.append(f)        # direct file: VASP-named -> downloaded, else availability
            positive = positive or _is_loose_primary(f)
            continue
        sub = _archive_subdir(base)
        if sub in subdirs:               # never seen on MC (census: 0), but it would merge calcs
            gaps.append(f"{key}: extracts into the same subdir {sub!r} as {subdirs[sub]!r} "
                        "(their calc_ids could collide)")
        subdirs.setdefault(sub, key)
        if not _peekable(f):
            unresolved = True            # tar-family: only a download can tell
            fetch_files.append(f)
            continue
        e = ev.get(key) or {}
        status = e.get("status", "not_peeked")
        if status != "ok":
            gaps.append(f"{key}: {'aiida export' if aiida else 'zip'} {status}")
            unresolved = True
            if not aiida:
                fetch_files.append(f)    # an unreadable zip: fetch confirms it at download
            continue                     # an unreadable .aiida is unusable to fetch
        has_vasp = e.get("n_primary", 0) > 0
        has_nested = e.get("n_nested", 0) > 0
        if has_nested and not has_vasp:
            gaps.append(f"{key}: {e.get('n_nested')} nested archive(s), contents invisible")
        if e.get("n_nested_aiida"):
            gaps.append(f"{key}: {e.get('n_nested_aiida')} AiiDA export(s) inside, not extracted")
        if aiida and e.get("aiida_format") == "sqlite_zip":
            gaps.append(f"{key}: aiida sqlite_zip export (names only in db.sqlite3)")
            unresolved = True
            continue
        if has_vasp or has_nested:
            positive = positive or has_vasp
            unresolved = unresolved or (has_nested and not has_vasp)
            # a legacy (or plain-zip) .aiida keeps real member names -> extract it as a zip
            fetch_files.append({**f, "archive_kind": "zip"} if aiida else f)
        # else: names visible, no VASP output, no sub-archive -> proven empty, pruned
    fetchable = any(_fetch_kind(f) or _is_loose_primary(f) for f in fetch_files)
    if cand.get("vasp_mention"):
        if not (positive or unresolved):
            return False, "peek_proved_no_vasp", [], gaps
        if not fetchable:                # e.g. its only VASP-candidates are sqlite_zip exports
            return False, "evidence_gap_only", [], gaps
        return True, "vasp_mention", fetch_files, gaps
    if positive and fetchable:           # positive evidence always leaves a fetchable file
        return True, "vasp_evidence", fetch_files, gaps
    return False, ("evidence_gap_only" if gaps else "no_vasp_evidence"), [], gaps


class _Pacer:
    """Spaces request STARTS across peek threads (thread-safe): at most one peek begins every
    ``interval`` seconds, which keeps several parallel peeks well under MC's 500 req/60 s."""

    def __init__(self, interval: float):
        self.interval = interval
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            time.sleep(start - now)


def _prepare(cand: dict[str, Any], allow: Mapping[str, Iterable[str]]) -> dict[str, Any]:
    """Apply the per-record file allowlist (and re-rank the restricted listing)."""
    globs = tuple(allow.get(cand["recid"]) or ())
    if not globs:
        return cand
    files = [f for f in cand.get("files") or []
             if any(fnmatch.fnmatch(f["key"], g) for g in globs)]
    fc = classify_mc_files(files)
    return {**cand, "files": files, "vasp_category": fc["category"], "vasp_rank": fc["rank"],
            "archives": fc["archives"],
            "signals": [*cand.get("signals", []), f"file allowlist {list(globs)}"]}


def triage(in_path: str | Path, out_path: str | Path, *,
           report_path: str | Path | None = None,
           rejections_path: str | Path | None = None,
           session: requests.Session | None = None,
           min_rank: int = 3, peek: bool = True,
           max_cd_bytes: int = DEFAULT_MAX_CD_BYTES,
           file_allowlist: Mapping[str, Iterable[str]] | None = None,
           split: bool = True, interval: float = 0.4,
           max_records: int | None = None,
           peek_retry_wait: float = 5.0,
           peek_workers: int = 4) -> dict[str, Any]:
    """Peek, decide and write the fetch-unit keep-list + a census report (see module docs).

    Peeks are REQUEST-bound (1-3 small Range reads, each through MC's ~0.3-2 s API redirect hop),
    so ``peek_workers`` run them concurrently (a thread each, its own session unless ``session``
    is given), with request starts paced by ``interval`` across all threads. Decisions depend only
    on per-file evidence, so the keep-list is identical whatever the concurrency."""
    in_path, out = Path(in_path), Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    report = Path(report_path) if report_path else out.with_name(out.stem + ".report.json")
    rej_path = Path(rejections_path) if rejections_path else out.parent / "mc_rejections.jsonl"
    allow = DEFAULT_FILE_ALLOWLIST if file_allowlist is None else {
        k: tuple(v) for k, v in file_allowlist.items()}
    cache_path = Path(str(out) + ".peeks.jsonl")
    cache = _load_peek_cache(cache_path)
    stats: Counter = Counter()

    # -- phase 1: candidates (allowlist + rank gate) and the peeks they still need ----------
    cands: list[dict[str, Any]] = []
    for n, cand in enumerate(read_jsonl(in_path)):
        if max_records and n >= max_records:
            break
        stats["records"] += 1
        cand = _prepare(cand, allow)
        if int(cand.get("vasp_rank", 0)) < min_rank:
            stats["below_min_rank"] += 1
            continue
        cands.append(cand)
    todo: dict[str, dict[str, Any]] = {}          # cache key -> file (deduplicated)
    if peek:
        for cand in cands:
            for f in cand.get("files") or []:
                if _peekable(f):
                    ck = _peek_key(cand["recid"], f)
                    if ck in cache:
                        stats["peeks_cached"] += 1
                    else:
                        todo.setdefault(ck, f)

    # -- phase 2: run the uncached peeks concurrently, caching file-intrinsic verdicts -------
    pacer = _Pacer(interval)
    tls = threading.local()
    results: dict[str, dict[str, Any]] = {}

    def _sess() -> requests.Session:
        if session is not None:
            return session
        s = getattr(tls, "s", None)
        if s is None:
            s = tls.s = new_session()
        return s  # type: ignore[no-any-return]

    def _peek(f: dict[str, Any]) -> tuple[dict[str, Any], int]:
        attempts = 0
        status, evidence = "peek_failed: not attempted", None
        for attempt in range(2):                 # one in-run retry of a failed peek
            if attempt:
                time.sleep(peek_retry_wait)
            pacer.wait()
            evidence, status = peek_archive(_sess(), f["download"], max_cd_bytes)
            attempts += 1
            if not status.startswith("peek_failed"):
                break
        return {"status": status, **(evidence.to_dict() if evidence else {})}, attempts

    if todo:
        logger.info("mc triage: %d archive(s) to peek (%d cached) with %d worker(s)",
                    len(todo), stats["peeks_cached"], peek_workers)
    with JsonlWriter(cache_path) as cache_w:
        with ThreadPoolExecutor(max_workers=max(1, peek_workers)) as pool:
            futs = {pool.submit(_peek, f): ck for ck, f in todo.items()}
            for i, fut in enumerate(as_completed(futs), 1):
                ck = futs[fut]
                row, attempts = fut.result()
                stats["peeked"] += attempts
                results[ck] = row
                # cache only verdicts that are a property of the FILE: a transient failure, or a
                # cap-dependent refusal, must be re-attempted on the next run
                if not row["status"].startswith(_UNCACHED_STATUSES):
                    cache_w.write({"k": ck, "v": row})
                if i % 100 == 0:
                    logger.info("mc triage: peeked %d/%d archives", i, len(todo))
    cache.update(results)

    # -- phase 3: decisions (sequential, deterministic order) --------------------------------
    per_record: list[dict[str, Any]] = []
    with RejectionLogger(rej_path) as rej, out.open("w") as keep_w:
        for cand in cands:
            rid = cand["recid"]
            files = list(cand.get("files") or [])
            ev = {f["key"]: cache[_peek_key(rid, f)] for f in files
                  if _peekable(f) and _peek_key(rid, f) in cache}
            keep, reason, fetch_files, gaps = _decide(cand, files, ev)
            stats[f"decision:{reason}"] += 1
            if not keep and not cand.get("vasp_mention"):
                # the deliberate blind spot of the evidence policy: unpeekable (tar-family)
                # archives in records that never mention VASP are not downloaded to check
                unpk = [f for f in files if _fetch_kind(f) and not _peekable(f)]
                stats["unpeekable_skipped_files"] += len(unpk)
                stats["unpeekable_skipped_bytes"] += sum(int(f.get("size") or 0) for f in unpk)
            if gaps:
                stats["records_with_gaps"] += 1
            prim = sorted({p for e in ev.values() for p in e.get("primary_sample", [])})
            entry = {"recid": rid, "title": str(cand.get("title", ""))[:100],
                     "vasp_mention": bool(cand.get("vasp_mention")), "keep": keep,
                     "reason": reason, "gaps": gaps,
                     "bytes_total": cand.get("bytes_total"),
                     "bytes_to_fetch": sum(int(f.get("size") or 0) for f in fetch_files),
                     "files": [{"key": f["key"], "size": f.get("size"),
                                "peek": ev.get(f["key"])} for f in files if _peekable(f)
                               or _fetch_kind(f)]}
            if not keep:
                per_record.append(entry)
                rej.reject("mc_triage", rid, reason, vasp_mention=bool(cand.get("vasp_mention")),
                           gaps=gaps or None, title=str(cand.get("title", ""))[:100])
                continue
            kept = {**cand, "files": fetch_files, "triage_reason": reason,
                    "peek": {k: v for k, v in ev.items()}, "evidence_gaps": gaps}
            if prim:
                kept["primary_vasp_files"] = prim[:20]
                kept["vasp_category"], kept["vasp_rank"] = "vasp_direct", 4
                kept["signals"] = [*kept.get("signals", []),
                                   f"peek confirmed VASP outputs ({len(prim)}+ shown)"]
            units = split_units(kept, fetch_files) if split else [kept]
            for u in units:
                keep_w.write(json.dumps(u) + "\n")
            stats["kept_records"] += 1
            stats["units"] += len(units)
            stats["bytes_to_fetch"] += entry["bytes_to_fetch"]
            entry["n_units"] = len(units)
            per_record.append(entry)

    kept_entries = [e for e in per_record if e["keep"]]
    summary = {
        "in": str(in_path), "out": str(out), "report": str(report),
        "records": stats["records"], "below_min_rank": stats["below_min_rank"],
        "kept_records": stats["kept_records"], "fetch_units": stats["units"],
        "bytes_to_fetch": stats["bytes_to_fetch"],
        "peeked": stats["peeked"], "peeks_cached": stats["peeks_cached"],
        "decisions": {k.split(":", 1)[1]: v for k, v in stats.items() if k.startswith("decision:")},
        "records_with_gaps": stats["records_with_gaps"],
        "blind_spot_recovered": [e["recid"] for e in kept_entries if not e["vasp_mention"]],
        "unpeekable_skipped": {"files": stats["unpeekable_skipped_files"],
                               "bytes": stats["unpeekable_skipped_bytes"]},
    }
    report.write_text(json.dumps({"summary": summary, "records": per_record}, indent=1))
    logger.info("mc triage: %s", summary)
    return summary
