"""Stage 1 — triage: peek every zip / AiiDA export, decide what to fetch, emit fetch units.

The Materials Cloud census (discover) hands over EVERY public record; triage turns that into a
keep-list with the evidence policy chosen for this harvest (user decision 2026-09-23):

* **Evidence.** Each ``.zip`` and ``.aiida`` file is peeked — its central directory read over HTTP
  Range (ZIP64-aware, :mod:`remote_zip`) — to list the VASP outputs inside without downloading; a
  sqlite_zip AiiDA archive (names only in its database) is resolved by pulling its ``db.sqlite3``
  over Range and listing the nodes that hold VASP outputs. Tar-family archives cannot be peeked
  (no index; compressed streams are non-seekable).
* **Keep rule.** A record whose own text mentions VASP (``vasp_mention``) is kept FAIL-SAFE, as on
  Zenodo: dropped only when every archive was peeked successfully and none holds a VASP primary or
  a nested archive. Any OTHER record is kept on POSITIVE evidence (a peek found a
  ``vasprun``/``OUTCAR``/``vaspout`` member — ``vasp_evidence``) or, per ``unresolved_policy``,
  when an archive could not be settled by a peek at all (``unresolved_fetch``): the policy was
  ``none`` in the first census and is ``all`` since 2026-09-24 (user decision, after the CSD3
  census): where zips could be peeked, 7.8% of records that never mention VASP held VASP outputs,
  so the ~1 TB of tars in such records is fetched too — fetch keeps only VASP files, and S3 serves
  CSD3 at ~50-100 MB/s.
* **File pruning** (fetch touches only what can yield VASP): a zip — or AiiDA archive — proven to
  hold none is dropped; a **legacy-format AiiDA export** with VASP members is marked
  ``archive_kind="zip"`` (its members keep real names, so the shared zip path — targeted member
  fetch included — handles it); a **sqlite_zip** one, and any AiiDA archive a peek could not
  resolve (a legacy TAR export, an unreadable or too-big one), is marked ``archive_kind="aiida"``
  for the shared AiiDA extractor. ``file_allowlist`` restricts a record to named files — by
  default the Bosoni ACWF verification record keeps only its two ``*_results_vasp.aiida`` exports
  (user decision).
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
from .remote_zip import DEFAULT_MAX_CD_BYTES, DEFAULT_MAX_DB_BYTES, peek_archive

logger = logging.getLogger(__name__)

# Per-record file allowlists (fnmatch globs on the file key). The Bosoni et al. ACWF verification
# record ships 20 AiiDA exports for 10 codes (75.5 GB); only its two VASP exports are harvested.
DEFAULT_FILE_ALLOWLIST: dict[str, tuple[str, ...]] = {
    "yf0rj-w3r97": ("*_results_vasp.aiida",),
}
UNIT_SEP = "~"
# Bump when the evidence rules (remote_zip.zip_evidence / aiida_format) change, so cached peek
# verdicts computed under the old rules are re-evaluated instead of trusted.
# v3 (2026-09-24): ZIP64-locator + truncated-count fixes, sqlite_zip database evidence, nested
# AiiDA archives counted as sub-archives.
EVIDENCE_RULES_VERSION = 3
# Peek outcomes that are NOT a property of the file and must be retried rather than cached.
_UNCACHED_STATUSES = ("peek_failed", "cd_too_large", "db_too_large")
UNRESOLVED_POLICIES = ("all", "dft", "none")
_DFT_WORDS = re.compile(r"\b(dft|density[- ]functional|first[- ]principles|ab[- ]initio|pbe|"
                        r"hse06?|paw|projector[- ]augmented)\b", re.IGNORECASE)


def _is_aiida(key: str) -> bool:
    return key.lower().endswith(AIIDA_EXT)


def _peekable(f: dict[str, Any]) -> bool:
    return f.get("ext") == ".zip" or _is_aiida(str(f.get("key", "")))


# A single compressed DATA file (``optimade.jsonl.gz``, ``alexandria_ps_000.json.bz2``,
# ``traj.xyz.gz``): the shared fetch's bare-compression heuristic tries such names as misnamed
# tarballs (right for Zenodo's ``…-vasp-raw.gz``), but a known data extension under the compression
# says it is not one — so triage does not fetch it as an archive (CSD3 census: ~10 GB of them in
# records that would otherwise be blind-fetched, incl. an 8.5 GB OPTIMADE dump).
_BARE_COMPRESSION = (".gz", ".bz2", ".xz", ".zst")
_DATA_EXTS = (".json", ".jsonl", ".xyz", ".extxyz", ".csv", ".tsv", ".txt", ".dat", ".lmp",
              ".lammps", ".dump", ".npy", ".npz", ".pkl", ".pickle", ".h5", ".hdf5", ".cif",
              ".pdb", ".log", ".out", ".xml", ".yaml", ".yml", ".html", ".md")


def _is_compressed_data_file(base: str) -> bool:
    low = base.lower()
    if not low.endswith(_BARE_COMPRESSION):
        return False
    stem = low.rsplit(".", 1)[0]
    # a compressed VASP file (``vasprun.xml.gz``, ``OUTCAR.xz``) is a VASP file, never "data"
    return stem.endswith(_DATA_EXTS) and not stem.endswith(".tar") and not _PARSE_RE.search(stem)


def _fetch_kind(f: dict[str, Any]) -> str | None:
    """The archive kind the shared fetch will use for this file (declared or sniffed), or None
    for a plain file — including a single compressed data file (see :data:`_DATA_EXTS`)."""
    declared = f.get("archive_kind")
    if declared:
        return str(declared)
    base = str(f.get("key", "")).rsplit("/", 1)[-1]
    kind = _is_archive(base)
    if kind in ("tar", "tarzst") and _is_compressed_data_file(base):
        return None
    return kind


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
    # (a compressed data file is left out: the shared fetch would sniff it as a tarball)
    direct = [f for f in files if not _fetch_kind(f)
              and not _is_compressed_data_file(str(f.get("key", "")).rsplit("/", 1)[-1])]
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


def _dft_worded(cand: dict[str, Any]) -> bool:
    """The ``dft`` unresolved policy's filter: DFT words in the record's metadata — discover's
    ``metadata_signals`` (scanned over title + description + keywords) or PBE/HSE-style terms in
    the title/keywords — and no other DFT code named."""
    text = " ".join([str(cand.get("title") or ""), " ".join(map(str, cand.get("keywords") or []))])
    worded = bool(cand.get("metadata_signals")) or bool(_DFT_WORDS.search(text))
    return worded and not cand.get("other_codes")


def _decide(cand: dict[str, Any], files: list[dict[str, Any]], ev: dict[str, dict[str, Any]],
            unresolved_policy: str = "all"
            ) -> tuple[bool, str, list[dict[str, Any]], list[str]]:
    """``(keep, reason, files_to_fetch, gaps)`` for one record (see the module docstring).

    Positive evidence = something the shared fetch would turn into a calc unit: a peeked archive
    member (for a sqlite_zip AiiDA archive: a database node) or a loose file whose name seeds a
    unit (``fetch._unit_role`` vasprun/vaspout/outcar). Loose INPUT files (POSCAR/INCAR/KPOINTS…)
    are not evidence of anything parseable. An archive whose contents no peek can settle (a tar, an
    unreadable zip/AiiDA archive, one holding sub-archives) is *unresolved*: fetched for a
    VASP-mentioning record, and for any other record as ``unresolved_policy`` says (``all`` — the
    2026-09-24 decision —, ``dft`` = DFT-worded records naming no other code, ``none``)."""
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
        # the shared aiida extractor handles every AiiDA shape (legacy zip/tar, sqlite_zip)
        as_aiida = {**f, "archive_kind": "aiida"}
        if status != "ok":
            # an unreadable zip, or an AiiDA archive that is not a zip (a legacy TAR export) / too
            # big to peek / whose database could not be read: only a download can tell
            gaps.append(f"{key}: {'aiida archive' if aiida else 'zip'} {status}")
            unresolved = True
            fetch_files.append(as_aiida if aiida else f)
            continue
        fmt = e.get("aiida_format")
        if fmt == "sqlite_zip" and e.get("db_status") != "ok":
            gaps.append(f"{key}: sqlite_zip AiiDA archive, database not inspected")
            unresolved = True
            fetch_files.append(as_aiida)
            continue
        has_vasp = e.get("n_primary", 0) > 0
        n_inner = int(e.get("n_nested", 0) or 0) + int(e.get("n_nested_aiida", 0) or 0)
        if n_inner and not has_vasp:
            gaps.append(f"{key}: {n_inner} nested archive(s), contents invisible to a peek")
        if has_vasp or n_inner:
            positive = positive or has_vasp
            unresolved = unresolved or (n_inner > 0 and not has_vasp)
            if fmt == "sqlite_zip":
                fetch_files.append(as_aiida)            # names only via its database
            elif aiida:
                fetch_files.append({**f, "archive_kind": "zip"})   # legacy: real member names
            else:
                fetch_files.append(f)
        # else: every member (for sqlite_zip: every database node) seen, no VASP output and no
        # sub-archive -> proven empty, pruned
    fetchable = any(_fetch_kind(f) or _is_loose_primary(f) for f in fetch_files)
    if cand.get("vasp_mention"):
        if not (positive or unresolved):
            return False, "peek_proved_no_vasp", [], gaps
        if not fetchable:
            return False, "evidence_gap_only", [], gaps
        return True, "vasp_mention", fetch_files, gaps
    if positive and fetchable:           # positive evidence always leaves a fetchable file
        return True, "vasp_evidence", fetch_files, gaps
    if unresolved and fetchable and (unresolved_policy == "all" or (
            unresolved_policy == "dft" and _dft_worded(cand))):
        return True, "unresolved_fetch", fetch_files, gaps
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
           peek_workers: int = 4,
           unresolved_policy: str = "all",
           max_db_bytes: int = DEFAULT_MAX_DB_BYTES) -> dict[str, Any]:
    """Peek, decide and write the fetch-unit keep-list + a census report (see module docs).

    Peeks are REQUEST-bound (1-3 small Range reads, each through MC's ~0.3-2 s API redirect hop),
    so ``peek_workers`` run them concurrently (a thread each, its own session unless ``session``
    is given), with request starts paced by ``interval`` across all threads. Decisions depend only
    on per-file evidence, so the keep-list is identical whatever the concurrency. A sqlite_zip AiiDA
    archive's peek also pulls its database (<= ``max_db_bytes``; 0 = never) to list its VASP calcs.
    ``unresolved_policy`` says which records WITHOUT a VASP mention or positive evidence still get
    their unresolvable archives fetched (see :func:`_decide`)."""
    if unresolved_policy not in UNRESOLVED_POLICIES:
        raise ValueError(f"unresolved_policy must be one of {UNRESOLVED_POLICIES}")
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
            evidence, status = peek_archive(_sess(), f["download"], max_cd_bytes, max_db_bytes)
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
            keep, reason, fetch_files, gaps = _decide(cand, files, ev, unresolved_policy)
            stats[f"decision:{reason}"] += 1
            if not keep and not cand.get("vasp_mention"):
                # the residual blind spot under a narrower unresolved_policy: unpeekable
                # (tar-family) archives in records that never mention VASP, not downloaded
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
                     "bytes_to_fetch": 0,
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
            # what fetch will really download: the units' files (a loose non-VASP file that
            # belongs to no unit is never fetched)
            entry["bytes_to_fetch"] = sum(int(f.get("size") or 0) for u in units
                                          for f in u.get("files") or [])
            for u in units:
                keep_w.write(json.dumps(u) + "\n")
            stats["kept_records"] += 1
            stats["units"] += len(units)
            stats["bytes_to_fetch"] += entry["bytes_to_fetch"]
            if reason == "unresolved_fetch":
                stats["blind_units"] += len(units)
                stats["blind_bytes"] += entry["bytes_to_fetch"]
            entry["n_units"] = len(units)
            per_record.append(entry)

    kept_entries = [e for e in per_record if e["keep"]]
    summary = {
        "in": str(in_path), "out": str(out), "report": str(report),
        "unresolved_policy": unresolved_policy,
        "records": stats["records"], "below_min_rank": stats["below_min_rank"],
        "kept_records": stats["kept_records"], "fetch_units": stats["units"],
        "bytes_to_fetch": stats["bytes_to_fetch"],
        "peeked": stats["peeked"], "peeks_cached": stats["peeks_cached"],
        "decisions": {k.split(":", 1)[1]: v for k, v in stats.items() if k.startswith("decision:")},
        "records_with_gaps": stats["records_with_gaps"],
        # records that never mention VASP but whose peek FOUND VASP outputs
        "blind_spot_recovered": [e["recid"] for e in kept_entries if e["reason"] == "vasp_evidence"],
        # records fetched only because their archives cannot be peeked (unresolved_policy)
        "blind_fetch": {"records": stats["decision:unresolved_fetch"],
                        "units": stats["blind_units"], "bytes": stats["blind_bytes"]},
        "unpeekable_skipped": {"files": stats["unpeekable_skipped_files"],
                               "bytes": stats["unpeekable_skipped_bytes"]},
    }
    report.write_text(json.dumps({"summary": summary, "records": per_record}, indent=1))
    logger.info("mc triage: %s", summary)
    return summary
