"""Stage 1' — census triage: peek the plausible records' archives, decide, emit a keep-list.

Which records are looked at (user decisions 2026-09-25):

* every ``T1`` (strong) and ``T2`` (plausible) record;
* a random sample of ``T3`` (low-signal) records — not software — to MEASURE the residual blind
  spot: if hidden VASP turns up at roughly 1 in 2,000 or better, a second run evaluates the whole
  non-software T3 tier (``tiers=("T3",)``);
* a small random sample of ``T0`` (another domain) records, to check the negative filter.

How an archive is read: a zip (and an AiiDA export) by its central directory over HTTP Range —
ZIP64-aware, the Materials Cloud reader (``materials_cloud_harvest.remote_zip``), since big VASP
zips are routinely ZIP64 (e.g. ``13888307``: 19.6 GB, 31,007 members) and the Zenodo triage peek
cannot read those; a tar-family stream by its first ~8 MB (:mod:`zenodo_census.headpeek`); rar/7z
not at all. Every request — both kinds of peek, their internal reads and retries — passes one
pacer shared by the worker threads, so the whole run stays under Zenodo's authenticated limits
(100/min and 5,000/h documented; 133/min seen in the headers).

What is kept (the user's policy): a record with POSITIVE evidence — a peek that lists a VASP
primary the shared fetch would parse, or a head-peek that shows VASP-named files, or a loose VASP
primary; plus, for ``T1`` records only, archives no peek could settle (fail-safe, as the keyword
harvest did for its VASP-mentioning records). Proven-empty archives are pruned from the fetch
list. A kept record whose licence the dataset does not admit (NoDerivatives / none) goes to
``licence_review.jsonl`` for a case-by-case decision instead of the keep-list.

Output: an ORDINARY Zenodo keep-list (``Candidate`` dicts, one per record, files pruned) for
``zenodo_harvest.cli pipeline`` — same fetch, parse, calc_ids (``zenodo:<recid>:<path>``) and
schema as the existing dataset — plus the review list, a report and a rejections log.
"""

from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import requests

from materials_cloud_harvest.remote_zip import DEFAULT_MAX_CD_BYTES, peek_archive
from materials_cloud_harvest.triage import _is_compressed_data_file
from zenodo_harvest.fetch import _ZenodoOnlyBearer, _is_archive, _is_junk_member
from zenodo_harvest.manifest import JsonlWriter, RejectionLogger, read_jsonl
from zenodo_harvest.models import Candidate

from .census import _records, iter_census, truncate_torn_tail
from .headpeek import DEFAULT_HEAD_BYTES, head_peek
from .score import PROBE_RECIDS, is_github_snapshot
from .signals import is_loose_primary

logger = logging.getLogger(__name__)

# Bump when the evidence rules change, so cached verdicts from older rules are re-evaluated.
EVIDENCE_RULES_VERSION = 2          # v2: strict head names, end-block completeness, fallbacks
_UNCACHED = ("peek_failed", "cd_too_large", "db_too_large", "head_too_small")
# Zenodo documents 100 req/min + 5,000 req/h for authenticated clients: 0.8 s between request
# starts is 75/min and 4,500/h.
DEFAULT_INTERVAL = 0.8
DEFAULT_RESIDUAL_SAMPLE = 3000
DEFAULT_NEGATIVE_SAMPLE = 300
RESIDUAL_EXCLUDED_TYPES = ("software",)
USER_AGENT = "zenodo-harvest/0.1 (census-triage)"


class Pacer:
    """At most one request START every ``interval`` seconds across all threads."""

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


class PacedSession(requests.Session):
    """A session whose every request waits on the shared :class:`Pacer`; the Zenodo token (if any)
    is attached only to zenodo.org hosts (the shared fetch's ``_ZenodoOnlyBearer``)."""

    def __init__(self, pacer: Pacer, token: str | None = None):
        super().__init__()
        self.pacer = pacer
        self.headers["User-Agent"] = USER_AGENT
        if token:
            self.auth = _ZenodoOnlyBearer(token)

    def request(self, *args: Any, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.pacer.wait()
        return super().request(*args, **kwargs)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    """``(rate, lo, hi)`` — the Wilson 95% interval of k successes in n trials."""
    if n <= 0:
        return 0.0, 0.0, 0.0
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return p, max(0.0, c - h), min(1.0, c + h)


def _mode(f: dict[str, Any]) -> str | None:
    """How a file is inspected: ``zip`` (central directory), ``head`` (stream head), ``none``
    (rar/7z — unpeekable), or None for a plain file."""
    base = str(f.get("key") or "").rsplit("/", 1)[-1]
    if _is_junk_member(base):
        return None
    if base.lower().endswith(".aiida"):
        return "zip"
    if base.lower().endswith(".tbz"):         # a bzip2 tarball the shared fetch does not sniff
        return "head"
    kind = _is_archive(base)
    if kind == "zip":
        return "zip"
    if kind in ("tar", "tarzst"):
        return None if _is_compressed_data_file(base) else "head"
    if kind in ("rar", "sevenzip"):
        return "none"
    return None


def _cache_key(mode: str, recid: str, f: dict[str, Any], head_bytes: int = 0) -> str:
    """Peek-cache key: rules version, mode, record, file identity — and, for a head-peek, the
    head size (a bigger head can see further, so a small-head verdict must not be reused)."""
    hb = f"\th{head_bytes}" if mode == "head" else ""
    return (f"v{EVIDENCE_RULES_VERSION}\t{mode}\t{recid}\t{f.get('key')}\t{f.get('size')}"
            f"\t{f.get('checksum')}{hb}")


# The shared fetch extractor for a sniffed stream (tarfile auto-detects gzip/bzip2/xz; zstd needs
# the dedicated extractor).
_HEAD_KIND_TO_FETCH = {"tar": "tar", "gzip": "tar", "bzip2": "tar", "xz": "tar", "zstd": "tarzst"}


def peek_file(session: requests.Session, mode: str, f: dict[str, Any],
              head_bytes: int = DEFAULT_HEAD_BYTES,
              max_cd_bytes: int = DEFAULT_MAX_CD_BYTES) -> dict[str, Any]:
    """Peek one file: ``{"mode", "status", **evidence}``, plus ``as_kind`` when the content is a
    different container than the name says (a zip named ``.tar.gz``, a tar named ``.zip``, zstd
    named ``.tar.xz`` — all seen on Materials Cloud): the fetch picks its extractor from the name,
    so the keep-list must then declare the real one."""
    url = (f.get("links") or {}).get("self") or f.get("download")
    if not url:
        return {"mode": mode, "status": "no_download_link"}
    base = str(f.get("key") or "").rsplit("/", 1)[-1]
    by_name = "zip" if base.lower().endswith(".aiida") else _is_archive(base)
    if mode == "head":
        ev, status = head_peek(session, url, str(f.get("key") or ""), head_bytes=head_bytes)
        if status in ("is_7z", "is_rar"):
            # another container in disguise: unpeekable, but the fetch must use its real extractor
            return {"mode": "head", "status": status,
                    "as_kind": "sevenzip" if status == "is_7z" else "rar"}
        if status != "is_zip":
            out = {"mode": "head", "status": status, **(ev.evidence() if ev else {})}
            real = _HEAD_KIND_TO_FETCH.get(str(out.get("kind")))
            if real and real != by_name:
                out["as_kind"] = real
            return out
    # the listing's size makes the tail read an ordinary range (Zenodo breaks suffix ranges
    # longer than the file — every zip under the 1 MiB tail would otherwise fail to peek)
    zev, status = peek_archive(session, url, max_cd_bytes=max_cd_bytes,
                               size=int(f.get("size") or 0) or None)
    aiida_name = base.lower().endswith(".aiida")
    if status == "not_zip" and mode == "zip" and not aiida_name:
        ev, hstatus = head_peek(session, url, str(f.get("key") or ""), head_bytes=head_bytes)
        if hstatus.startswith("peek_failed") or hstatus == "head_too_small":
            return {"mode": "zip", "status": hstatus}           # transient: never cached
        if hstatus in ("is_7z", "is_rar"):
            return {"mode": "head", "status": hstatus,
                    "as_kind": "sevenzip" if hstatus == "is_7z" else "rar"}
        if ev is not None:
            out = {"mode": "head", "status": hstatus, **ev.evidence()}
            if ev.kind != "single":
                out["as_kind"] = _HEAD_KIND_TO_FETCH.get(ev.kind, "tar")
            return out
    if zev is not None and not aiida_name and (
            status.startswith("peek_failed: db_failed") or status == "db_too_large"):
        # a plain zip that merely contains a file called db.sqlite3: judge it by its member names
        out = {"mode": "zip", "status": "ok", **zev.to_dict()}
        out["aiida_format"] = None
        out.pop("db_status", None)
        return out
    out = {"mode": "zip", "status": status, **(zev.to_dict() if zev else {})}
    if by_name != "zip" and status == "ok":
        out["as_kind"] = "zip"
    return out


def file_verdict(f: dict[str, Any], e: dict[str, Any] | None
                 ) -> tuple[str, dict[str, Any] | None]:
    """``(state, file_to_fetch)``: state ``vasp`` (positive evidence), ``empty`` (proven: pruned),
    ``unresolved`` (no peek could settle it) or ``loose`` (a plain file). The fetch file carries
    ``archive_kind`` when the peek found a different container than the name implies."""
    mode = _mode(f)
    key = str(f.get("key") or "")
    if mode is None:
        base = key.rsplit("/", 1)[-1]
        return ("empty", None) if _is_compressed_data_file(base) and _is_archive(base) else (
            "loose", f)
    if mode == "none":
        return "unresolved", f
    aiida = key.lower().endswith(".aiida")
    if not e or e.get("status") != "ok":
        if e and e.get("as_kind"):
            return "unresolved", {**f, "archive_kind": e["as_kind"]}
        return "unresolved", ({**f, "archive_kind": "aiida"} if aiida else f)
    as_fetch = {**f, "archive_kind": e["as_kind"]} if e.get("as_kind") else f
    if e.get("mode") == "zip":
        fmt = e.get("aiida_format")
        if fmt == "sqlite_zip":
            # names live only in its database, whatever the file is called: the AiiDA extractor
            as_fetch = {**f, "archive_kind": "aiida"}
        elif aiida:
            as_fetch = {**f, "archive_kind": "zip"}
        if fmt == "sqlite_zip" and e.get("db_status") != "ok":
            return "unresolved", {**f, "archive_kind": "aiida"}
        if int(e.get("n_primary") or 0) > 0:
            return "vasp", as_fetch
        if int(e.get("n_nested") or 0) + int(e.get("n_nested_aiida") or 0) > 0:
            return "unresolved", as_fetch
        return "empty", None
    # head-peek of a tar-family stream
    if e.get("kind") == "single":
        # one compressed FILE under a non-VASP name (a VASP name never reaches a head-peek):
        # the shared fetch would try it as a tarball and fail, so there is nothing to fetch
        return "empty", None
    n_prim = int(e.get("n_primary") or 0)
    if e.get("complete"):
        if n_prim > 0:
            return "vasp", as_fetch
        return ("unresolved", as_fetch) if int(e.get("n_nested") or 0) else ("empty", None)
    if n_prim or int(e.get("n_vasp_named") or 0) or int(e.get("n_heavy") or 0):
        return "vasp", as_fetch
    return "unresolved", as_fetch


def evidence_strength(files: list[dict[str, Any]], ev: dict[str, dict[str, Any]]) -> str:
    """``primary`` when a VASP output was actually seen (a peeked primary or a loose one), else
    ``hint`` (only VASP-named inputs / heavy files in a partial head)."""
    if any(is_loose_primary(f) for f in files):
        return "primary"
    return "primary" if any(int(e.get("n_primary") or 0) > 0 for e in ev.values()
                            if e.get("status") == "ok") else "hint"


def decide(tier: str, files: list[dict[str, Any]], ev: dict[str, dict[str, Any]]
           ) -> tuple[bool, str, list[dict[str, Any]], dict[str, int]]:
    """``(keep, reason, files_to_fetch, bytes)`` for one record (see the module docstring)."""
    states: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
    for f in files:
        st, ff = file_verdict(f, ev.get(str(f.get("key"))))
        states.append((st, ff, f))
    loose_vasp = any(st == "loose" and is_loose_primary(f) for st, _, f in states)
    positive = loose_vasp or any(st == "vasp" for st, _, _ in states)
    unresolved = any(st == "unresolved" for st, _, _ in states)
    fetch = [ff for st, ff, _ in states if ff is not None and st in ("vasp", "unresolved", "loose")]
    nbytes = {"evidence": sum(int(f.get("size") or 0) for st, _, f in states if st == "vasp"),
              "blind": sum(int(f.get("size") or 0) for st, _, f in states if st == "unresolved")}
    fetchable = any(st in ("vasp", "unresolved") for st, _, _ in states) or loose_vasp
    if positive and fetchable:
        return True, "vasp_evidence", fetch, nbytes
    if unresolved and tier == "T1":
        return True, "strong_unresolved", fetch, nbytes
    if unresolved:
        return False, "unresolved_not_fetched", [], nbytes
    return False, "proved_no_vasp", [], nbytes


def _select(scored_path: str | Path, tiers: Iterable[str], types: Iterable[str] | None,
            residual_sample: int, negative_sample: int, seed: int,
            include_github_residual: bool = False) -> dict[str, dict[str, Any]]:
    """``recid -> scored row`` (+ ``selected_as``) for the records this run evaluates."""
    tiers = set(tiers)
    types = set(types) if types else None
    chosen: dict[str, dict[str, Any]] = {}
    t3: list[dict[str, Any]] = []
    t0: list[dict[str, Any]] = []
    def residual_ok(row: dict[str, Any]) -> bool:
        """A T3 record the residual measurement covers: the given types, else everything but
        software (user decision: GitHub snapshots are not brute-forced)."""
        if types is not None:
            return row.get("resource_type") in types
        return row.get("resource_type") not in RESIDUAL_EXCLUDED_TYPES and (
            include_github_residual or not is_github_snapshot(row))

    for row in read_jsonl(scored_path):
        tier = row.get("tier")
        if tier == "X":
            continue
        if tier in tiers and (tier in ("T1", "T2", "T0") or residual_ok(row)):
            chosen[row["recid"]] = {**row, "selected_as": "tier"}
            continue
        if tier == "T3" and residual_ok(row):
            t3.append(row)
        elif tier == "T0":
            t0.append(row)
    rng = random.Random(seed)
    for pool, n, label in ((t3, residual_sample, "residual_sample"),
                           (t0, negative_sample, "negative_sample")):
        pool = [r for r in pool if r["recid"] not in chosen]
        pool.sort(key=lambda r: r["recid"])
        for r in rng.sample(pool, min(n, len(pool))):
            chosen[r["recid"]] = {**r, "selected_as": label}
    return chosen


def keep_entry(rec: dict[str, Any], row: dict[str, Any], files: list[dict[str, Any]],
               reason: str, ev: dict[str, dict[str, Any]], gaps: list[str]) -> dict[str, Any]:
    """A Zenodo keep-list line: the Candidate the keyword harvest would have written for this
    record (same provenance fields), its file list pruned to what can yield VASP, plus the census
    bookkeeping (tier, signals, peek evidence)."""
    cand = Candidate.from_record(rec).to_dict()
    by_key = {str(f.get("key")): f for f in files}
    cand_files = []
    for cf in cand["files"]:
        src = by_key.get(str(cf.get("key")))
        if src is None:
            continue
        if src.get("archive_kind"):
            cf = {**cf, "archive_kind": src["archive_kind"]}
        cand_files.append(cf)
    cand["files"] = cand_files
    cand["files_total"] = len(cand_files)
    cand["bytes_total"] = sum(int(f.get("size") or 0) for f in cand_files)
    prim = sorted({p for k, e in ev.items() if k in by_key for p in e.get("primary_sample") or []})
    if prim:
        cand["primary_vasp_files"] = prim[:20]
        cand["vasp_category"], cand["vasp_rank"] = "vasp_direct", 4
    cand["query"] = "census"
    cand["census"] = {"tier": row.get("tier"), "reasons": row.get("reasons"),
                      "selected_as": row.get("selected_as"), "triage_reason": reason,
                      "licence_class": row.get("licence_class"), "evidence_gaps": gaps,
                      "peek": {k: {kk: vv for kk, vv in v.items()
                                   if kk in ("mode", "status", "n_members", "n_primary",
                                             "n_vasp_named", "n_heavy", "n_nested",
                                             "complete", "kind", "aiida_format")}
                               for k, v in ev.items()}}
    return cand


def triage(scored_path: str | Path, census_path: str | Path, out_path: str | Path, *,
           tiers: Iterable[str] = ("T1", "T2"), types: Iterable[str] | None = None,
           residual_sample: int = DEFAULT_RESIDUAL_SAMPLE,
           negative_sample: int = DEFAULT_NEGATIVE_SAMPLE, seed: int = 20260925,
           token: str | None = None, session_factory: Any = None,
           interval: float = DEFAULT_INTERVAL, peek_workers: int = 3,
           head_bytes: int = DEFAULT_HEAD_BYTES, max_cd_bytes: int = DEFAULT_MAX_CD_BYTES,
           review_path: str | Path | None = None, report_path: str | Path | None = None,
           rejections_path: str | Path | None = None, cache_path: str | Path | None = None,
           max_records: int | None = None, include_github_residual: bool = False,
           exclude_keep: Iterable[str | Path] = ()) -> dict[str, Any]:
    """Peek, decide, and write the keep-list + licence-review list + report (module docstring).

    ``tiers`` are evaluated in full (T3 restricted to ``types``); the residual/negative samples are
    drawn from what is left (seeded, so a re-run draws the same records). Records already in an
    earlier run's keep-list (``exclude_keep``) are skipped, so a later full-T3 run never re-fetches
    the residual sample. Peek verdicts are cached in ONE file beside the keep-list
    (``peeks.jsonl``, keyed by record/file/size/checksum), shared by every run, so a killed run —
    or the next one — never repeats a completed read."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    review = Path(review_path) if review_path else out.with_name(
        out.stem + ".licence_review.jsonl")
    report = Path(report_path) if report_path else out.with_name(out.stem + ".report.json")
    rej_path = Path(rejections_path) if rejections_path else out.with_name(
        out.stem + ".rejections.jsonl")
    cache_file = Path(cache_path) if cache_path else out.with_name("peeks.jsonl")

    chosen = _select(scored_path, tiers, types, residual_sample, negative_sample, seed,
                     include_github_residual)
    already: set[str] = set()
    for kp in exclude_keep:
        if Path(kp).is_file() and Path(kp).resolve() != out.resolve():
            for r in read_jsonl(kp):
                already.add(str(r.get("recid")))
                if r.get("conceptrecid"):
                    already.add(str(r["conceptrecid"]))
    if already:     # by recid OR concept: a newer version of a kept record is skipped too
        chosen = {r: v for r, v in chosen.items()
                  if r not in already and str(v.get("conceptrecid") or r) not in already}
    if max_records:
        keep_ids = sorted(chosen, key=lambda r: ({"T1": 0, "T2": 1}.get(chosen[r]["tier"], 2), r))
        chosen = {r: chosen[r] for r in keep_ids[:max_records]}
    recs: dict[str, dict[str, Any]] = {}
    for rec in iter_census(census_path):
        rid = str(rec.get("id"))
        if rid in chosen:
            recs[rid] = rec
    missing = sorted(set(chosen) - set(recs))
    if missing:
        logger.warning("census triage: %d selected records missing from the census", len(missing))

    cache: dict[str, dict[str, Any]] = {}
    truncate_torn_tail(cache_file)
    if cache_file.is_file():
        for row in _records(cache_file):
            cache[row["k"]] = row["v"]
    order = sorted(recs, key=lambda r: ({"T1": 0, "T2": 1, "T3": 2, "T0": 3}.get(
        chosen[r]["tier"], 4), int(r)))
    todo: dict[str, tuple[str, dict[str, Any]]] = {}
    stats: Counter = Counter()
    for rid in order:
        for f in recs[rid].get("files") or []:
            m = _mode(f)
            if m in ("zip", "head"):
                ck = _cache_key(m, rid, f, head_bytes)
                if ck in cache:
                    stats["peeks_cached"] += 1
                else:
                    todo.setdefault(ck, (m, f))

    pacer = Pacer(interval)
    tls = threading.local()

    def sess() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = tls.s = (session_factory() if session_factory else PacedSession(pacer, token))
        return s  # type: ignore[no-any-return]

    def safe_peek(mode: str, f: dict[str, Any]) -> dict[str, Any]:
        try:
            return peek_file(sess(), mode, f, head_bytes, max_cd_bytes)
        except Exception as exc:  # noqa: BLE001 - one bad file must never stop the run
            return {"mode": mode, "status": f"peek_failed: {type(exc).__name__}: "
                                            f"{str(exc)[:120]}"}

    def run(item: tuple[str, dict[str, Any]]) -> dict[str, Any]:
        mode, f = item
        v = safe_peek(mode, f)
        if str(v.get("status", "")).startswith("peek_failed"):
            time.sleep(5)                       # one in-run retry of a transient failure
            v = safe_peek(mode, f)
        return v

    logger.info("census triage: %d records selected, %d files to peek (%d cached)",
                len(recs), len(todo), stats["peeks_cached"])
    t0 = time.monotonic()
    with JsonlWriter(cache_file) as cw, ThreadPoolExecutor(max_workers=max(1, peek_workers)) as pool:
        futs = {pool.submit(run, item): ck for ck, item in todo.items()}
        for i, fut in enumerate(as_completed(futs), 1):
            ck = futs[fut]
            v = fut.result()
            cache[ck] = v
            stats[f"peek_{v.get('mode')}_{str(v.get('status')).split(':', 1)[0]}"] += 1
            if not str(v.get("status", "")).startswith(_UNCACHED):
                cw.write({"k": ck, "v": v})
            if i % 500 == 0:
                rate = i / max(1e-9, time.monotonic() - t0) * 3600
                logger.info("census triage: peeked %d/%d files (%.0f/h)", i, len(todo), rate)

    rej_path.unlink(missing_ok=True)          # regenerated whole, like the keep-list
    per_record: list[dict[str, Any]] = []
    kept_by: Counter = Counter()
    samples: dict[str, Counter] = {"residual_sample": Counter(), "negative_sample": Counter()}
    positives_by_reason: Counter = Counter()
    with RejectionLogger(rej_path) as rej, out.open("w") as kw, review.open("w") as rw:
        for rid in order:
            rec, row = recs[rid], chosen[rid]
            files = list(rec.get("files") or [])
            ev = {str(f.get("key")): cache[_cache_key(m, rid, f, head_bytes)] for f in files
                  if (m := _mode(f)) in ("zip", "head")
                  and _cache_key(m, rid, f, head_bytes) in cache}
            keep, reason, fetch, nbytes = decide(str(row["tier"]), files, ev)
            gaps = [f"{k}: {v.get('mode')} {v.get('status')}" for k, v in ev.items()
                    if v.get("status") != "ok"]
            sel = str(row.get("selected_as"))
            stats[f"decision:{row['tier']}:{reason}"] += 1
            strength = evidence_strength(files, ev) if reason == "vasp_evidence" else None
            if sel in samples:
                samples[sel]["n"] += 1
                samples[sel]["positive"] += reason == "vasp_evidence"
                samples[sel]["positive_primary"] += strength == "primary"
            entry = {"recid": rid, "tier": row["tier"], "selected_as": sel,
                     "resource_type": row.get("resource_type"), "title": row.get("title"),
                     "reasons": row.get("reasons"), "licence_class": row.get("licence_class"),
                     "keep": keep, "reason": reason, "evidence": strength, **nbytes,
                     "gaps": gaps}
            per_record.append(entry)
            if not keep:
                rej.reject("census_triage", rid, reason, tier=row["tier"], selected_as=sel)
                continue
            if reason == "vasp_evidence":
                for r in row.get("reasons") or ["(none)"]:
                    positives_by_reason[f"{row['tier']}:{r}"] += 1
            line = keep_entry(rec, row, fetch, reason, ev, gaps)
            if row.get("licence_admitted", True):
                kw.write(json.dumps(line) + "\n")
                kept_by[f"{reason}"] += 1
                stats["kept_records"] += 1
                stats["kept_bytes_evidence"] += nbytes["evidence"]
                stats["kept_bytes_blind"] += nbytes["blind"]
            else:
                rw.write(json.dumps(line) + "\n")
                stats["review_records"] += 1

    resid_pop = sum(1 for r in read_jsonl(scored_path) if r.get("tier") == "T3"
                    and (r.get("resource_type") in set(types) if types is not None else
                         r.get("resource_type") not in RESIDUAL_EXCLUDED_TYPES
                         and (include_github_residual or not is_github_snapshot(r))))
    sample_stats: dict[str, dict[str, Any]] = {}
    for label, c in samples.items():
        rate, lo, hi = wilson(c["positive"], c["n"])
        prate, plo, phi = wilson(c["positive_primary"], c["n"])
        sample_stats[label] = {"n": c["n"], "positive": c["positive"], "rate": rate,
                               "ci95": [lo, hi],
                               # the go/no-go for the full T3 run uses the STRICT rate: a VASP
                               # output actually seen (not just INCAR/POSCAR in a partial head)
                               "positive_primary": c["positive_primary"],
                               "rate_primary": prate, "ci95_primary": [plo, phi]}
        if label == "residual_sample":
            # hidden VASP records expected in the whole residual tier: [estimate, lo, hi]
            sample_stats[label]["population"] = resid_pop
            sample_stats[label]["extrapolated_hidden_records"] = [
                round(x * resid_pop) for x in (rate, lo, hi)]
            sample_stats[label]["extrapolated_hidden_records_primary"] = [
                round(x * resid_pop) for x in (prate, plo, phi)]
    probes = {e["recid"]: {k: e[k] for k in ("tier", "keep", "reason")}
              for e in per_record if e["recid"] in PROBE_RECIDS}
    summary = {"scored": str(scored_path), "out": str(out), "review": str(review),
               "selected": dict(Counter(f"{chosen[r]['tier']}:{chosen[r]['selected_as']}"
                                        for r in recs)),
               "missing_from_census": len(missing),
               "peeks": {k: v for k, v in stats.items() if k.startswith("peek")},
               "decisions": {k.split(":", 1)[1]: v for k, v in stats.items()
                             if k.startswith("decision:")},
               "kept": {"records": stats["kept_records"], "by_reason": dict(kept_by),
                        "bytes_evidence": stats["kept_bytes_evidence"],
                        "bytes_blind": stats["kept_bytes_blind"]},
               "licence_review_records": stats["review_records"],
               "samples": sample_stats, "positives_by_signal": dict(positives_by_reason),
               "probes": probes, "minutes": round((time.monotonic() - t0) / 60, 1)}
    report.write_text(json.dumps({"summary": summary, "records": per_record}, indent=1))
    logger.info("census triage: %s", {k: summary[k] for k in ("selected", "kept", "samples")})
    return summary
