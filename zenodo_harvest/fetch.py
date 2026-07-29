"""Stage 2 — fetch.

Download the VASP-relevant files for each triaged record and lay them out on
disk as one directory per calculation, ready for parsing. Design goals:

* **Cheap on disk / bandwidth**: from archives we extract only the files needed
  to parse (`vasprun.xml`, `OUTCAR`, `INCAR`, structures, …) and merely *record
  the presence* of heavy files (CHGCAR, WAVECAR, DOSCAR, EIGENVAL, …) as
  availability flags — never extracting them (mentor: record, don't store).
* **Resumable**: records already in ``fetched.jsonl`` are skipped wholesale, and
  within a record any file already present with a matching md5 is skipped — so an
  interrupted harvest resumes cleanly, with no duplicate work or manifest lines
  (important on the cluster).
* **Robust**: size-capped (archive *and* per-member uncompressed), checksum-verified,
  zip-slip-safe, and never holding a whole member in RAM (zip/tar/tar.zst/rar stream in
  chunks; 7z extracts selected members straight to disk). `.zip`/`.tar*` use the stdlib;
  `.rar`/`.7z`/`.tar.zst` need the optional `archives` extra (rarfile/py7zr/zstandard),
  else they are logged as a rejection rather than being fatal.
* **Interruptible**: a part-downloaded file resumes over HTTP Range instead of
  restarting (a cluster job's wallclock can expire mid-transfer of a >100 GB archive).
* **Quota-paced**: `max_disk_bytes` AND `max_disk_files` stop the run cleanly (resumably)
  once staging fills either limit, because cluster scratch is inode-limited as well as
  byte-limited and — measured on real records — inodes bind first.
"""

from __future__ import annotations

import errno
import logging
import os
import re
import shutil
import tarfile
import threading
import time
import zipfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from hashlib import md5
from pathlib import Path
from typing import Any

import requests

from . import config, zipstream
from .client import _parse_retry_after
from .manifest import JsonlWriter, RejectionLogger, read_jsonl

logger = logging.getLogger(__name__)

# Optional archive backends (installed via the ``archives`` extra). ``.rar`` also
# needs an ``unrar``/``bsdtar`` binary on PATH; ``rarfile`` raises at open time if
# it is missing, which we catch and log as a clean skip rather than a crash.
try:
    import py7zr  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    py7zr = None  # type: ignore
try:
    import rarfile  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    rarfile = None  # type: ignore
try:
    import zstandard  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    zstandard = None  # type: ignore

# Per-member *uncompressed* size cap: bounds disk and defuses decompression bombs
# (the archive-level max_bytes only caps the *compressed* download). Every backend
# writes members to disk without holding one in memory, so RAM stays ~chunk-sized
# regardless of member size (zip/tar/tar.zst/rar stream in chunks; 7z extracts to disk).
# NB: even with no per-member cap, the disk-budget valve (--max-disk-bytes/-files) is the
# real bomb guard — it charges every chunk as written, so a runaway member just fills the
# budget and the record is rolled back, whatever the cap.
DEFAULT_MAX_MEMBER_BYTES = 20_000_000_000

# --- Targeted ZIP member fetch (see zipstream.py) --------------------------------
# A ZIP's tail central directory gives every member's byte offset, so we can pull just
# the VASP files out of a .zip over HTTP Range instead of downloading the whole archive
# to extract a few files and delete the rest (mentor/user 2026-07-29; the third survey
# investigation found this the one worthwhile random-access play — tar has no index and
# compressed tars are non-seekable). Transfer, transient staging, and time all drop. ON
# by default (`--no-zip-stream` disables). Chosen over a whole-archive download only when
# it is worthwhile AND the request count is bounded:
#   * worthwhile — the archive is huge (avoid staging it whole) OR targeted fetch would
#     skip a meaningful mass of heavy files (CHGCAR/WAVECAR/…) it would otherwise pull
#     down and immediately discard;
#   * bounded — at most `zip_stream_max_files` target members, since each costs ~1-2 HTTP
#     requests and a many-small-member VASP dump is cheaper as one whole download.
# Anything not addressable (a ZIP64/encrypted/odd-compression *target* member, enumeration
# failure, Range not honoured, or a member that comes back corrupt) falls back to the
# whole-archive download+extract path, so there is never a regression. Targeted fetch is
# deliberately NOT bounded by --max-bytes/--max-member-bytes: every targeted member is a
# wanted VASP file, so only the disk-budget valve bounds it.
DEFAULT_ZIP_STREAM_MAX_FILES = 128
# At/above this archive size, target it regardless of skip (avoid staging it whole).
ZIP_STREAM_LARGE_ARCHIVE_BYTES = 10_000_000_000
# ...otherwise target only if we would skip at least this many (compressed) heavy bytes.
ZIP_STREAM_MIN_SKIP_BYTES = 50_000_000

# Record-level rejection reasons that are terminal (re-running can't help): the
# archive downloaded + extracted fine but held no usable VASP outputs. These recids
# are skipped on resume so their (often large) archives aren't re-downloaded and
# re-rejected every run. Transient reasons (download_error/http_*) are NOT here, so
# they still retry. See --retry-rejected to override.
_TERMINAL_REJECT_REASONS = {"no_calc_units_after_extract", "no_vasp_files_fetched"}

# Per-file failure reasons that say "the data may well be fine, we just couldn't get it
# this time": rate limits, server errors, dropped connections, and — importantly on a
# cluster — a full disk / exceeded quota (ENOSPC/EDQUOT) while writing.
# ``disk_budget_*`` counts as "couldn't get it this time" too: a file refused because the
# staging budget is full has nothing wrong with it, so the record must stay retryable —
# otherwise a pacing event would leave it looking like "this record contains no VASP",
# which is terminal and would be skipped by every future run.
_TRANSIENT_REASON_PREFIXES = ("download_error", "write_error", "http_5", "http_429",
                              "disk_budget")
# Errnos that mean "out of space", not "bad data", when extraction fails mid-write.
_OUT_OF_SPACE_ERRNOS = {errno.ENOSPC, errno.EDQUOT}
# Non-terminal stand-in for the record-level verdicts above: used when a record produced
# nothing but at least one failure was transient. WITHOUT this, a quota-exhaustion event
# would make every affected record look like "contains no VASP" — a TERMINAL reason — and
# they would be silently skipped by every later run. That is data loss from a condition
# the harvest is otherwise designed to ride out, so it must stay retryable.
_TRANSIENT_RECORD_REASON = "fetch_failed_transient"


def _is_transient_reason(reason: str) -> bool:
    return reason.startswith(_TRANSIENT_REASON_PREFIXES)

# Files worth extracting for parsing / provenance. A canonical VASP stem taken as
# a *word* — at the start of the basename or after a path/name separator — with
# any suffix. Deliberately kept at least as permissive as triage's
# ``models._VASP_RE`` so fetch never drops a file triage already confirmed
# (e.g. ``site1_OUTCAR``, ``Si.vasprun.xml``, ``vasprun_1.xml``). Over-matching is
# recall-safe: the pymatgen parse in stage 3 is the real precision gate. POTCAR
# is kept only to read its titel strings. Heavy outputs (CHGCAR/DOSCAR/…) contain
# no stem word and fall through to the availability branch.
_PARSE_STEMS = ("vasprun", "vaspout", "outcar", "incar", "kpoints",
                "poscar", "contcar", "potcar", "oszicar")
_PARSE_RE = re.compile(r"(?:^|[/_.\-])(?:" + "|".join(_PARSE_STEMS) + r")", re.IGNORECASE)

# Heavy outputs: presence recorded as availability, contents never extracted.
_AVAILABILITY = {
    "charge_density": re.compile(r"^(chgcar|chg|aeccar\d*|parchg)", re.IGNORECASE),
    "wavefunction": re.compile(r"^wavecar", re.IGNORECASE),
    "dos": re.compile(r"^doscar", re.IGNORECASE),
    "eigenvalues": re.compile(r"^eigenval", re.IGNORECASE),
    "projected": re.compile(r"^procar", re.IGNORECASE),
    "local_potential": re.compile(r"^locpot", re.IGNORECASE),
    "elf": re.compile(r"^elfcar", re.IGNORECASE),
}

_TAR_SUFFIXES = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")
# zstd-compressed tarballs: common for large ML/DFT datasets (measured on Zenodo:
# ~100+ GB of `.tar.zst` VASP training data). tarfile gained native `r:zst` only in
# CPython 3.14, so we decompress via the `zstandard` lib (the `archives` extra) and
# stream the result through tarfile — see `_extract_tar_zst`.
_ZST_TAR_SUFFIXES = (".tar.zst", ".tzst")
# Bare single-compression suffixes. Ambiguous by design: either ONE compressed file
# (OUTCAR.gz) or a tarball whose uploader dropped the `.tar` (research_data.gz). See
# _is_archive for how the two are told apart.
_BARE_COMPRESS_SUFFIXES = (".gz", ".bz2", ".xz")
# Split/spanned archive part names (`foo.z01`, `foo.z02`, …). The stdlib cannot
# reassemble a multi-volume zip, so these are surfaced as an explicit rejection
# rather than silently ignored (which looked like "record had no VASP").
_MULTIPART_RE = re.compile(r"\.(z\d{2}|r\d{2}|part\d+\.rar)$", re.IGNORECASE)


def _session(token: str | None) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "zenodo-harvest/0.1 (fetch)"
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s


def _md5(path: Path, chunk: int = 1 << 20) -> str:
    h = md5()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _dir_usage(path: Path) -> tuple[int, int]:
    """``(bytes, inodes)`` under ``path`` (``(0, 0)`` if absent), from ONE walk.

    Bytes drive the disk-budget valve (bounding persistent staging — extracted VASP
    files awaiting parse+purge). The second number is reported alongside because cluster
    scratch is also inode-limited (CSD3 ``/rds/user/<crsid>/hpc-work``: 1 TB **and 1
    million files**), and a single archive of many small per-calc dirs can burn inodes
    long before bytes — a silent stall we would otherwise only see as write errors.

    **Directories are counted as well as files.** CSD3's ``/rds`` is Lustre, where the
    "1 million files" quota is an *inode* quota and a directory consumes an inode exactly
    like a file (the CSD3 page states the limit but not what it counts; the Lustre quota
    model does). Real screening uploads are thousands of tiny per-calc *directories*, so
    counting only regular files understated the quota that actually stops the job. Bytes
    ignore directories: their own size is filesystem metadata, not payload.
    """
    if not path.exists():
        return 0, 0
    total = count = 0
    # Walk defensively: in the overlapped `pipeline`, fetch baselines ``raw_dir`` for
    # batch i+1 (this walk) WHILE ``purge-raw`` for batch i deletes trees under the same
    # ``raw_dir`` in a background thread. A plain ``rglob`` + ``stat`` would then hit a
    # just-deleted entry and raise ``FileNotFoundError`` mid-walk, aborting the foreground
    # fetch (which the pipeline reports as a hard fetch_error). ``os.walk`` with an
    # error-swallowing ``onerror`` skips a directory that vanishes, and the per-file
    # ``stat`` is guarded so a file removed between listing and stat is simply not counted.
    for root, dirs, files in os.walk(path, onerror=lambda _e: None):
        count += len(dirs)              # a directory is an inode too (Lustre quota)
        for name in files:
            try:
                total += os.stat(os.path.join(root, name)).st_size
                count += 1
            except OSError:
                continue                # vanished mid-walk (concurrent purge) — skip it
    return total, count


def _charged_mkdir(path: Path, budget: "StagingBudget | None",
                   own: list[int] | None = None) -> bool:
    """``mkdir -p`` that charges the budget one inode per directory it actually creates.

    Charged BEFORE creating, so a refusal leaves nothing on disk. Returns False when the
    inode limit refuses the directories (and raises :class:`BudgetExceeded` when a purge
    could make room, exactly like any other charge). ``own`` is only needed by callers
    running outside the record's own thread (see :class:`_BudgetedWriterFactory`).
    """
    if path.is_dir():
        return True
    if budget is not None and budget.enabled:
        missing = 0
        probe = path
        while not probe.exists() and probe.parent != probe:
            missing += 1
            probe = probe.parent
        if missing and not budget.charge(0, missing, own=own):
            return False
    path.mkdir(parents=True, exist_ok=True)
    return True


def _dir_bytes(path: Path) -> int:
    """Total size of the files under ``path`` (0 if absent)."""
    return _dir_usage(path)[0]


def download_file(
    session: requests.Session, url: str, dest: Path, expected_md5: str | None,
    max_bytes: int | None = None, resume: bool = True,
    budget: "StagingBudget | None" = None,
) -> tuple[bool, str]:
    """Download ``url`` to ``dest``, verifying md5. Returns (ok, reason).

    On HTTP 429 the download backs off (``min(Retry-After, 120)`` s) and retries in
    place, up to 3 attempts; only once that budget is spent does it fall through to
    the ``http_429`` transient rejection (so a still-throttled file simply retries on
    the next run rather than being dropped as a hard failure).

    **Byte-range resume** (``resume``, default on): a ``<dest>.part`` left by a killed
    run is continued with ``Range: bytes=<have>-`` + an append, instead of restarting
    from byte 0. This matters on the cluster, where the job wallclock (36 h on CSD3)
    can expire mid-transfer of a >100 GB archive — without it every resume threw away
    all the bytes already pulled. Safety: resume only happens when Zenodo gave us an
    ``expected_md5``, so a stale/garbage ``.part`` (e.g. from a superseded file version)
    is always caught by the final checksum and deleted, costing at most one wasted pass
    rather than corrupting the staged archive. A server that ignores ``Range`` answers
    200 and we transparently restart from scratch; a ``416`` means the ``.part`` is
    already complete, which the checksum then confirms.

    **Budget** (when given): every byte is charged to the :class:`StagingBudget` *as it is
    written*, never from the manifest's declared ``size`` — a server (or manifest) that
    understates a file's length cannot make the harvest write more than it counted. Bytes
    charged here are refunded only on the paths that actually delete the partial file; a
    ``.part`` deliberately kept for a later resume stays charged, because it stays on disk.
    """
    if dest.exists() and expected_md5 and _md5(dest) == expected_md5:
        return True, "cached"   # already on disk => already in the budget's baseline
    charged_bytes = 0
    charged_files = 0

    def _take(n_bytes: int, n_files: int = 0) -> bool:
        """Charge for bytes/inodes about to be written here (tracked so we can refund)."""
        nonlocal charged_bytes, charged_files
        if budget is None or budget.charge(n_bytes, n_files):
            charged_bytes += n_bytes
            charged_files += n_files
            return True
        return False

    def _give_back() -> None:
        """Refund this call's charges — only where the bytes are being deleted."""
        nonlocal charged_bytes, charged_files
        if budget is not None and (charged_bytes or charged_files):
            budget.refund(charged_bytes, charged_files)
        charged_bytes = charged_files = 0

    if not _charged_mkdir(dest.parent, budget):
        return False, "disk_budget_reached"
    tmp = dest.with_suffix(dest.suffix + ".part")
    # Resuming without a checksum would risk silently appending to unrelated bytes.
    can_resume = resume and expected_md5 is not None
    outcome = "downloaded"
    for attempt in range(3):
        have = tmp.stat().st_size if (can_resume and tmp.is_file()) else 0
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            with session.get(url, stream=True, timeout=120, headers=headers) as r:
                if r.status_code == 429 and attempt < 2:
                    wait = min(_parse_retry_after(r.headers.get("Retry-After")), 120)
                    logger.warning("download rate limited; sleeping %ss", wait)
                    time.sleep(wait)
                    continue
                if have and r.status_code == 416:
                    # Range unsatisfiable => the .part already covers the whole file.
                    # Let the md5 check below decide whether it is the right whole file.
                    outcome = "resumed"
                    break
                if have and r.status_code == 206:
                    mode, written = "ab", have   # continuing where we left off
                    outcome = "resumed"
                elif r.status_code == 200:
                    mode, written = "wb", 0      # fresh (or Range ignored -> restart)
                    if have:
                        logger.info("server ignored Range for %s; restarting download", url)
                        # Opening "wb" truncates the .part, so the bytes a previous run
                        # charged for it are about to vanish: hand them back.
                        if budget is not None:
                            budget.refund(have, 0)
                else:
                    return False, f"http_{r.status_code}"  # incl. http_429 after retries
                clen = r.headers.get("Content-Length")
                # With a 206 the Content-Length is the REMAINING bytes, so compare the
                # full projected size (already-have + remaining) against the cap.
                if max_bytes and clen and written + int(clen) > max_bytes:
                    return False, "over_size_cap"
                if not tmp.exists() and not _take(0, 1):
                    return False, "disk_budget_reached"   # the .part is a new inode
                with tmp.open(mode) as fh:
                    for chunk in r.iter_content(1 << 20):
                        if max_bytes and written + len(chunk) > max_bytes:
                            fh.close()
                            tmp.unlink(missing_ok=True)
                            _give_back()
                            return False, "over_size_cap"
                        # Charge what is ABOUT to be written, byte-exactly. Nothing
                        # declared is trusted, so a mis-stated Content-Length or manifest
                        # size cannot put more on disk than the budget accounted for.
                        if not _take(len(chunk)):
                            fh.close()
                            tmp.unlink(missing_ok=True)
                            _give_back()
                            return False, "disk_budget_reached"
                        written += len(chunk)
                        fh.write(chunk)
        except requests.RequestException as exc:
            # Keep the .part on a mid-transfer network failure: the next run resumes
            # from these bytes (only a checksum mismatch is allowed to discard them).
            # Kept bytes stay CHARGED — they are still occupying scratch.
            if not can_resume:
                tmp.unlink(missing_ok=True)
                _give_back()
            return False, f"download_error:{type(exc).__name__}"
        except OSError as exc:
            # e.g. ENOSPC / EDQUOT (disk, or the CSD3 1M-file quota) while writing the
            # .part on cluster scratch. Treat as a TRANSIENT per-file failure (not a hard
            # crash of the whole run) so a later resume retries it.
            tmp.unlink(missing_ok=True)
            _give_back()
            return False, f"write_error:{type(exc).__name__}"
        break  # a non-429 response streamed to completion
    if expected_md5 and _md5(tmp) != expected_md5:
        tmp.unlink(missing_ok=True)  # wrong/stale bytes: never resume from them again
        _give_back()
        return False, "md5_mismatch"
    tmp.replace(dest)
    return True, outcome


def _safe_members(names: list[str]) -> dict[str, Any]:
    """Scan archive member names for availability, without extracting anything."""
    avail = {k: False for k in _AVAILABILITY}
    avail_files: dict[str, str] = {}
    for name in names:
        base = name.rsplit("/", 1)[-1]
        for kind, rx in _AVAILABILITY.items():
            if rx.match(base):
                avail[kind] = True
                avail_files.setdefault(kind, name)
    return {"availability": avail, "availability_files": avail_files}


def _is_within(base: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


# _copy_capped outcomes. "over_cap" is about THIS member (skip it, keep extracting);
# "no_budget" is about the whole staging area (stop extracting this archive).
_COPY_OK = "ok"
_COPY_OVER_CAP = "over_cap"
_COPY_NO_BUDGET = "no_budget"


def _copy_capped(src: Any, out: Path, cap: int,
                 budget: "StagingBudget | None" = None) -> str:
    """Stream ``src`` -> ``out`` in chunks, charging ``budget`` for what is written.

    Streaming keeps peak memory ~chunk-sized regardless of the member's uncompressed
    size (the old ``src.read()`` allocated the whole member — an OOM / zip-bomb risk).

    **The budget is charged per chunk, before that chunk is written**, so the archive's
    own declared member size is never trusted: an entry whose header understates its real
    length simply runs out of allowance part way and is aborted. That is what makes the
    limits hold for any expansion ratio and any single-file size, rather than only for
    archives whose metadata happens to be honest. A member that cannot be finished is
    deleted and fully refunded — a half-file is never left behind counting against nothing.

    Returns one of ``_COPY_OK`` / ``_COPY_OVER_CAP`` (member exceeds ``cap``, i.e.
    ``--max-member-bytes``) / ``_COPY_NO_BUDGET`` (the staging limit refused it and no
    purge could help). Raises :class:`BudgetExceeded` when a purge *could* free room, so
    the record is rolled back and retried later.
    """
    if not _charged_mkdir(out.parent, budget):
        return _COPY_NO_BUDGET
    if budget is not None and not budget.charge(0, 1):   # the member's own inode
        return _COPY_NO_BUDGET
    written = 0
    complete = False
    try:
        with out.open("wb") as dst:
            for chunk in iter(lambda: src.read(1 << 20), b""):
                if written + len(chunk) > cap:
                    return _COPY_OVER_CAP
                if budget is not None and not budget.charge(len(chunk)):
                    return _COPY_NO_BUDGET
                written += len(chunk)
                dst.write(chunk)
        complete = True
        return _COPY_OK
    finally:
        # Any exit other than a complete copy (including a raised BudgetExceeded or a
        # write error) discards the partial file, so give back everything it was charged
        # — bytes AND its inode. Otherwise a refused member would permanently shrink the
        # budget for the rest of the run.
        if not complete:
            out.unlink(missing_ok=True)
            if budget is not None:
                budget.refund(written, 1)


def _extract_zip(path: Path, dest: Path, member_cap: int,
                 budget: "StagingBudget | None" = None) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = info.filename.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / info.filename
            if not _is_within(dest, out):
                continue  # zip-slip guard
            if info.file_size > member_cap:  # header says it's too big -> skip pre-extract
                logger.warning("skip oversized member %s (%d B) in %s", base, info.file_size, path.name)
                continue
            # Declared size is used ONLY to avoid starting a decompression that clearly
            # cannot fit (a cheap early skip). The real accounting happens per chunk
            # inside _copy_capped, so a header that lies changes nothing.
            if budget is not None and not budget.check(info.file_size, 1):
                break  # a disk/inode limit is reached — stop; the run pauses to purge
            with zf.open(info) as src:
                outcome = _copy_capped(src, out, member_cap, budget)
            if outcome == _COPY_OK:
                extracted.append(info.filename)
            elif outcome == _COPY_NO_BUDGET:
                break
    return names, extracted


def _rollback_targeted(paths: list[Path], budget: "StagingBudget | None") -> None:
    """Delete members a targeted fetch wrote (refunding the budget) before falling back,
    so the whole-archive re-extract does not double-charge the same files."""
    for p in paths:
        try:
            if p.is_file():
                sz = p.stat().st_size
                p.unlink()
                if budget is not None:
                    budget.refund(sz, 1)
        except OSError:  # pragma: no cover - best-effort cleanup
            pass


def _fetch_zip_targeted(url: str, dest: Path, session: requests.Session,
                        budget: "StagingBudget | None",
                        max_files: int) -> tuple[list[str], list[str]] | None:
    """Stage a ``.zip``'s VASP files by targeted HTTP-Range fetch, or return None.

    Returns ``(member_names, extracted)`` when the zip was handled here — including the
    case where enumeration *proves* it holds no VASP file, which returns ``(names, [])``
    and downloads nothing at all. Returns None to tell the caller to fall back to the
    whole-archive download+extract path: the zip is not addressable this way (Range not
    honoured, ZIP64/encrypted/odd-compression VASP member, too many members, or not worth
    it), or a member came back corrupt and should be re-verified via the archive md5.

    Members stream through :func:`_copy_capped`, so the disk budget is charged exactly as
    bytes land and :class:`BudgetExceeded` propagates for a whole-record rollback —
    identical to the archive extractors. Targeted fetch is intentionally NOT bounded by
    the per-member size cap: every targeted member is a wanted VASP file.
    """
    entries = zipstream.remote_central_directory(url, session)
    if entries is None:
        return None
    names = [e.name for e in entries]
    targets = [e for e in entries
               if not e.is_dir and _PARSE_RE.search(e.name.rsplit("/", 1)[-1])]
    if not targets:
        return names, []                    # proven no VASP inside -> no download needed
    if any(not t.targetable for t in targets):
        return None                          # a ZIP64/encrypted/odd VASP member -> fall back
    if len(targets) > max_files:
        return None                          # many members -> one whole download is cheaper
    total_comp = sum(e.compressed_size for e in entries if not e.needs_zip64)
    target_comp = sum(t.compressed_size for t in targets)
    worthwhile = (any(e.needs_zip64 for e in entries)
                  or total_comp >= ZIP_STREAM_LARGE_ARCHIVE_BYTES
                  or (total_comp - target_comp) >= ZIP_STREAM_MIN_SKIP_BYTES)
    if not worthwhile:
        return None                          # small, ~all-VASP zip -> whole download simpler

    extracted: list[str] = []
    written: list[Path] = []
    for t in targets:
        out = dest / t.name
        if not _is_within(dest, out):
            continue                         # zip-slip guard
        reader = zipstream.open_member_reader(session, url, t)
        if reader is None:                   # Range stopped being honoured mid-way
            _rollback_targeted(written, budget)
            return None
        try:
            # Unbounded member cap: a targeted member is always a wanted VASP file; only
            # the disk budget bounds it (charged per chunk inside _copy_capped).
            outcome = _copy_capped(reader, out, 1 << 62, budget)
        finally:
            reader.close()
        if outcome == _COPY_NO_BUDGET:
            # Member cannot fit the whole budget: stop like the extractors do — keep what
            # landed (the budget marked the record truncated). Do NOT fall back.
            break
        if outcome != _COPY_OK or not reader.complete:
            # short/corrupt member -> abandon targeted, fall back to the md5-verified
            # whole-archive download.
            if out.exists():
                sz = out.stat().st_size
                out.unlink(missing_ok=True)
                if budget is not None:
                    budget.refund(sz, 1)
            _rollback_targeted(written, budget)
            return None
        extracted.append(t.name)
        written.append(out)
    return names, extracted


def _extract_tar(path: Path, dest: Path, member_cap: int,
                 budget: "StagingBudget | None" = None) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with tarfile.open(path) as tf:
        names = tf.getnames()
        for member in tf.getmembers():
            if not member.isfile():
                continue
            base = member.name.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / member.name
            if not _is_within(dest, out):
                continue
            if member.size > member_cap:
                logger.warning("skip oversized member %s (%d B) in %s", base, member.size, path.name)
                continue
            if budget is not None and not budget.check(member.size, 1):
                break
            src = tf.extractfile(member)
            if src is None:
                continue
            outcome = _copy_capped(src, out, member_cap, budget)
            if outcome == _COPY_OK:
                extracted.append(member.name)
            elif outcome == _COPY_NO_BUDGET:
                break
    return names, extracted


def _extract_rar(path: Path, dest: Path, member_cap: int,
                 budget: "StagingBudget | None" = None) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with rarfile.RarFile(path) as rf:  # type: ignore[union-attr]
        names = rf.namelist()
        for info in rf.infolist():
            if info.isdir():
                continue
            base = info.filename.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / info.filename
            if not _is_within(dest, out):
                continue
            size = getattr(info, "file_size", 0) or 0
            if size > member_cap:
                logger.warning("skip oversized member %s in %s", base, path.name)
                continue
            if budget is not None and not budget.check(size, 1):
                break
            with rf.open(info) as src:
                outcome = _copy_capped(src, out, member_cap, budget)
            if outcome == _COPY_OK:
                extracted.append(info.filename)
            elif outcome == _COPY_NO_BUDGET:
                break
    return names, extracted


class _BudgetedMemberWriter:
    """A file-like sink for ONE py7zr member: writes to disk, charging as it goes.

    py7zr hands each member a writer object (``py7zr.io.Py7zIO``) and calls ``write``
    with each decompressed block, so this is the ``.7z`` equivalent of
    :func:`_copy_capped`'s per-chunk charge. ``out=None`` makes it a sink that swallows
    bytes without writing them — used once something has gone over a limit, so py7zr can
    finish its CRC bookkeeping while nothing more lands on disk.
    """

    def __init__(self, factory: "_BudgetedWriterFactory", out: Path | None, name: str):
        self._factory = factory
        self._out = out
        self._name = name
        self._fh = out.open("wb") if out is not None else None
        self._written = 0
        self._dropped = out is None

    def write(self, s: bytes | bytearray) -> int:
        n = len(s)
        if self._dropped:
            return n                       # deliberately discarded; report it as written
        if self._written + n > self._factory.cap:
            logger.warning("skip oversized member %s in %s", self._name, self._factory.label)
            self._drop()
            return n
        if not self._factory.take(n):      # charge BEFORE the bytes land
            self._drop()
            return n
        self._written += n
        assert self._fh is not None
        self._fh.write(s)
        return n

    def _drop(self) -> None:
        """Abandon this member: close, delete, and refund everything it was charged."""
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if self._out is not None:
            self._out.unlink(missing_ok=True)
        self._factory.give_back(self._written, 1)
        self._written = 0
        self._dropped = True

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
            self._factory.extracted.append(self._name)

    # --- the rest of the Py7zIO protocol; py7zr seeks to 0 and may query the size ---
    def read(self, size: int | None = None) -> bytes:
        return b""

    def seek(self, offset: int, whence: int = 0) -> int:
        return offset      # a write-only sink: nothing to rewind

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def size(self) -> int:
        return self._written


class _BudgetedWriterFactory:
    """py7zr ``WriterFactory`` that keeps a ``.7z`` extraction inside the staging budget.

    The other backends can check the budget between members because they drive the loop.
    py7zr does not: one ``extract()`` call decompresses everything, possibly on worker
    threads. So the check moves into the writer (see :class:`_BudgetedMemberWriter`), and
    a refusal is recorded here rather than raised — an exception thrown inside py7zr's
    threads would be lost, and the classification (defer vs. never-fits) depends on a
    per-record snapshot that only the calling thread holds. :func:`_extract_7z` inspects
    :attr:`abort` after ``extract()`` returns and re-raises in the right thread.
    """

    def __init__(self, dest: Path, cap: int, budget: "StagingBudget | None",
                 own: list[int], label: str):
        self.dest = dest
        self.cap = cap
        self.budget = budget
        self.own = own              # the record's own-usage tally; classifies a refusal
        self.label = label          # archive name, for log lines
        self.extracted: list[str] = []
        self.abort = ""             # "" | "defer" (purge can help) | "unfittable"
        self.deferred: BudgetExceeded | None = None
        self._writers: list[_BudgetedMemberWriter] = []
        self._lock = threading.Lock()

    def take(self, n_bytes: int, n_files: int = 0) -> bool:
        if self.budget is None:
            return True
        try:
            if self.budget.charge(n_bytes, n_files, own=self.own):
                return True
            with self._lock:
                self.abort = self.abort or "unfittable"
        except BudgetExceeded as exc:
            with self._lock:
                self.abort, self.deferred = "defer", exc
        return False

    def give_back(self, n_bytes: int, n_files: int = 0) -> None:
        if self.budget is not None and (n_bytes or n_files):
            self.budget.refund(n_bytes, n_files, own=self.own)

    def create(self, filename: str) -> Any:
        """Called by py7zr once per member, just before it is decompressed."""
        out = Path(filename)
        try:
            name = out.relative_to(self.dest).as_posix()
        except ValueError:
            name = out.name
        usable = not self.abort and _is_within(self.dest, out) and self._parents(out.parent)
        writer = _BudgetedMemberWriter(
            self, out if usable and self.take(0, 1) else None, name)
        self._writers.append(writer)
        return writer

    def _parents(self, path: Path) -> bool:
        try:
            ok = _charged_mkdir(path, self.budget, own=self.own)
        except BudgetExceeded as exc:
            with self._lock:
                self.abort, self.deferred = "defer", exc
            return False
        if not ok:
            with self._lock:
                self.abort = self.abort or "unfittable"
        return ok

    def close_all(self) -> None:
        """Close any writer py7zr abandoned (it skips ``close()`` on a CRC error)."""
        for w in self._writers:
            w.close()


def _extract_7z(path: Path, dest: Path, member_cap: int,
                budget: "StagingBudget | None" = None) -> tuple[list[str], list[str]]:
    """Selectively extract VASP files from a ``.7z``.

    Uses py7zr's ``extract(path=, targets=, factory=)``: the factory streams each member
    to disk through :class:`_BudgetedMemberWriter`, which charges the staging budget for
    every block *before* it is written. So the ``.7z`` path holds the same invariant as
    the others — nothing lands that was not counted, whatever the header claims — while
    peak memory stays block-sized. (The older ``read(targets)`` API returned in-memory
    ``BytesIO`` objects holding the SUM of the selected members, an OOM risk, and py7zr
    1.x removed it: calling it raised ``AttributeError`` and no ``.7z`` record could be
    staged at all.)

    Targets are pre-filtered on the header (VASP-relevant name, inside ``dest``, under the
    member cap) purely to avoid pointless decompression; the header is never the authority.
    """
    with py7zr.SevenZipFile(path, "r") as zf:  # type: ignore[union-attr]
        infos = zf.list()
        names = [i.filename for i in infos]
        sizes = {i.filename: getattr(i, "uncompressed", 0) or 0 for i in infos}
        targets = []
        for n in names:
            if (n.endswith("/") or not _PARSE_RE.search(n.rsplit("/", 1)[-1])
                    or not _is_within(dest, dest / n)):
                continue
            if sizes.get(n, 0) > member_cap:
                logger.warning("skip oversized member %s (%d B) in %s",
                               n, sizes.get(n, 0), path.name)
                continue
            # Cheap early skip only: if the declared size already cannot fit, don't start.
            if budget is not None and not budget.check(sizes.get(n, 0) or 0, 1):
                break
            targets.append(n)
        if not targets:
            return names, []
        if not _charged_mkdir(dest, budget):
            return names, []
        own = budget.own_handle() if budget is not None else [0, 0]
        factory = _BudgetedWriterFactory(dest, member_cap, budget, own, path.name)
        try:
            zf.extract(path=dest, targets=targets, factory=factory)
        finally:
            factory.close_all()
    if factory.abort == "defer" and factory.deferred is not None:
        raise factory.deferred          # roll the record back and retry after a purge
    if factory.abort == "unfittable" and budget is not None:
        budget.mark_truncated()         # cannot ever fit; keep what landed, report it
    return names, factory.extracted


def _extract_tar_zst(path: Path, dest: Path, member_cap: int,
                     budget: "StagingBudget | None" = None) -> tuple[list[str], list[str]]:
    """Extract VASP files from a zstd-compressed tarball (``.tar.zst``/``.tzst``).

    Streams the decompressed bytes through ``tarfile`` in streaming (``r|``) mode so
    peak memory/disk stays ~chunk-sized regardless of the (often huge) decompressed
    size — the same selective, cap-guarded, zip-slip-safe extraction as ``_extract_tar``.
    """
    extracted: list[str] = []
    names: list[str] = []
    dctx = zstandard.ZstdDecompressor()  # type: ignore[union-attr]
    with path.open("rb") as fh, dctx.stream_reader(fh) as reader:
        # streaming tar: members arrive in order; extractfile() works on the current one.
        with tarfile.open(fileobj=reader, mode="r|") as tf:
            for member in tf:
                names.append(member.name)
                if not member.isfile():
                    continue
                base = member.name.rsplit("/", 1)[-1]
                if not _PARSE_RE.search(base):
                    continue
                out = dest / member.name
                if not _is_within(dest, out):
                    continue
                if member.size > member_cap:
                    logger.warning("skip oversized member %s (%d B) in %s", base, member.size, path.name)
                    continue
                if budget is not None and not budget.check(member.size, 1):
                    break
                src = tf.extractfile(member)
                if src is None:
                    continue
                outcome = _copy_capped(src, out, member_cap, budget)
                if outcome == _COPY_OK:
                    extracted.append(member.name)
                elif outcome == _COPY_NO_BUDGET:
                    break
    return names, extracted


_EXTRACTORS = {"zip": _extract_zip, "tar": _extract_tar, "rar": _extract_rar,
               "sevenzip": _extract_7z, "tarzst": _extract_tar_zst}

# Exceptions a bad/truncated archive can raise across all backends — caught per
# archive so one corrupt file rejects just that file, not the whole run. The optional
# backends add their own error types when installed. RuntimeError/AttributeError are
# included because these are third-party libraries whose APIs drift between versions
# (py7zr 1.x removed the `read()` this module once used): an API mismatch must reject
# ONE archive, not abort a multi-hour harvest.
_EXTRACT_ERRORS: tuple[type[BaseException], ...] = (
    zipfile.BadZipFile, tarfile.TarError, EOFError, OSError, ValueError,
    RuntimeError, AttributeError,
)
if zstandard is not None:  # pragma: no branch - trivial
    _EXTRACT_ERRORS = (*_EXTRACT_ERRORS, zstandard.ZstdError)
if py7zr is not None:  # pragma: no branch - trivial
    _EXTRACT_ERRORS = (*_EXTRACT_ERRORS, py7zr.exceptions.ArchiveError)
if rarfile is not None:  # pragma: no branch - trivial
    _EXTRACT_ERRORS = (*_EXTRACT_ERRORS, rarfile.Error)


def _is_archive(name: str) -> str | None:
    low = name.lower()
    if low.endswith(".zip"):
        return "zip"
    if low.endswith(_TAR_SUFFIXES):
        return "tar"
    if low.endswith(".rar"):
        return "rar"
    if low.endswith(".7z"):
        return "sevenzip"
    if low.endswith(_ZST_TAR_SUFFIXES):
        return "tarzst"
    if low.endswith(".zst"):
        # A bare `.zst` is ambiguous: a single compressed VASP file (``OUTCAR.zst``)
        # stays a direct-file download (handled by _PARSE_RE below); anything else is
        # treated as a zstd tarball (``Research_Data.zst`` etc. seen on Zenodo).
        stem = low[:-4].rsplit("/", 1)[-1]
        return None if _PARSE_RE.search(stem) else "tarzst"
    if low.endswith(_BARE_COMPRESS_SUFFIXES):
        # Same ambiguity for bare `.gz`/`.bz2`/`.xz`. ``OUTCAR.gz`` is a single compressed
        # VASP file and stays a direct download (_PARSE_RE branch, and parse decompresses
        # it). Anything else is very often a MISNAMED compressed tarball — measured on
        # Zenodo, e.g. a 17 GB `…-vasp-raw.gz` — which we previously fell through to the
        # availability branch, downloading nothing and reporting the record as holding no
        # VASP. Treat it as a tar: ``tarfile.open`` transparently auto-detects
        # gzip/bzip2/xz from the CONTENT, not the name, and a file that is not a tar
        # raises ``TarError`` (in _EXTRACT_ERRORS) -> one clean `extract_error` rejection.
        stem = low.rsplit(".", 1)[0].rsplit("/", 1)[-1]
        return None if _PARSE_RE.search(stem) else "tar"
    return None


def _archive_subdir(base: str) -> str:
    """A filesystem-safe per-archive extraction subdir (avoids cross-archive
    member-path collisions when one record ships multiple archives)."""
    stem = base
    for suf in (*_TAR_SUFFIXES, *_ZST_TAR_SUFFIXES, ".zip", ".rar", ".7z", ".zst",
                *_BARE_COMPRESS_SUFFIXES):
        if stem.lower().endswith(suf):
            stem = stem[: -len(suf)]
            break
    return re.sub(r"[^A-Za-z0-9._-]", "_", stem) or "archive"


# Role detection for grouping extracted files into calc units. Separator-tolerant
# (``site1_OUTCAR``, ``Si.vasprun.xml``) and suffix-tolerant (``vasprun_1.xml``),
# consistent with ``_PARSE_RE`` above; vasprun/vaspout additionally pin the
# expected extension so a stray ``vasprun.json`` isn't mistaken for the primary.
_ROLE_RES: tuple[tuple[str, "re.Pattern[str]"], ...] = (
    ("vasprun", re.compile(r"(?:^|[/_.\-])vasprun[\w.\-]*\.xml(?:\.(?:gz|bz2|xz|zst))?$", re.IGNORECASE)),
    ("vaspout", re.compile(r"(?:^|[/_.\-])vaspout[\w.\-]*\.h5$", re.IGNORECASE)),
    ("outcar",  re.compile(r"(?:^|[/_.\-])outcar", re.IGNORECASE)),
    ("incar",   re.compile(r"(?:^|[/_.\-])incar", re.IGNORECASE)),
    ("contcar", re.compile(r"(?:^|[/_.\-])contcar", re.IGNORECASE)),
    ("poscar",  re.compile(r"(?:^|[/_.\-])poscar", re.IGNORECASE)),
    ("potcar",  re.compile(r"(?:^|[/_.\-])potcar", re.IGNORECASE)),
)


def _unit_role(base: str) -> str | None:
    """Which calc-unit slot a filename fills (vasprun/outcar/incar/…), or None."""
    for role, rx in _ROLE_RES:
        if rx.search(base):
            return role
    return None


_PRIMARY_ROLES = ("vasprun", "vaspout", "outcar")


def _unit_tag(base: str, role: str) -> str:
    """Distinguishing prefix/suffix of a filename once its role word is removed.

    ``site1_OUTCAR`` -> ``site1``; ``vasprun.xml`` -> ``""``; ``vasprun_1.xml`` ->
    ``1``. Files sharing a tag in a multi-calc directory belong to the same calc.
    """
    low = base.lower()
    i = low.find(role)
    tag = (low[:i] + low[i + len(role):]) if i != -1 else low
    tag = re.sub(r"\.(xml|h5|gz|bz2|xz|zst|json)$", "", tag)
    return tag.strip(" ._-/")


def _assign_role(slot: dict[str, str], role: str, path: Path) -> None:
    if role == "vasprun" and "vasprun" in slot:
        if path.name.lower() == "vasprun.xml":  # prefer the canonical name if several
            slot["vasprun"] = str(path)
    else:
        slot.setdefault(role, str(path))


def _find_calc_units(root: Path) -> list[dict[str, str]]:
    """Group extracted files into calc units — one per distinct primary output.

    Grouping by directory alone silently collapses multiple independent
    calculations that share a flat directory but differ only by a filename
    prefix/suffix (e.g. ``site1_OUTCAR`` + ``site2_OUTCAR``): all-but-one primary is
    dropped and the survivors are cross-paired. So within each directory: with a
    single primary the whole directory is one unit (the common per-calc-directory
    layout — tagged inputs stay attached); with several primaries we split by tag so
    each OUTCAR/vasprun/vaspout seeds its own unit, pairing same-tag inputs and
    sharing untagged inputs across them.
    """
    by_dir: dict[Path, list[tuple[str, Path]]] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        role = _unit_role(p.name)
        if role is not None:
            by_dir.setdefault(p.parent, []).append((role, p))

    units: list[dict[str, str]] = []
    for _d, items in sorted(by_dir.items()):
        primaries = [(r, p) for r, p in items if r in _PRIMARY_ROLES]
        if not primaries:
            continue  # inputs only (POSCAR/INCAR/…): no energies/forces to parse
        if len(primaries) == 1:
            slot: dict[str, str] = {"dir": str(_d)}
            for r, p in items:
                _assign_role(slot, r, p)
            units.append(slot)
            continue
        # multiple primaries in one flat directory -> split into one unit per tag
        groups: dict[str, list[tuple[str, Path]]] = {}
        shared_inputs: list[tuple[str, Path]] = []
        for r, p in items:
            tag = _unit_tag(p.name, r)
            if r not in _PRIMARY_ROLES and tag == "":
                shared_inputs.append((r, p))  # untagged input applies to every calc here
            else:
                groups.setdefault(tag, []).append((r, p))
        for _tag, grp in sorted(groups.items()):
            if not any(r in _PRIMARY_ROLES for r, _ in grp):
                continue  # a tagged input with no matching primary -> nothing to seed
            slot = {"dir": str(_d)}
            for r, p in (*grp, *shared_inputs):
                _assign_role(slot, r, p)
            units.append(slot)
    return units


def _worth_keeping(dest: Path, transient: bool, truncated: bool) -> bool:
    """Whether to leave an unrecorded record's staging tree on disk for the next run.

    Worth keeping only when the failure was transient AND something reusable actually
    landed: verified files and ``.part`` remnants let a resume skip the re-download. A
    tree kept for no reason holds budget nothing can reclaim (see :func:`_discard_stage`),
    and a record already known not to fit will not be helped by resuming it.
    """
    if truncated or not transient or not dest.exists():
        return False
    return any(p.is_file() for p in dest.rglob("*"))


def _discard_stage(dest: Path, budget: "StagingBudget | None") -> tuple[int, int]:
    """Delete a record's staging tree and refund it. Returns ``(bytes, inodes)`` freed.

    Used whenever a record will NOT be written to ``fetched.jsonl``. Anything it left
    behind is unreachable: ``purge-raw`` only reclaims trees whose calc_ids reached the
    dataset, so an unlisted leftover would hold budget for the rest of the harvest. That
    is not merely untidy — it is a stall. A record refused as "too big for the whole
    budget" used to leave its empty directories charged, which made the budget non-empty
    when the *next* record started; a non-empty budget means "a purge could free room", so
    every following record was deferred and retried forever against space nothing could
    reclaim. (Observed in a randomised sweep: 8 of 9 records never collected.)
    """
    if not dest.exists():
        return 0, 0
    freed_bytes, freed_inodes = _dir_usage(dest)
    if dest.is_dir():
        freed_inodes += 1                      # the record dir itself
    shutil.rmtree(dest, ignore_errors=True)
    if budget is not None:
        budget.refund(freed_bytes, freed_inodes)
    return freed_bytes, freed_inodes


def fetch_record(rec: dict, session: requests.Session, raw_dir: Path,
                 max_bytes: int | None, rej: RejectionLogger,
                 max_member_bytes: int = DEFAULT_MAX_MEMBER_BYTES,
                 budget: "StagingBudget | None" = None,
                 zip_stream: bool = True,
                 zip_stream_max_files: int = DEFAULT_ZIP_STREAM_MAX_FILES) -> dict | None:
    """Download + stage one record. Returns a fetched-manifest entry or None.

    ``budget`` (see :class:`StagingBudget`) is charged for every byte and file actually
    written, and refunded when an archive is deleted after extraction. When a limit is
    reached the record keeps whatever already landed (partial calc units still parse) and
    a ``disk_budget_reached`` rejection is logged, so the truncation is auditable and the
    caller can stop cleanly and let the pacing loop parse+purge.

    ``zip_stream`` (default on) pulls only the VASP files out of a ``.zip`` over HTTP
    Range when worthwhile, instead of downloading the whole archive (see
    :func:`_fetch_zip_targeted`); ``zip_stream_max_files`` bounds the per-archive request
    count before it falls back to a whole download.
    """
    recid = rec["recid"]
    dest = raw_dir / recid
    availability = {k: False for k in _AVAILABILITY}
    availability_files: dict[str, str] = {}
    got_any = False
    transient = False   # any "couldn't get it THIS time" failure (see _is_transient_reason)

    if budget is not None:
        budget.begin_record()
    try:
        got_any, transient = _stage_record_files(
            rec, recid, dest, session, max_bytes, rej, max_member_bytes, budget,
            availability, availability_files, zip_stream, zip_stream_max_files)
    except BudgetExceeded as exc:
        # A limit was reached part way through this record. Roll it back entirely — a
        # record is staged whole or not at all — refund what it had taken, and leave it
        # OUT of the fetched manifest so a later pass (after parse+purge frees room)
        # re-fetches it complete. Recording a truncated record instead would make the
        # partial data permanent, since resumes skip recids already in the manifest.
        rolled_bytes, rolled_files = _dir_usage(dest)
        if dest.is_dir():
            rolled_files += 1   # rmtree also removes the record dir, itself a charged inode
        shutil.rmtree(dest, ignore_errors=True)
        if budget is not None:
            budget.refund(rolled_bytes, rolled_files)
        rej.reject("fetch", recid, "disk_budget_deferred", detail=str(exc),
                   rolled_back_bytes=rolled_bytes, rolled_back_files=rolled_files)
        logger.info("record %s deferred (%s); rolled back %d B / %d files",
                    recid, exc, rolled_bytes, rolled_files)
        return None

    # A record refused by the budget must NEVER end up with a terminal verdict. It has
    # nothing wrong with it — the budget was simply too small for it — so it has to stay
    # collectable once the budget is raised. Without this, an inode/byte limit that refused
    # a record's very first member left it looking like "this record contains no VASP",
    # which resumes skip forever: silent, permanent data loss.
    truncated = budget is not None and budget.record_was_truncated()
    if truncated:
        logger.warning("record %s does not fit in the whole staging budget", recid)

    def _budget_detail(kept: bool) -> str:
        return ("record does not fit in --max-disk-bytes/--max-disk-files even when the "
                "budget is empty; " + ("staged as much as fitted" if kept else
                                       "nothing could be staged") +
                ". Raise the budget (and remove this recid from fetched.jsonl) to collect "
                "the rest — this reason is NOT terminal, so it is retried on the next run.")

    if not got_any:
        # Only call it "no VASP here" (terminal) when nothing transient and no budget
        # refusal got in the way — otherwise the record must stay retryable.
        if truncated:
            reason = "record_exceeds_disk_budget"
        else:
            reason = _TRANSIENT_RECORD_REASON if transient else "no_vasp_files_fetched"
        if not _worth_keeping(dest, transient, truncated):
            _discard_stage(dest, budget)
        rej.reject("fetch", recid, reason,
                   transient=(transient or truncated) or None,
                   detail=_budget_detail(kept=False) if truncated else None)
        return None
    if truncated:
        # The record cannot fit in the whole budget, so it was staged as far as the budget
        # allowed rather than retried forever. Keep what landed — a partial screening record
        # is still hundreds of usable calcs — but say so loudly.
        rej.reject("fetch", recid, "record_exceeds_disk_budget",
                   detail=_budget_detail(kept=True))

    units = _find_calc_units(dest / "extracted")
    if not units:
        if truncated:
            reason = "record_exceeds_disk_budget"
        else:
            reason = (_TRANSIENT_RECORD_REASON if transient
                      else "no_calc_units_after_extract")
        if not _worth_keeping(dest, transient, truncated):
            _discard_stage(dest, budget)
        rej.reject("fetch", recid, reason,
                   transient=(transient or truncated) or None,
                   detail=_budget_detail(kept=False) if truncated else None)
        return None
    return _fetched_entry(rec, recid, dest, raw_dir, units, availability, availability_files)


def _stage_record_files(
    rec: dict, recid: str, dest: Path, session: requests.Session,
    max_bytes: int | None, rej: RejectionLogger, max_member_bytes: int,
    budget: "StagingBudget | None", availability: dict, availability_files: dict,
    zip_stream: bool = True,
    zip_stream_max_files: int = DEFAULT_ZIP_STREAM_MAX_FILES,
) -> tuple[bool, bool]:
    """Download + extract one record's files. Returns ``(got_any, transient)``.

    Raises :class:`BudgetExceeded` if a disk/inode limit is reached part way, which
    :func:`fetch_record` turns into a full rollback of this record.
    """
    got_any = False
    transient = False
    for f in rec["files"]:
        key, url, size = f["key"], f.get("download"), f.get("size") or 0
        cksum = (f.get("checksum") or "").split(":", 1)[-1] or None
        base = (key or "").rsplit("/", 1)[-1]
        kind = _is_archive(base)

        # A split/spanned archive part (foo.z01, foo.r01, foo.partN.rar) cannot be
        # reassembled by the stdlib. Surface it explicitly instead of letting it fall
        # through to the availability branch (which silently produced no VASP files and
        # looked like "record had no VASP").
        if kind is None and _MULTIPART_RE.search(base):
            rej.reject("fetch", f"{recid}:{key}", "archive_multipart_unsupported",
                       detail="split/spanned archive; not reassembled")
            continue

        # Optional-backend archives: skip cleanly (visible rejection) when the backing
        # library isn't installed, rather than silently dropping the record's data.
        _missing_backend = (
            (kind == "rar" and rarfile is None)
            or (kind == "sevenzip" and py7zr is None)
            or (kind == "tarzst" and zstandard is None)
        )
        if _missing_backend:
            rej.reject("fetch", f"{recid}:{key}", "archive_unsupported",
                       format=base.rsplit(".", 1)[-1], detail="install the 'archives' extra")
            continue

        if kind is not None:  # a real archive to download + selectively extract
            # Targeted ZIP fetch: pull only the VASP members over HTTP Range when it is
            # worthwhile, instead of downloading the whole archive. Handles the record's
            # zip entirely on success (including proving it holds no VASP without any
            # download); returns None to fall back to the whole-archive path below.
            # Deliberately attempted BEFORE the max_bytes check: we never download the
            # whole archive, so an over-cap zip can still yield its sub-cap VASP files.
            if kind == "zip" and zip_stream and url:
                extract_dir = dest / "extracted" / _archive_subdir(base)
                try:
                    result = _fetch_zip_targeted(url, extract_dir, session, budget,
                                                 zip_stream_max_files)
                except BudgetExceeded:
                    raise  # a disk/inode limit -> whole-record rollback (see fetch_record)
                except _EXTRACT_ERRORS as exc:
                    # A disk write / decode error during targeted fetch: same handling as
                    # the whole-archive extractor path (out-of-space stays transient).
                    rej.reject("fetch", f"{recid}:{key}", "extract_error",
                               detail=f"{type(exc).__name__}: {exc}")
                    if isinstance(exc, OSError) and exc.errno in _OUT_OF_SPACE_ERRNOS:
                        transient = True
                    continue
                if result is not None:
                    names, extracted = result
                    info = _safe_members(names)
                    for akey, aval in info["availability"].items():
                        availability[akey] = availability[akey] or aval
                    availability_files.update(info["availability_files"])
                    got_any = got_any or bool(extracted)
                    continue
                # not addressable by targeted fetch -> fall through to whole download

            if max_bytes and size > max_bytes:
                rej.reject("fetch", f"{recid}:{key}", "over_size_cap", size=size)
                continue
            arc = dest / base
            # An archive occupies disk while it is downloaded and extracted, so it is
            # charged too — and refunded below, since it is deleted straight afterwards.
            # The declared size is only a pre-check ("don't even start"); download_file
            # charges the bytes it actually receives.
            if budget is not None and not budget.check(size, 1):
                rej.reject("fetch", f"{recid}:{key}", "disk_budget_reached",
                           size=size, limit=budget.hit_limit)
                transient = True   # refused for space, not because the data is unusable
                continue
            ok, why = download_file(session, url, arc, cksum, max_bytes, budget=budget)
            if not ok:
                rej.reject("fetch", f"{recid}:{key}", why, size=size)
                transient = transient or _is_transient_reason(why)
                continue
            arc_bytes = arc.stat().st_size   # what really landed, not what was advertised
            # Extract into a per-archive subdir so identical member paths across
            # multiple archives in one record can't overwrite each other.
            extract_dir = dest / "extracted" / _archive_subdir(base)
            try:
                names, extracted = _EXTRACTORS[kind](arc, extract_dir, max_member_bytes,
                                                     budget)
            except _EXTRACT_ERRORS as exc:
                rej.reject("fetch", f"{recid}:{key}", "extract_error",
                           detail=f"{type(exc).__name__}: {exc}")
                # A write that failed for lack of space is transient, not bad data.
                if isinstance(exc, OSError) and exc.errno in _OUT_OF_SPACE_ERRNOS:
                    transient = True
                arc.unlink(missing_ok=True)
                if budget is not None:
                    budget.refund(arc_bytes, 1)
                continue
            info = _safe_members(names)
            for k, v in info["availability"].items():
                availability[k] = availability[k] or v
            availability_files.update(info["availability_files"])
            arc.unlink(missing_ok=True)  # drop the archive; keep only extracted VASP files
            if budget is not None:
                budget.refund(arc_bytes, 1)  # transient bytes, not staged footprint
            got_any = got_any or bool(extracted)

        elif _PARSE_RE.search(base):  # directly-exposed VASP file
            if max_bytes and size > max_bytes:
                rej.reject("fetch", f"{recid}:{key}", "over_size_cap", size=size)
                continue
            if budget is not None and not budget.check(size, 1):
                rej.reject("fetch", f"{recid}:{key}", "disk_budget_reached",
                           size=size, limit=budget.hit_limit)
                transient = True   # refused for space, not because the data is unusable
                continue
            out = dest / "extracted" / key
            if not _is_within(dest / "extracted", out):
                rej.reject("fetch", f"{recid}:{key}", "unsafe_path")  # path traversal guard
                continue
            ok, why = download_file(session, url, out, cksum, max_bytes, budget=budget)
            if ok:
                got_any = True
            else:
                rej.reject("fetch", f"{recid}:{key}", why, size=size)
                transient = transient or _is_transient_reason(why)

        else:  # heavy / irrelevant direct file -> availability only
            for akind, rx in _AVAILABILITY.items():
                if rx.match(base):
                    availability[akind] = True
                    availability_files.setdefault(akind, key)

    return got_any, transient


def _fetched_entry(rec: dict, recid: str, dest: Path, raw_dir: Path, units: list[dict],
                   availability: dict, availability_files: dict) -> dict:
    """Build the fetched-manifest entry for a staged record."""
    # Store paths RELATIVE to raw_dir. Absolute paths break the manifest as soon as
    # staged data is moved between cluster scratch areas; parse resolves these back
    # against its --raw-dir. calc_id keys off the primary path relative to
    # <local_dir>/extracted, so the recid prefix cancels and the id is unchanged.
    rel_units = [{k: str(Path(v).relative_to(raw_dir)) for k, v in u.items()} for u in units]

    return {
        "recid": recid,
        "provenance": {
            "source": "zenodo",
            "record_id": recid,
            "conceptrecid": rec.get("conceptrecid"),
            "doi": rec.get("doi"),
            "conceptdoi": rec.get("conceptdoi"),
            "url": rec.get("zenodo_url"),
            "title": rec.get("title"),
            "creators": rec.get("creators"),
            "license": rec.get("license"),
            "resource_type": rec.get("resource_type"),  # source-quality tag for late filtering
            "publication_date": rec.get("publication_date"),
            "keywords": rec.get("keywords"),
        },
        "local_dir": str(dest.relative_to(raw_dir)),
        "n_calc_units": len(units),
        "calc_units": rel_units,
        "availability": availability,
        "availability_files": availability_files,
    }


def _done_recids(out_path: Path) -> set[str]:
    """recids already staged in a prior ``fetched.jsonl`` (record-level resume)."""
    if not out_path.is_file():
        return set()
    return {rec["recid"] for rec in read_jsonl(out_path) if rec.get("recid")}


def _terminal_reject_recids(rejections_path: Path) -> set[str]:
    """recids terminally rejected in a prior run (won't succeed on retry).

    Reading these into the skip set stops fetch from re-downloading + re-extracting
    + re-rejecting the same (often large) archives on every resume, and stops
    rejections.jsonl from growing without bound. Only record-level terminal reasons
    count; transient per-file failures (download errors, 5xx) are left to retry.
    """
    if not rejections_path.is_file():
        return set()
    out: set[str] = set()
    for r in read_jsonl(rejections_path):
        if (r.get("stage") == "fetch" and r.get("reason") in _TERMINAL_REJECT_REASONS
                and isinstance(r.get("id"), str) and ":" not in r["id"]):  # ":" => per-file id
            out.add(r["id"])
    return out


class BudgetExceeded(Exception):
    """Raised by :meth:`StagingBudget.charge` when a limit is reached by data that a
    ``parse``+``purge-raw`` COULD reclaim.

    It aborts the record being staged so :func:`fetch_record` can roll that record back
    and defer it: a record must be staged whole or not at all. Staging it *partially* and
    recording it in ``fetched.jsonl`` would make the partial data permanent (later runs
    skip recids already listed there), and leaving partial files behind un-listed would
    occupy budget that ``purge-raw`` — which only knows about fetched records — could
    never reclaim. Deliberately NOT a subclass of any exception in ``_EXTRACT_ERRORS``,
    so it passes straight through the per-archive error handling.
    """


class StagingBudget:
    """Live, exact accounting of what the harvest has staged — the disk/inode valve.

    **Why not an estimate.** An earlier design booked a record's expected staging up front
    as ``download_size x <a fixed expansion factor>``. That cannot work: the expansion is
    whatever the uploader's compression happened to achieve. Measured on real Zenodo data
    it ranged from ~1x (already-compressed payloads) through 4.1x (a 3.86 GB zip of
    vasprun.xml staging 15.9 GB) to 880x (a synthetic zip of highly compressible text).
    Any single factor is badly wrong somewhere: too low and the budget is silently
    overrun, too high and the harvest paces itself to a crawl.

    So nothing is predicted. Every byte and every file is charged **as it is written**,
    against the real limits, and refunded when it is removed:

    * ``charge`` before/while writing — returns False when the write would breach a limit,
      which makes the caller stop that archive/download cleanly;
    * ``refund`` when a staged file is deleted again (an archive is unlinked as soon as its
      VASP members are extracted, so its bytes are transient, not part of the footprint).

    The result is an exact invariant — ``used <= limit`` at all times, for any compression
    ratio, with no tuning constant. ``peak_bytes``/``peak_files`` record the high-water
    mark so a run can *report* how close it came instead of being audited from outside.

    Thread-safe: ``fetch --workers N`` stages several records concurrently.
    """

    def __init__(self, max_bytes: int | None = None, max_files: int | None = None,
                 used_bytes: int = 0, used_files: int = 0):
        self.max_bytes = max_bytes
        self.max_files = max_files
        # Resume-aware starting point: whatever a previous (unpurged) run left staged.
        self.used_bytes = used_bytes
        self.used_files = used_files
        self.peak_bytes = used_bytes
        self.peak_files = used_files
        self.hit_limit = ""       # "bytes" | "files" — last limit touched (diagnostic)
        # Set only when a limit was reached by data a purge CAN reclaim; that is the
        # signal to stop the run cleanly. Kept separate from hit_limit so a single
        # too-big-for-anything item cannot masquerade as "pause and reclaim".
        self.pause = ""
        self.unfittable = 0       # items no purge could ever make room for
        self._lock = threading.Lock()
        # Per-record context (thread-local: workers stage different records at once).
        # `own` is how much of the tally the record being staged is itself responsible for,
        # which is how a refusal gets classified — see charge().
        self._tls = threading.local()

    @property
    def enabled(self) -> bool:
        return self.max_bytes is not None or self.max_files is not None

    def begin_record(self) -> None:
        """Start a record's own-usage tally (call once per record, per thread)."""
        self._tls.own = [0, 0]
        self._tls.truncated = False

    def record_was_truncated(self) -> bool:
        """Whether the record this thread just staged hit a limit it can never fit in."""
        return bool(getattr(self._tls, "truncated", False))

    def mark_truncated(self) -> None:
        """Flag the current record as not-fitting from another thread's refusal."""
        self._tls.truncated = True

    def own_handle(self) -> list[int]:
        """This record's own-usage tally, to pass to :meth:`charge` from a worker thread."""
        own = getattr(self._tls, "own", None)
        if own is None:
            own = self._tls.own = [0, 0]
        return own  # type: ignore[no-any-return]

    def check(self, n_bytes: int, n_files: int = 0,
              own: list[int] | None = None) -> bool:
        """Would ``n_bytes``/``n_files`` fit? Same verdict as :meth:`charge`, no accounting.

        Used with a *declared* size (an archive header, a manifest entry) purely to skip
        work that clearly cannot fit — never as the accounting itself, which is always
        done against bytes as they are written. Being conservative here is harmless: an
        over-stated size only defers a record that would then be retried.
        """
        return self._fits(n_bytes, n_files, own, commit=False)

    def charge(self, n_bytes: int, n_files: int = 0,
               own: list[int] | None = None) -> bool:
        """Account ``n_bytes``/``n_files`` about to be written.

        Returns True when it fits. Two distinct failure modes:

        * the record could fit on its own, so a ``parse``+``purge-raw`` can make room ->
          sets :attr:`pause` and raises :class:`BudgetExceeded`, and the record is rolled
          back whole and re-fetched later;
        * the record's OWN footprint already exceeds the limit, so no purge could ever help
          -> returns False; the caller skips that item, keeps what landed, and the run goes
          on. Without this split the pacing loop would retry an unstageable record forever.

        ``own`` overrides the per-record tally for callers on a different thread from the
        one that called :meth:`begin_record` (py7zr's extraction workers).
        """
        return self._fits(n_bytes, n_files, own, commit=True)

    def _fits(self, n_bytes: int, n_files: int, own: list[int] | None,
              commit: bool) -> bool:
        if not self.enabled:
            return True
        own = own if own is not None else getattr(self._tls, "own", None)
        with self._lock:
            over = ""
            if self.max_bytes is not None and self.used_bytes + n_bytes > self.max_bytes:
                over = "bytes"
            elif self.max_files is not None and self.used_files + n_files > self.max_files:
                over = "files"
            if over:
                self.hit_limit = over
                # Classify the refusal by the record's OWN footprint, not by what happens
                # to be staged alongside it:
                #
                # * own + this > limit  -> the record cannot be staged even against an
                #   empty budget (note this includes its transient peak: an archive is on
                #   disk while its members are being extracted). Retrying after a purge
                #   would fail identically and re-download it every time — an observed
                #   livelock. So return False: staging stops here, what landed is KEPT, the
                #   record is reported, and the run carries on.
                # * otherwise -> the budget is full of OTHER records' data, which a
                #   parse+purge really can reclaim. Abort for a clean rollback and retry.
                #
                # Deciding this from the record's own usage (rather than from "was the
                # budget empty when I started?") keeps the verdict deterministic under
                # `--workers N`, where a record almost never starts against an empty
                # budget and would otherwise be deferred for ever.
                own_bytes, own_files = own if own is not None else (0, 0)
                cannot_ever_fit = (
                    (over == "bytes" and self.max_bytes is not None
                     and own_bytes + n_bytes > self.max_bytes)
                    or (over == "files" and self.max_files is not None
                        and own_files + n_files > self.max_files))
                if cannot_ever_fit:
                    # Bookkeeping, not accounting: a refusal is a refusal whether it came
                    # from a real charge or from a pre-check that skipped the work.
                    self.unfittable += 1
                    self._tls.truncated = True
                    return False
                # Set even for a non-committing check: raising means a record is being
                # deferred, which IS the signal to stop and let the pacing loop reclaim.
                self.pause = over
                raise BudgetExceeded(
                    f"{over} limit reached (used {self.used_bytes} B / {self.used_files} "
                    f"files); parse+purge to reclaim, then resume")
            if commit:
                self.used_bytes += n_bytes
                self.used_files += n_files
                self.peak_bytes = max(self.peak_bytes, self.used_bytes)
                self.peak_files = max(self.peak_files, self.used_files)
                if own is not None:
                    own[0] += n_bytes
                    own[1] += n_files
            return True

    def refund(self, n_bytes: int, n_files: int = 0,
               own: list[int] | None = None) -> None:
        """Give back space for staged data that has just been deleted."""
        if not self.enabled:
            return
        own = own if own is not None else getattr(self._tls, "own", None)
        with self._lock:
            self.used_bytes = max(0, self.used_bytes - n_bytes)
            self.used_files = max(0, self.used_files - n_files)
            if own is not None:
                own[0] = max(0, own[0] - n_bytes)
                own[1] = max(0, own[1] - n_files)

    def full(self) -> bool:
        """Whether the run should stop cleanly so the pacing loop can parse+purge."""
        return bool(self.pause)

    def stats(self) -> dict:
        return {"peak_staged_bytes": self.peak_bytes, "peak_staged_files": self.peak_files,
                "staged_bytes_now": self.used_bytes, "staged_files_now": self.used_files}


def _fetch_parallel(
    records: Any, done: set[str], raw_dir: Path, max_bytes: int | None,
    rej: RejectionLogger, max_member_bytes: int, token: str | None,
    max_records: int | None, budget: StagingBudget, out: JsonlWriter, stats: dict,
    workers: int, zip_stream: bool = True,
    zip_stream_max_files: int = DEFAULT_ZIP_STREAM_MAX_FILES,
) -> None:
    """Record-level parallel fetch. Records are independent (each stages into its own
    ``raw_dir/<recid>/``), so a thread pool of ``workers`` overlaps their downloads.
    Only ``rej`` is shared into worker threads (guarded by a lock); ``out``/``stats``
    are touched solely by this (main) thread as futures complete, so they need no lock."""
    tls = threading.local()
    sessions: list[requests.Session] = []
    sess_lock = threading.Lock()
    rej_lock = threading.Lock()

    def _session_for_thread() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = _session(token)
            tls.s = s
            with sess_lock:
                sessions.append(s)
        return s

    class _LockedRej:
        def reject(self, *a: Any, **k: Any) -> None:
            with rej_lock:
                rej.reject(*a, **k)

    locked_rej = _LockedRej()

    def _process(rec: dict) -> tuple[dict, dict | None]:
        # The budget is charged inside fetch_record as bytes/files actually land, so
        # workers need no reservation and no post-hoc settling — the accounting is exact
        # and shared (StagingBudget is thread-safe).
        try:
            entry = fetch_record(rec, _session_for_thread(), raw_dir, max_bytes,
                                 locked_rej, max_member_bytes,  # type: ignore[arg-type]
                                 budget, zip_stream, zip_stream_max_files)
            return rec, entry
        except Exception:
            logger.exception("fetch worker crashed on %s", rec.get("recid"))
            return rec, None

    def _handle(fut: Any) -> None:
        rec, entry = fut.result()
        if entry:
            out.write(entry)
            stats["fetched"] += 1
            stats["calc_units"] += entry["n_calc_units"]

    inflight: set = set()
    submitted = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for rec in records:
                stats["records"] += 1
                if rec["recid"] in done:
                    stats["skipped_existing"] += 1
                    continue
                # Count SUBMITTED records, not completed ones: stats["fetched"] only
                # advances when a future is harvested, so gating on it would overshoot
                # `max_records` by however many are still in flight.
                if max_records and submitted >= max_records:
                    break
                # Stop between records once a limit has been reached by reclaimable data.
                if budget.full():
                    stats["stopped_disk_budget"] = True
                    stats["stopped_on"] = budget.hit_limit
                    logger.info("disk budget (%s) reached; stopping — parse+purge then "
                                "resume", budget.hit_limit)
                    break
                inflight.add(ex.submit(_process, rec))
                submitted += 1
                if len(inflight) >= workers * 2:   # drain to bound memory + refresh stats
                    finished, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    for f in finished:
                        _handle(f)
            for f in wait(inflight)[0]:            # drain the rest
                _handle(f)
    finally:
        for s in sessions:
            s.close()


def fetch(
    in_path: str | Path,
    out_path: str | Path = config.MANIFEST_DIR / "fetched.jsonl",
    raw_dir: str | Path = config.RAW_DIR,
    rejections_path: str | Path = config.MANIFEST_DIR / "rejections.jsonl",
    max_bytes: int | None = 500_000_000,
    max_records: int | None = None,
    token: str | None = None,
    retry_rejected: bool = False,
    max_member_bytes: int = DEFAULT_MAX_MEMBER_BYTES,
    max_disk_bytes: int | None = None,
    max_disk_files: int | None = None,
    workers: int = 1,
    zip_stream: bool = True,
    zip_stream_max_files: int = DEFAULT_ZIP_STREAM_MAX_FILES,
) -> dict:
    """Fetch all records in ``in_path`` (a triaged keep-list).

    Resumable at the record level: any recid already staged in ``out_path`` — or
    terminally rejected in ``rejections_path`` (unless ``retry_rejected``) — is
    skipped, so a re-run after an interrupted harvest neither re-downloads it nor
    appends duplicate manifest/rejection lines. Pass ``retry_rejected=True`` to
    reprocess previously-rejected records (e.g. after raising ``max_bytes`` or
    installing the ``archives`` extra). ``max_records`` caps records fetched *this
    run* (newly staged), not counting resumed skips.

    ``max_bytes`` caps each downloaded file/archive (``None`` = no cap); the archive
    is deleted right after its VASP files are extracted, so persistent disk is the
    extracted files, not the archive. ``max_member_bytes`` caps each single extracted
    file (guards against decompression bombs / a runaway multi-GB vasprun.xml).

    ``max_disk_bytes`` (``None`` = no limit) is a **disk-budget valve**: before starting
    each new record, fetch measures the staging tree (``raw_dir``) and stops cleanly once
    it reaches the budget, setting ``stopped_disk_budget`` in the returned stats. Because
    fetch is resumable, the intended use is an uncapped (``max_bytes=0``) harvest paced in
    place: loop ``fetch (stops at budget) -> parse -> purge-raw -> fetch (resume) -> ...``
    so persistent staging never exceeds the budget.

    **Sizing it.** The budget bounds the ENTIRE ``raw_dir``, so it needs no per-batch
    division — `pipeline`'s two concurrently-staged batches are both inside this one
    number. Enforcement is exact (every byte charged as written, see
    :class:`StagingBudget`), so the only headroom the budget itself does not cover is:

    * the dataset dir (extxyz.gz shards + metadata) if it shares the same quota
      (est. 15-75 GB for a full Zenodo harvest);
    * filesystem overhead / other users of the volume.

    ~0.8 x quota is a sensible setting for a 1 TB quota.

    ``max_disk_files`` is the same valve for **inodes**, and on CSD3 it is the binding one:
    ``hpc-work`` allows 1 TB *and* 1M files, while extracted VASP trees measured ~270 KiB
    mean / 7.6 KiB median per file (screening uploads are thousands of tiny per-calc dirs),
    so 1M files can be reached around ~0.3 TB. Without it, hitting the inode quota shows up
    only as per-file EDQUOT write errors, which fetch treats as transient — the harvest
    would quietly under-collect rather than pause for a purge.

    Both limits are enforced on ACTUAL usage rather than any predicted decompression
    expansion: measured ratios on real Zenodo records span ~1x to 4.1x (and a synthetic
    zip reached 880x), so no fixed factor is safe. A file that cannot fit even in an empty
    budget is rejected individually (``disk_budget_reached``, counted as
    ``items_over_whole_budget``) instead of stopping the run, so the pacing loop can never
    spin on it.

    ``workers`` (default 1) runs that many record downloads concurrently via a thread
    pool — records are independent (each stages into its own ``raw_dir/<recid>/``), and
    downloads are I/O-bound, so this is the main throughput lever for a big harvest. Peak
    disk still respects ``max_disk_bytes`` with ``workers>1`` because the shared,
    thread-safe :class:`StagingBudget` charges every byte as it is written (no up-front
    reservation and no per-worker split of the budget is needed — the accounting is exact
    across all in-flight downloads); keep ``workers`` small (~4) to stay well within
    Zenodo's global 100 req/min, 5000 req/hour authenticated limits.

    The returned stats include the run's own high-water marks — ``peak_staged_bytes`` and
    ``peak_staged_files`` — plus ``staged_bytes_now``/``staged_files_now`` and
    ``items_over_whole_budget``. Cluster scratch is inode-limited as well as byte-limited
    (CSD3 ``hpc-work``: 1 TB **and 1M files**), so both are reported.
    """
    out_path, raw_dir = Path(out_path), Path(raw_dir)
    rejections_path = Path(rejections_path)
    done = _done_recids(out_path)
    if not retry_rejected:
        done |= _terminal_reject_recids(rejections_path)
    rej = RejectionLogger(rejections_path)
    stats: dict[str, Any] = {"records": 0, "fetched": 0, "skipped_existing": 0,
                             "calc_units": 0, "stopped_disk_budget": False,
                             "stopped_on": ""}
    token = token or os.environ.get("ZENODO_TOKEN")

    # ONE walk of the staging tree seeds the budget with whatever a previous, not-yet-purged
    # run left behind (resume-aware). From here on the accounting is incremental and exact:
    # every byte/file is charged as it is written and refunded when deleted, so no estimate
    # of compression expansion is involved anywhere.
    # Create the staging root FIRST: it is the accounting root (a walk of it never counts
    # the root itself), so leaving it to be created on demand would charge one inode that
    # no later measurement of the tree can see, and the tally would drift from reality.
    raw_dir.mkdir(parents=True, exist_ok=True)
    base_bytes, base_files = (_dir_usage(raw_dir) if (max_disk_bytes or max_disk_files)
                              else (0, 0))
    budget = StagingBudget(max_disk_bytes, max_disk_files, base_bytes, base_files)

    def _serial(count_records: bool) -> None:
        with _session(token) as session, JsonlWriter(out_path) as out:
            for rec in read_jsonl(in_path):
                if count_records:
                    stats["records"] += 1
                if rec["recid"] in done:
                    if count_records:
                        stats["skipped_existing"] += 1
                    continue
                # Stop between records once a limit is reached by data a purge can reclaim.
                # (A record too big for the WHOLE budget does not stop the run — it is
                # reported individually, so the pacing loop cannot spin on it forever.)
                if budget.full():
                    stats["stopped_disk_budget"] = True
                    stats["stopped_on"] = budget.hit_limit
                    logger.info("disk budget (%s) reached (staged %d B / %d files); "
                                "stopping — parse+purge then resume", budget.hit_limit,
                                budget.used_bytes, budget.used_files)
                    break
                entry = fetch_record(rec, session, raw_dir, max_bytes, rej,
                                     max_member_bytes, budget, zip_stream,
                                     zip_stream_max_files)
                if entry:
                    out.write(entry)
                    stats["fetched"] += 1
                    stats["calc_units"] += entry["n_calc_units"]
                    done.add(rec["recid"])
                if max_records and stats["fetched"] >= max_records:
                    break

    if workers > 1:
        with JsonlWriter(out_path) as out:
            _fetch_parallel(read_jsonl(in_path), done, raw_dir, max_bytes, rej,
                            max_member_bytes, token, max_records, budget, out, stats,
                            workers, zip_stream, zip_stream_max_files)
        # Guaranteed forward progress. If a whole parallel pass staged NOTHING and stopped
        # on the budget, then the records in flight each fitted individually but filled the
        # budget between them, and every one of them was rolled back. Handing the same
        # batch back to the pacing loop would repeat that for ever (there is nothing new to
        # purge), so retry serially: alone against a budget the rollbacks have just
        # emptied, a record either stages or is recognised as too big for the budget at
        # all. Either way the harvest moves, and no record is dropped.
        if stats["fetched"] == 0 and budget.full():
            logger.warning("parallel pass staged nothing (%s limit); retrying serially so "
                           "the harvest cannot stall", budget.hit_limit)
            stats["serial_fallback"] = True
            # Clear the pause so the fallback can run; the authoritative post-loop check
            # below re-derives whether the run really ended on the budget.
            budget.pause = ""
            stats["stopped_disk_budget"], stats["stopped_on"] = False, ""
            _serial(count_records=False)
    else:
        _serial(count_records=True)

    rej.close()
    stats["rejections"] = rej.n
    # Authoritative check, AFTER all workers have finished: a limit can be reached by a
    # worker after the main loop made its last admission decision (or by the very last
    # record), and the flag is what tells `pipeline` to reclaim and resume this part —
    # records deferred by the budget are absent from the manifest, so they are re-fetched.
    if budget.full():
        stats["stopped_disk_budget"] = True
        stats["stopped_on"] = budget.pause
    # The run reports its own high-water mark, so "did we stay inside the limits?" is
    # answerable from the summary rather than needing external monitoring.
    stats.update(budget.stats())
    stats["items_over_whole_budget"] = budget.unfittable
    logger.info("fetch: %s", stats)
    return {"out_path": str(out_path), **stats}
