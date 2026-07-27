"""Stage 1 — triage.

Read the candidate manifest and decide what to keep for download. Two levels:

1. Cheap, offline: rank by the file-listing classification already computed in
   discovery (:func:`models.classify_files`) plus metadata-text signals.
2. Optional ``--peek``: for records whose VASP data is hidden inside a ``.zip``,
   read just the zip *central directory* over HTTP Range (tail of the file) to
   list the contained filenames — confirming ``vasprun.xml``/``OUTCAR`` without
   downloading gigabytes. Only ZIP is randomly peekable; ``.tar.gz``/``.rar``
   are not, and are left as "needs download to confirm".
"""

from __future__ import annotations

import json
import logging
import struct
import time
from pathlib import Path

import requests

from .client import _parse_retry_after
from .manifest import read_jsonl
from .models import ARCHIVE_EXTS, VASP_PRIMARY, _ext, _VASP_RE

logger = logging.getLogger(__name__)

# Only ZIP has a tail central directory we can read over HTTP Range; every other
# archive format must be downloaded before its contents are knowable.
_UNPEEKABLE_ARCHIVE_EXTS = ARCHIVE_EXTS - {".zip"}

EOCD_SIG = b"\x50\x4b\x05\x06"  # end of central directory
CDH_SIG = 0x02014b50            # central directory file header

# Range GETs for the zip peek share Zenodo's 30/min search budget, so a peek can
# be rate-limited (429) just like a search. Retry it (bounded) rather than giving
# up — see _ranged_get.
_PEEK_MAX_ATTEMPTS = 3
# Polite spacing (seconds) between successive peek requests in the triage loop.
_PEEK_MIN_INTERVAL = 1.0


# ---------------------------------------------------------------------------
# Remote ZIP central-directory reader (best-effort, no full download).
# ---------------------------------------------------------------------------

def _ranged_get(session: requests.Session, url: str, range_header: str,
                max_attempts: int = _PEEK_MAX_ATTEMPTS) -> requests.Response | None:
    """A single Range GET, honouring 429 + Retry-After (sleep, retry, ≤ attempts).

    A 429 here is throttling, not "peek failed". Treating it as failure is unsafe:
    under ``--require-confirmed`` a peek that comes back empty silently DROPS the
    record, so a rate-limited peek would be indistinguishable from "peeked, no VASP
    inside" — a real record lost to transient throttling. So back off on Retry-After
    and retry. Returns the response (caller checks for 206), or None if still 429
    after the budget is spent.
    """
    for _ in range(max_attempts):
        # stream=True so the caller can inspect headers (Content-Range) *before* the
        # body is read — needed to detect Zenodo's small-file suffix-range underflow
        # (see peek_zip_filenames) without tripping the broken body read.
        r = session.get(url, headers={"Range": range_header}, timeout=60, stream=True)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After")) + 1
        logger.warning("peek rate limited; sleeping %ss", wait)
        time.sleep(wait)
    return None


def _range_get(session: requests.Session, url: str, start: int, end: int) -> bytes | None:
    r = _ranged_get(session, url, f"bytes={start}-{end}")
    if r is None or r.status_code != 206:  # 200 => Range ignored; refuse whole (GB) file
        return None
    return r.content


def peek_zip_filenames(url: str, session: requests.Session | None = None, tail: int = 65_536) -> list[str] | None:
    """List filenames inside a remote ZIP by reading its central directory.

    Uses a single HTTP *suffix-range* request (``Range: bytes=-N``) to grab the
    tail of the file plus the total size (from ``Content-Range``); Zenodo serves
    206 for these even though it omits an ``Accept-Ranges`` header. Returns None
    if Range isn't honoured, the file isn't a plain ZIP, or it's ZIP64 in a form
    we don't parse. Best-effort by design.

    Small-file guard: when the zip is *smaller* than ``tail``, Zenodo mishandles the
    suffix range — it returns a 206 whose ``Content-Range`` start has underflowed
    (``size - tail`` wrapped to ~2**64) and a ``Content-Length`` it cannot deliver, so
    reading the body raises ``ChunkedEncodingError``/``IncompleteRead``. Every zip under
    ``tail`` (64 KiB) would otherwise be dropped as an un-peekable failure (measured
    ~15% of records — mostly small input/structure zips). When the tail body read
    breaks we re-fetch the whole (small) file instead, which is cheap and parseable. A
    correct server that simply returns the whole short file for an over-long suffix
    range reads fine and needs no second request.
    """
    session = session or requests.Session()
    try:
        r = _ranged_get(session, url, f"bytes=-{tail}")  # retries 429 (see _ranged_get)
        if r is None or r.status_code != 206:
            return None
        cr = r.headers.get("Content-Range", "")
        size = int(cr.split("/")[-1]) if "/" in cr else None
        try:
            blob = r.content
        except requests.RequestException:
            # Zenodo's small-file suffix underflow: the 206 promises a body it cannot
            # deliver, so the read breaks. Only a file SMALLER than the tail hits this
            # (a real tail of a big file reads fine), so re-fetch the whole small file.
            r.close()
            whole = session.get(url, timeout=60)
            if whole.status_code != 200:
                return None
            blob = whole.content
            size = len(blob)
        if size is None:
            size = len(blob)
        blob_start = size - len(blob)  # absolute offset of blob[0] within the file

        idx = blob.rfind(EOCD_SIG)
        if idx == -1:
            return None
        eocd = blob[idx:idx + 22]
        _, _, _, _, total, cd_size, cd_off, _ = struct.unpack("<IHHHHIIH", eocd)
        if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
            return None  # ZIP64 — skip (rare for these archives); confirm via download

        cd: bytes | None
        if cd_off >= blob_start:  # central directory already in the tail we fetched
            cd = blob[cd_off - blob_start: cd_off - blob_start + cd_size]
        else:
            cd = _range_get(session, url, cd_off, cd_off + cd_size - 1)
        if not cd:
            return None
        names: list[str] = []
        p = 0
        for _ in range(total):
            if p + 46 > len(cd) or struct.unpack("<I", cd[p:p + 4])[0] != CDH_SIG:
                break
            n_len, m_len, k_len = struct.unpack("<HHH", cd[p + 28:p + 34])
            name = cd[p + 46:p + 46 + n_len].decode("utf-8", "replace")
            names.append(name)
            p += 46 + n_len + m_len + k_len
        return names
    except (requests.RequestException, struct.error, ValueError) as exc:
        logger.debug("zip peek failed for %s: %s", url, exc)
        return None


def _zip_vasp_hits(names: list[str]) -> dict[str, list[str]]:
    vasp, primary, nested = [], [], []
    for n in names:
        base = n.rsplit("/", 1)[-1]
        m = _VASP_RE.search(base)
        if m:
            vasp.append(n)
            if m.group(1).lower() in VASP_PRIMARY:
                primary.append(n)
        # A nested archive (a .tar.gz/.zip/… *inside* the peeked zip) hides its own
        # listing from the central-directory peek, so "no VASP name visible" is NOT
        # proof of no VASP — see _peek_record, which refuses to negatively-confirm.
        if _ext(base) in ARCHIVE_EXTS:
            nested.append(n)
    return {"vasp_files": vasp, "primary_vasp_files": primary, "nested_archives": nested}


# ---------------------------------------------------------------------------
# Triage driver
# ---------------------------------------------------------------------------

def _peek_record(rec: dict, session: requests.Session, peek_max_bytes: int,
                 peek_interval: float, last_peek: float,
                 stats: dict[str, int]) -> tuple[bool, bool, float]:
    """Peek a record's ``.zip`` archives, upgrading it in place when VASP is found.

    Peeking a zip reads only its central directory over an HTTP Range request
    (~tens of KB) — thousands of times cheaper than downloading the whole archive —
    so it is done by default (mentor/user 2026-07-21). Returns
    ``(confirmed, negatively_confirmed, last_peek)``:

    * ``confirmed`` — a VASP file was found inside (record upgraded to ``vasp_direct``).
    * ``negatively_confirmed`` — we obtained a COMPLETE, SUCCESSFUL peek of every
      archive (all were ``.zip``, all listings read) and none contained any VASP file.
      This is the only case safe to drop on: it is positive evidence of "no VASP",
      never a failed/absent peek. False if any archive was unpeekable (tar/rar/7z),
      any zip lacked a usable download link / exceeded ``peek_max_bytes``, any peek
      request failed, or any peeked zip held a *nested* archive (whose contents the
      central-directory peek cannot see) — in all those cases we keep the record and
      let fetch decide.
    """
    files = rec.get("files", [])
    archives = [f for f in files if f.get("ext") in ARCHIVE_EXTS]
    has_unpeekable = any(f.get("ext") in _UNPEEKABLE_ARCHIVE_EXTS for f in archives)
    peekable_zips = [f for f in archives if f.get("ext") == ".zip"
                     and f.get("download") and (f.get("size") or 0) <= peek_max_bytes]
    # a zip we cannot even attempt (no link / too big) is an evidence gap -> keep
    unknown_zip = any(f.get("ext") == ".zip" and f not in peekable_zips for f in archives)

    peek_failed = False
    saw_nested = False
    peeked_ok = 0
    for f in peekable_zips:
        wait = peek_interval - (time.monotonic() - last_peek)
        if wait > 0:
            time.sleep(wait)
        names = peek_zip_filenames(f["download"], session)
        last_peek = time.monotonic()
        stats["peeked"] += 1
        if names is None:
            peek_failed = True
            continue
        peeked_ok += 1
        hits = _zip_vasp_hits(names)
        if hits["vasp_files"]:
            rec["vasp_files"] = hits["vasp_files"]
            rec["primary_vasp_files"] = hits["primary_vasp_files"]
            rec["signals"].append(f"peek confirmed VASP files in {f['key']}")
            rec["vasp_category"] = "vasp_direct"
            rec["vasp_rank"] = 4
            stats["peek_confirmed"] += 1
            return True, False, last_peek
        if hits["nested_archives"]:
            saw_nested = True  # peek can't see inside a nested archive -> evidence gap

    negatively_confirmed = (
        not has_unpeekable and not unknown_zip and not peek_failed and not saw_nested
        and peeked_ok > 0 and peeked_ok == len(archives)
    )
    return False, negatively_confirmed, last_peek


def triage(
    in_path: str | Path,
    out_path: str | Path,
    min_rank: int = 3,
    peek: bool = True,
    peek_max_bytes: int = 20_000_000_000,
    require_confirmed: bool = True,
    peek_interval: float = _PEEK_MIN_INTERVAL,
) -> dict:
    """Filter candidates to a keep-list.

    Peeking is ON by default and drops archives PROVEN to hold no VASP — because a
    zip peek costs ~tens of KB (an HTTP Range read of the central directory) while
    blindly downloading the archive can cost hundreds of MB to many GB. Measured
    2026-07-21: without this, a full harvest would download tens of thousands of
    non-VASP archives (worm-tracking, genomes, GC-MS, …) before rejecting them.

    Parameters
    ----------
    min_rank:
        Minimum ``vasp_rank`` to keep (4=vasp_direct, 3=archive, 2=processed).
    peek:
        Peek remote ``.zip`` central directories on ``archive`` records (default True;
        ``--no-peek`` disables). Recall-neutral: it only UPGRADES a record to
        ``vasp_direct`` when VASP files are found.
    require_confirmed:
        Default True: DROP an archive record only when peeking gives positive evidence
        of no VASP — every archive was a peekable ``.zip``, every listing read, and none
        contained a VASP file (see :func:`_peek_record`). FAIL-SAFE: a record is kept if
        any archive is unpeekable (tar/rar/7z), any zip could not be fetched/was too big,
        or any peek request failed — fetch confirms those at download. Set False
        (``--keep-unconfirmed``) to keep every rank>=min_rank record (peek then only
        upgrades, never drops). Ignored when ``peek`` is False.
    peek_interval:
        Minimum seconds between successive peek requests (polite spacing; peek Range
        GETs share Zenodo's 30/min search budget, so don't burst them). Set 0 in tests.
    """
    in_path, out_path = Path(in_path), Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "zenodo-harvest/0.1 (triage)"

    kept = 0
    last_peek = 0.0
    stats: dict[str, int] = {"seen": 0, "kept": 0, "peeked": 0, "peek_confirmed": 0,
                             "dropped_no_vasp": 0}
    with out_path.open("w") as out:
        for rec in read_jsonl(in_path):
            stats["seen"] += 1
            if rec["vasp_rank"] < min_rank:
                continue
            confirmed = bool(rec.get("primary_vasp_files"))
            negatively_confirmed = False
            if peek and rec["vasp_category"] == "archive" and not confirmed:
                confirmed, negatively_confirmed, last_peek = _peek_record(
                    rec, session, peek_max_bytes, peek_interval, last_peek, stats)
            if (require_confirmed and peek and negatively_confirmed
                    and rec["vasp_category"] == "archive" and not confirmed):
                stats["dropped_no_vasp"] += 1
                continue
            out.write(json.dumps(rec) + "\n")
            kept += 1
    stats["kept"] = kept
    logger.info("triage: %s", stats)
    return {"in_path": str(in_path), "out_path": str(out_path), **stats}
