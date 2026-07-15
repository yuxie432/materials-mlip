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
from .models import ARCHIVE_EXTS, VASP_PRIMARY, _VASP_RE

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
        r = session.get(url, headers={"Range": range_header}, timeout=60)
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
    """
    session = session or requests.Session()
    try:
        r = _ranged_get(session, url, f"bytes=-{tail}")  # retries 429 (see _ranged_get)
        if r is None or r.status_code != 206:
            return None
        blob = r.content
        cr = r.headers.get("Content-Range", "")
        size = int(cr.split("/")[-1]) if "/" in cr else len(blob)
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
    vasp, primary = [], []
    for n in names:
        base = n.rsplit("/", 1)[-1]
        m = _VASP_RE.search(base)
        if m:
            vasp.append(n)
            if m.group(1).lower() in VASP_PRIMARY:
                primary.append(n)
    return {"vasp_files": vasp, "primary_vasp_files": primary}


# ---------------------------------------------------------------------------
# Triage driver
# ---------------------------------------------------------------------------

def triage(
    in_path: str | Path,
    out_path: str | Path,
    min_rank: int = 3,
    peek: bool = False,
    peek_max_bytes: int = 20_000_000_000,
    require_confirmed: bool = False,
    peek_interval: float = _PEEK_MIN_INTERVAL,
) -> dict:
    """Filter candidates to a keep-list.

    Parameters
    ----------
    min_rank:
        Minimum ``vasp_rank`` to keep (4=vasp_direct, 3=archive, 2=processed).
    peek:
        Attempt remote ZIP central-directory inspection on ``archive`` records.
    require_confirmed:
        If True, drop an ``archive`` record that peeking could not confirm — but
        *only* when every archive in it was peekable (``.zip``). A ``.tar``/``.tar.gz``/
        ``.rar``/``.7z`` cannot be range-peeked yet fetch can still extract it, so
        such records are kept (confirmed at download) rather than silently discarded.
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
    stats: dict[str, int] = {"seen": 0, "kept": 0, "peeked": 0, "peek_confirmed": 0}
    with out_path.open("w") as out:
        for rec in read_jsonl(in_path):
            stats["seen"] += 1
            if rec["vasp_rank"] < min_rank:
                continue
            confirmed = bool(rec.get("primary_vasp_files"))
            if peek and rec["vasp_category"] == "archive" and not confirmed:
                for f in rec["files"]:
                    if (f.get("ext") == ".zip") and (f.get("size") or 0) <= peek_max_bytes and f.get("download"):
                        wait = peek_interval - (time.monotonic() - last_peek)
                        if wait > 0:
                            time.sleep(wait)
                        names = peek_zip_filenames(f["download"], session)
                        last_peek = time.monotonic()
                        stats["peeked"] += 1
                        if names:
                            hits = _zip_vasp_hits(names)
                            if hits["vasp_files"]:
                                rec["vasp_files"] = hits["vasp_files"]
                                rec["primary_vasp_files"] = hits["primary_vasp_files"]
                                rec["signals"].append(f"peek confirmed VASP files in {f['key']}")
                                rec["vasp_category"] = "vasp_direct"
                                rec["vasp_rank"] = 4
                                confirmed = bool(hits["primary_vasp_files"])
                                stats["peek_confirmed"] += 1
                                break
            if require_confirmed and rec["vasp_category"] == "archive" and not confirmed:
                # Only drop if we actually had a chance to confirm: every archive was
                # peekable (.zip) and came back without VASP files. If any archive is
                # unpeekable (tar/rar/7z), keep it — fetch confirms it at download.
                if not any(f.get("ext") in _UNPEEKABLE_ARCHIVE_EXTS for f in rec["files"]):
                    continue
            out.write(json.dumps(rec) + "\n")
            kept += 1
    stats["kept"] = kept
    logger.info("triage: %s", stats)
    return {"in_path": str(in_path), "out_path": str(out_path), **stats}
