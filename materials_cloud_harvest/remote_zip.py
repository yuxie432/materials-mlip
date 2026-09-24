"""Remote ZIP / AiiDA-export inspection over HTTP Range — ZIP64-aware, no download.

Materials Cloud serves files through a 302 to a presigned CSCS S3 URL that honours Range, so a
zip's *central directory* (its tail index of every member's name/size/offset) can be read without
downloading the archive — the same trick as ``zenodo_harvest.zipstream`` / ``triage``, but:

* **ZIP64-aware.** MC's big archives (multi-GB zips, and AiiDA exports with 10^5+ members — the
  Bosoni VASP export has 359,272) use the ZIP64 end-of-central-directory, which the Zenodo
  32-bit peek reports as "unpeekable". The member parser is NOMAD's tested ZIP64 one
  (``nomad_harvest.upload_zip._parse_central_directory``), reused, not copied.
* **AiiDA-aware.** An ``*.aiida`` file is an AiiDA archive. Two on-disk formats occur on MC
  (live-verified 2026-09-23): the **legacy** export (aiida-core 1.x, export_version 0.x) is a zip
  whose members keep their REAL names — ``nodes/<uuid-shards>/path/vasprun.xml`` — so VASP outputs
  inside are visible to the peek and extractable by the ordinary zip machinery; the **sqlite_zip**
  archive (aiida-core ≥2.0) stores files as content-addressed ``repo/<sha256>`` blobs whose names
  live only in its ``db.sqlite3`` — so the peek pulls that one member over Range and queries it
  (:func:`sqlite_evidence`, the shared ``zenodo_harvest.aiida_archive`` rules the fetch extractor
  uses too). :func:`aiida_format` tells the formats apart from the member names.
"""

from __future__ import annotations

import logging
import os
import struct
import tempfile
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path

import requests

from nomad_harvest.upload_zip import ZipMember, _parse_central_directory
from zenodo_harvest import aiida_archive
from zenodo_harvest.aiida_archive import archive_format
from zenodo_harvest.fetch import (
    _PARSE_RE,
    _PRIMARY_ROLES,
    _is_junk_member,
    _is_primary_name,
    _nested_archive_kind,
    _unit_role,
)

logger = logging.getLogger(__name__)

_EOCD_SIG = b"PK\x05\x06"
_EOCD64_LOC_SIG = b"PK\x06\x07"
_EOCD64_SIG = b"PK\x06\x06"
_LFH_SIG = b"PK\x03\x04"
_SENTINEL32 = 0xFFFFFFFF
DEFAULT_TAIL = 1 << 20              # the EOCD (+ZIP64 locator/record) is always in the last ~64 KiB
DEFAULT_MAX_CD_BYTES = 1 << 30      # refuse to pull a central directory bigger than this (1 GiB)
# Largest sqlite_zip database a peek pulls (the CSD3 census: 4 MB - 1.14 GB; the whole set ~7 min).
DEFAULT_MAX_DB_BYTES = 2 << 30
_RETRY_STATUS = {429, 500, 502, 503, 504}


class RemoteZipError(Exception):
    """The remote file could not be enumerated as a zip (Range refused, not a zip, too big…)."""


def _ranged_get(session: requests.Session, url: str, spec: str, stream: bool = False,
                max_attempts: int = 4, timeout: float = 300) -> requests.Response:
    """One Range GET (redirect-following), retrying 429/5xx/network errors; returns a 206
    response or raises :class:`RemoteZipError`."""
    last = ""
    for attempt in range(max_attempts):
        try:
            r = session.get(url, headers={"Range": spec}, timeout=timeout, stream=stream)
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
            time.sleep(min(2 ** (attempt + 1), 30))
            continue
        if r.status_code == 206:
            return r
        r.close()
        last = f"HTTP {r.status_code}"
        if r.status_code in _RETRY_STATUS:
            ra = r.headers.get("Retry-After")
            try:
                wait = float(ra) + 1 if ra else min(2 ** (attempt + 1), 30)
            except ValueError:
                wait = min(2 ** (attempt + 1), 30)
            time.sleep(min(wait, 120))
            continue
        break  # 200 (Range ignored), 403/404, … — not retryable
    raise RemoteZipError(f"range {spec} failed: {last}")


def _range_bytes(session: requests.Session, url: str, start: int, end: int) -> bytes:
    with _ranged_get(session, url, f"bytes={start}-{end}") as r:
        return r.content


def _find_eocd(blob: bytes) -> int:
    """Offset of the end-of-central-directory record in a file TAIL ``blob`` (which ends at EOF).

    The last ``PK\\x05\\x06`` is not necessarily the record: an archive comment may contain the
    signature. A genuine record's comment-length field makes it end exactly at EOF, so candidates
    are tried from the end until one is consistent (falling back to the last one, like
    ``zipfile``, for a writer that mis-states the comment length). -1 if there is none."""
    last = -1
    end = len(blob)
    while True:
        idx = blob.rfind(_EOCD_SIG, 0, end)
        if idx < 0:
            return last
        if idx + 22 <= len(blob):
            if last < 0:
                last = idx
            (comment_len,) = struct.unpack("<H", blob[idx + 20:idx + 22])
            if idx + 22 + comment_len == len(blob):
                return idx
        end = idx


def read_central_directory(session: requests.Session, url: str, tail: int = DEFAULT_TAIL,
                           max_cd_bytes: int = DEFAULT_MAX_CD_BYTES
                           ) -> tuple[list[ZipMember], int]:
    """Enumerate a remote zip's members: ``(members, total_size)``. ZIP64-aware.

    One suffix-Range read fetches the tail (EOCD, and for ZIP64 the locator + ZIP64 EOCD record,
    and for small archives the whole central directory too); the central directory is read by
    offset only when it lies before that tail. Mirrors ``zipfile``'s tolerance of data PREPENDED to
    the archive (self-extractors, concatenations): the offset shift is derived from where the end
    records actually sit, and member offsets are corrected by it. Validates what it read — the
    first header signature and the member count against the end record — so a misparse can never
    masquerade as "an archive with no VASP inside" (the caller would prune it on that verdict).
    Raises :class:`RemoteZipError` when the file is not a zip we can address (Range refused, no
    EOCD — e.g. a tar-format AiiDA export —, an inconsistent directory, or one above
    ``max_cd_bytes``)."""
    with _ranged_get(session, url, f"bytes=-{tail}") as r:
        cr = r.headers.get("Content-Range", "")
        blob = r.content
    if "/" not in cr:
        raise RemoteZipError("no Content-Range on the tail read")
    try:
        total = int(cr.split("/")[-1])
    except ValueError as exc:
        raise RemoteZipError(f"bad Content-Range {cr!r}") from exc
    blob_start = total - len(blob)
    idx = _find_eocd(blob)
    if idx < 0:
        raise RemoteZipError("no end-of-central-directory record (not a zip?)")
    eocd_abs = blob_start + idx
    n_entries, cd_size, cd_off = struct.unpack("<HII", blob[idx + 10:idx + 20])
    sentinel = cd_off == _SENTINEL32 or cd_size == _SENTINEL32 or n_entries == 0xFFFF
    # Like zipfile, the ZIP64 end records are used whenever their LOCATOR sits right before the
    # EOCD — not only when a 32-bit field holds a sentinel. CPython's own zipfile writes ZIP64 end
    # records as soon as the central directory starts past 2 GiB (its ZIP64_LIMIT) yet keeps the
    # real, still-32-bit offset in the EOCD, and some writers always emit them: ignoring the locator
    # there misplaces the central directory by the 76 bytes of ZIP64 records (CSD3 census
    # 2026-09-24: 25 MC archives of 0.09-4 GiB failed exactly so, incl. 14 AiiDA exports).
    loc = blob[idx - 20:idx] if idx >= 20 else b""
    zip64 = False
    end_of_cd_abs = eocd_abs              # where the central directory must end (+ any shift)
    if loc[:4] == _EOCD64_LOC_SIG:
        # the ZIP64 record normally sits immediately BEFORE the locator (robust to prepended
        # data); else at the locator's recorded offset (extensible data in between)
        for z64_abs in (eocd_abs - 20 - 56, struct.unpack("<Q", loc[8:16])[0]):
            if z64_abs < 0:
                continue
            if z64_abs >= blob_start:
                z = blob[z64_abs - blob_start:z64_abs - blob_start + 56]
            else:
                z = _range_bytes(session, url, z64_abs, z64_abs + 55)
            if z[:4] == _EOCD64_SIG and len(z) >= 56:
                n_entries, cd_size, cd_off = struct.unpack("<QQQ", z[32:56])
                zip64, end_of_cd_abs = True, z64_abs
                break
    if sentinel and not zip64:
        raise RemoteZipError("ZIP64 end-of-central-directory record missing")
    # Offset shift of the whole archive within the file (0 for a normal zip): the central
    # directory ends where the (ZIP64) end record begins.
    concat = end_of_cd_abs - cd_size - cd_off
    if concat < 0:
        raise RemoteZipError("inconsistent end-of-central-directory offsets")
    if cd_size > max_cd_bytes:
        raise RemoteZipError(f"central directory is {cd_size} B (> cap {max_cd_bytes})")
    cd_abs = cd_off + concat
    if cd_abs >= blob_start:
        cd = blob[cd_abs - blob_start:cd_abs - blob_start + cd_size]
    else:
        cd = _range_bytes(session, url, cd_abs, cd_abs + cd_size - 1)
    if len(cd) < cd_size:
        raise RemoteZipError("short central-directory read")
    if n_entries and cd[:4] != b"PK\x01\x02":
        raise RemoteZipError("central directory does not start with a file header")
    members = _parse_central_directory(cd)
    # A writer without ZIP64 end records that stores only the LOW 16 bits of a >65,535 entry count
    # is non-compliant but real (CSD3 census: a zip with 132,202 members whose EOCD says 1,130 =
    # 132,202 mod 65,536); zipfile reads such archives by walking the directory bytes, as we do.
    if len(members) != n_entries and (zip64 or len(members) % 65536 != n_entries % 65536):
        raise RemoteZipError(f"central directory lists {len(members)} of {n_entries} entries")
    if concat:
        members = [ZipMember(m.name, m.method, m.comp_size, m.uncomp_size, m.crc,
                             m.local_offset + concat) for m in members]
    return members, total


# The format rules live in the shared ``zenodo_harvest.aiida_archive`` (the fetch extractor uses
# the same ones); re-exported under the historical name.
aiida_format = archive_format


@dataclass
class ZipEvidence:
    """What a central-directory peek reveals about one archive.

    For a sqlite_zip AiiDA archive the member NAMES are opaque blobs, so ``primary``/``vasp_any``
    come from its database instead (:func:`sqlite_evidence`): ``db_status`` is then ``"ok"`` (the
    counts are authoritative), ``"db_too_large"`` or ``"db_failed: …"`` (unknown — an evidence
    gap); it is None for every other archive."""

    n_members: int = 0
    primary: list[str] = field(default_factory=list)      # members fetch would seed a calc from
    vasp_any: int = 0                                      # members fetch would extract (VASP-named)
    nested: list[str] = field(default_factory=list)       # sub-archives (contents invisible)
    nested_aiida: list[str] = field(default_factory=list)  # AiiDA archives inside (extractable)
    aiida_format: str | None = None
    primary_bytes: int = 0                                 # uncompressed bytes of the primaries
    db_status: str | None = None                           # sqlite_zip only (see above)
    db_bytes: int = 0

    def to_dict(self, max_names: int = 5) -> dict:
        d = {"n_members": self.n_members, "n_primary": len(self.primary),
             "primary_sample": self.primary[:max_names], "n_vasp_named": self.vasp_any,
             "n_nested": len(self.nested), "nested_sample": self.nested[:max_names],
             "n_nested_aiida": len(self.nested_aiida), "aiida_format": self.aiida_format,
             "primary_bytes": self.primary_bytes}
        if self.db_status is not None:
            d["db_status"], d["db_bytes"] = self.db_status, self.db_bytes
        return d


def zip_evidence(members: list[ZipMember]) -> ZipEvidence:
    """Classify an archive's members with EXACTLY the rules the shared fetch applies.

    A member counts as a **primary** iff fetch would extract it (``fetch._PARSE_RE``) AND it would
    seed a calc unit (``fetch._unit_role`` ∈ vasprun/vaspout/outcar — e.g. ``OUTCAR1``,
    ``vasprun_1.xml``, ``OUTCAR.gz`` all count, as they do at fetch). Using any other rule here
    (the triage classifier's stricter ``models._VASP_RE`` misses ``OUTCAR1``) would let triage prune
    an archive that fetch would have turned into calc units. AppleDouble/``__MACOSX`` junk is
    skipped and nested archives are recognised by fetch's own member rule."""
    ev = ZipEvidence(n_members=len(members))
    ev.aiida_format = aiida_format([m.name for m in members])
    for m in members:
        n = m.name
        if n.endswith("/") or _is_junk_member(n):
            continue
        base = n.rsplit("/", 1)[-1]
        if _PARSE_RE.search(base):
            ev.vasp_any += 1
            if _unit_role(base) in _PRIMARY_ROLES:
                ev.primary.append(n)
                ev.primary_bytes += int(m.uncomp_size or 0)
        if base.lower().endswith(".aiida"):
            ev.nested_aiida.append(n)
        elif _nested_archive_kind(base) is not None:
            ev.nested.append(n)
    return ev


def sqlite_evidence(session: requests.Session, url: str, members: list[ZipMember],
                    ev: ZipEvidence, max_db_bytes: int = DEFAULT_MAX_DB_BYTES) -> ZipEvidence:
    """Fill ``ev`` for a sqlite_zip AiiDA archive from its ``db.sqlite3``, pulled over Range.

    The database (MB to ~1 GB on MC) is streamed to a temp file under ``$TMPDIR`` and queried
    with the SHARED rule set (:func:`zenodo_harvest.aiida_archive.vasp_nodes` + fetch's
    ``_unit_role``), so triage sees exactly the calcs the fetch extractor will write: ``primary``
    lists them at their legacy-layout paths, ``primary_bytes`` sums their ``repo/<key>`` blobs."""
    names = [m.name for m in members]
    db_name = aiida_archive.db_member(names)
    db = next((m for m in members if m.name == db_name), None) if db_name else None
    if db is None:
        ev.db_status = "db_failed: no db.sqlite3 member"
        return ev
    ev.db_bytes = int(db.uncomp_size or 0)
    if ev.db_bytes > max_db_bytes:
        ev.db_status = "db_too_large"
        return ev
    fd, tmp_name = tempfile.mkstemp(prefix="mc_aiida_", suffix=".sqlite3")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        if not fetch_member(session, url, db, tmp):
            ev.db_status = "db_failed: database transfer failed"
            return ev
        try:
            nodes = aiida_archive.vasp_nodes(tmp, _is_primary_name)
        except ValueError as exc:
            ev.db_status = f"db_failed: {exc}"[:160]
            return ev
    finally:
        tmp.unlink(missing_ok=True)
    prefix = aiida_archive.repo_prefix(db_name or "")
    sizes = {m.name: int(m.uncomp_size or 0) for m in members}
    for node in nodes:
        ndir = aiida_archive.legacy_node_dir(node["uuid"])
        for rel, key in node["files"]:
            base = rel.rsplit("/", 1)[-1]
            if _is_junk_member(base) or not _PARSE_RE.search(base):
                continue
            ev.vasp_any += 1
            if _is_primary_name(base):
                ev.primary.append(f"{ndir}/{rel}")
                ev.primary_bytes += sizes.get(f"{prefix}repo/{key}", 0)
    ev.db_status = "ok"
    return ev


def peek_archive(session: requests.Session, url: str, max_cd_bytes: int = DEFAULT_MAX_CD_BYTES,
                 max_db_bytes: int = DEFAULT_MAX_DB_BYTES) -> tuple[ZipEvidence | None, str]:
    """Peek one remote zip/AiiDA export: ``(evidence, status)`` with status ``"ok"`` or a short
    failure reason (``"not_zip"``, ``"cd_too_large"``, ``"peek_failed: …"``). A sqlite_zip AiiDA
    archive is resolved through its database (:func:`sqlite_evidence`): a database over
    ``max_db_bytes`` (0 = never read one) makes the status ``"db_too_large"``, a failed transfer
    or query ``"peek_failed: db …"`` (retried in-run, never cached)."""
    try:
        members, _total = read_central_directory(session, url, max_cd_bytes=max_cd_bytes)
    except RemoteZipError as exc:
        msg = str(exc)
        if "not a zip" in msg:
            return None, "not_zip"
        if "> cap" in msg:
            return None, "cd_too_large"
        return None, f"peek_failed: {msg[:160]}"
    except (struct.error, ValueError) as exc:
        return None, f"peek_failed: {type(exc).__name__}: {exc}"
    ev = zip_evidence(members)
    if ev.aiida_format == "sqlite_zip" and max_db_bytes:
        ev = sqlite_evidence(session, url, members, ev, max_db_bytes)
        if ev.db_status == "db_too_large":
            return ev, "db_too_large"
        if ev.db_status != "ok":
            return ev, f"peek_failed: {ev.db_status}"
    return ev, "ok"


def fetch_member(session: requests.Session, url: str, m: ZipMember, dest: Path) -> bool:
    """Stream ONE member (STORED or DEFLATE) out of a remote zip into ``dest``, CRC-verified.

    Two Range reads: the 30-byte local header (its own name/extra lengths locate the data — the
    local extra may differ from the central one), then exactly the compressed bytes, inflated on
    the fly. Used by the CSD3 probe to pull an AiiDA ``db.sqlite3``; returns False (and removes
    ``dest``) on an unsupported method or a CRC/length mismatch."""
    if m.method not in (0, 8):
        return False
    try:
        head = _range_bytes(session, url, m.local_offset, m.local_offset + 29)
    except (RemoteZipError, requests.RequestException) as exc:
        logger.warning("local header read failed for %s: %s", m.name, exc)
        return False
    if head[:4] != _LFH_SIG:
        return False
    n_len, e_len = struct.unpack("<HH", head[26:30])
    start = m.local_offset + 30 + n_len + e_len
    dec = zlib.decompressobj(-15) if m.method == 8 else None
    crc = written = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with _ranged_get(session, url, f"bytes={start}-{start + max(m.comp_size, 1) - 1}",
                         stream=True) as r, dest.open("wb") as fh:
            remaining = m.comp_size
            for chunk in r.iter_content(1 << 20):
                chunk = chunk[:remaining]
                remaining -= len(chunk)
                out = dec.decompress(chunk) if dec is not None else chunk
                crc = zlib.crc32(out, crc)
                written += len(out)
                fh.write(out)
                if remaining <= 0:
                    break
            if dec is not None:
                tail = dec.flush()
                crc = zlib.crc32(tail, crc)
                written += len(tail)
                fh.write(tail)
    except (RemoteZipError, requests.RequestException, zlib.error, OSError) as exc:
        logger.warning("member fetch failed for %s: %s", m.name, exc)
        dest.unlink(missing_ok=True)
        return False
    if written != m.uncomp_size or (crc & 0xFFFFFFFF) != (m.crc & 0xFFFFFFFF):
        dest.unlink(missing_ok=True)
        return False
    return True
