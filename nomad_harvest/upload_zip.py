"""Targeted extraction of raw files from a NOMAD upload's PRE-PACKED zip over HTTP Range.

This is the fast NOMAD fetch path, and it is a direct consequence of how NOMAD stores data
(live-verified + read from NOMAD's source, 2026-08-17):

* Every *published upload* is stored server-side as ONE zip, ``raw-public.plain.zip``. All
  ~7.1M direct-upload VASP entries live in only ~3,800 uploads.
* ``GET /uploads/{id}/raw`` streams that already-packed zip **straight off disk** (NOMAD's
  own docstring: "more efficient because it simply streams an already existing .zip file").
  It is anonymous, honours HTTP **Range** *and* **multi-range**, and runs at ~15-30 MB/s.
* By contrast ``POST /entries/raw/query`` (the old bulk path) re-opens and re-parses that
  upload zip's whole central directory **~4x per file** with no caching, so it is ~0.8 s/file
  (~0.3 MB/s) however you batch it — server-CPU-bound, not bandwidth-bound.

So we address data the way it is physically stored — **by upload, by byte offset**:

1. one Range read of the zip's tail -> parse its (ZIP64) central directory -> every member's
   byte offset/size/CRC (:func:`read_central_directory`);
2. multi-range-fetch only the wanted members (each entry's ``mainfile``, +optional OUTCAR)
   straight out of the zip, CRC-verified (:func:`fetch_members`).

Members are STORED (compression method 0) in practice — a ``vasprun.xml.bz2`` sits verbatim
in the outer zip — so a targeted fetch returns exact bytes with no server-side work. DEFLATE
(method 8) is handled defensively too.

The one governor is the endpoint's rate limit: **per IP, one in-flight connection, a new one
only every ~5 s** (:data:`~nomad_harvest.client.NomadClient.UPLOAD_MIN_INTERVAL`). Requests
here are therefore serial; concurrency cannot help (a 2nd connection just 429s). Batching many
members into one multi-range request is what keeps the *request count* — and thus the run —
small (~30k requests, ~2 days for the whole corpus).
"""

from __future__ import annotations

import logging
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import NomadClient

logger = logging.getLogger(__name__)

# ZIP structural signatures.
_EOCD_SIG = b"PK\x05\x06"          # end of central directory record
_EOCD64_LOC_SIG = b"PK\x06\x07"    # zip64 end-of-central-directory locator
_EOCD64_SIG = b"PK\x06\x06"        # zip64 end of central directory record
_CDH_SIG = b"PK\x01\x02"           # central directory file header
_LFH_SIG = b"PK\x03\x04"           # local file header
_ZIP64_SENTINEL32 = 0xFFFFFFFF
_ZIP64_SENTINEL16 = 0xFFFF

# How much of the zip tail to read for the central directory. A ~200k-member upload has a
# ~15-20 MB central directory, so 48 MB covers the common case in a single suffix-range read;
# a rare larger one triggers one extra explicit read (see read_central_directory).
_TAIL_BYTES = 48 << 20
# Multi-range request budget. NOMAD's proxy caps the Range HEADER at ~8 KB (~300 compact
# ranges); stay well under. And cap the in-memory batch bytes so a multi-range response is
# read into RAM safely — a member bigger than this is streamed to disk on its own instead.
MAX_RANGES_PER_REQUEST = 200
MAX_BATCH_BYTES = 96 << 20
# Room for a local-file-header extra field (observed 0; padded so a rare non-zero elen still
# lands the member's data inside the fetched range).
_LOCAL_EXTRA_PAD = 512


class UploadNotAvailable(Exception):
    """The upload's pre-packed zip could not be read over Range (e.g. not published, or the
    endpoint returned a non-retryable error / ignored Range). The caller falls back to the
    per-entry ``entries/{id}/raw`` path for that upload's entries — never a coverage loss."""


@dataclass(slots=True)
class ZipMember:
    """One central-directory record — everything needed to fetch + verify the member."""

    name: str
    method: int          # 0 = STORED, 8 = DEFLATE
    comp_size: int
    uncomp_size: int
    crc: int
    local_offset: int

    @property
    def on_disk_size(self) -> int:
        """Bytes this member occupies once staged (the actual file content): the stored
        bytes for STORED, the inflated size for DEFLATE."""
        return self.uncomp_size if self.method == 8 else self.comp_size


# --- central directory ---------------------------------------------------------

def _parse_central_directory(cd: bytes) -> list[ZipMember]:
    """Parse concatenated central-directory headers (ZIP64-aware) into members."""
    out: list[ZipMember] = []
    i, n = 0, len(cd)
    while i + 46 <= n and cd[i:i + 4] == _CDH_SIG:
        # Central-directory header (46 bytes). Fields (little-endian): 0 sig, 1 version-made-by,
        # 2 version-needed, 3 flags, 4 method, 5 modtime, 6 moddate, 7 crc32, 8 comp-size,
        # 9 uncomp-size, 10 name-len, 11 extra-len, 12 comment-len, 13 disk, 14 int-attr,
        # 15 ext-attr, 16 local-header-offset.
        f = struct.unpack("<IHHHHHHIIIHHHHHII", cd[i:i + 46])
        method, crc, comp, uncomp = f[4], f[7], f[8], f[9]
        nlen, elen, clen, off = f[10], f[11], f[12], f[16]
        name = cd[i + 46:i + 46 + nlen].decode("utf-8", "replace")
        extra = cd[i + 46 + nlen:i + 46 + nlen + elen]
        # ZIP64 extended-information extra field (0x0001): the real 64-bit values for any
        # field stored as the 32-bit sentinel, in a fixed order (uncomp, comp, offset).
        if _ZIP64_SENTINEL32 in (off, comp, uncomp):
            j = 0
            while j + 4 <= len(extra):
                hid, hsz = struct.unpack("<HH", extra[j:j + 4])
                body = extra[j + 4:j + 4 + hsz]
                if hid == 0x0001:
                    # The zip64 extra holds a 64-bit value for each sentinel field, in the
                    # fixed order uncomp, comp, offset — only those actually overflowed.
                    vals = iter(struct.unpack_from(f"<{len(body) // 8}Q", body))
                    if uncomp == _ZIP64_SENTINEL32:
                        uncomp = next(vals)
                    if comp == _ZIP64_SENTINEL32:
                        comp = next(vals)
                    if off == _ZIP64_SENTINEL32:
                        off = next(vals)
                    break
                j += 4 + hsz
        out.append(ZipMember(name, method, comp, uncomp, crc, off))
        i += 46 + nlen + elen + clen
    return out


def _read_range(client: "NomadClient", upload_id: str, start: int, end: int) -> bytes:
    """Read bytes [start, end] of an upload's pre-packed zip (fully into memory)."""
    with client.upload_raw_get(upload_id, f"bytes={start}-{end}") as r:
        return r.content


def read_central_directory(client: "NomadClient", upload_id: str
                           ) -> tuple[dict[str, ZipMember], int]:
    """Enumerate an upload's zip members over Range: ``({name: ZipMember}, total_size)``.

    One suffix-range read grabs the tail (and the total size from ``Content-Range``); the
    central directory usually sits inside it, so this is normally ONE request per upload. A
    rare larger central directory triggers one extra explicit read. Handles both 32-bit and
    ZIP64 archives (NOMAD's big uploads are >4 GB → ZIP64). Raises :class:`UploadNotAvailable`
    if the tail/EOCD cannot be read or parsed.
    """
    try:
        with client.upload_raw_get(upload_id, f"bytes=-{_TAIL_BYTES}") as r:
            cr = r.headers.get("Content-Range", "")
            blob = r.content
    except Exception as exc:  # noqa: BLE001 - HTTPError (not published), network, etc.
        raise UploadNotAvailable(f"{upload_id}: tail read failed: {exc}") from exc
    if "/" not in cr:
        raise UploadNotAvailable(f"{upload_id}: no Content-Range (Range not honoured)")
    total = int(cr.split("/")[-1])
    blob_start = total - len(blob)

    idx = blob.rfind(_EOCD_SIG)
    if idx < 0:
        raise UploadNotAvailable(f"{upload_id}: no EOCD in tail")
    # EOCD record: [12:16] central-directory size, [16:20] central-directory offset.
    cd_size, cd_off = struct.unpack("<II", blob[idx + 12:idx + 20])
    if cd_off == _ZIP64_SENTINEL32 or cd_size == _ZIP64_SENTINEL32:
        loc = blob[idx - 20:idx]
        if loc[:4] != _EOCD64_LOC_SIG:
            raise UploadNotAvailable(f"{upload_id}: zip64 locator missing")
        z64_off = struct.unpack("<Q", loc[8:16])[0]
        try:
            z = _read_range(client, upload_id, z64_off, z64_off + 55)
        except Exception as exc:  # noqa: BLE001
            raise UploadNotAvailable(f"{upload_id}: zip64 EOCD read failed: {exc}") from exc
        if z[:4] != _EOCD64_SIG:
            raise UploadNotAvailable(f"{upload_id}: zip64 EOCD missing")
        cd_size, cd_off = struct.unpack("<QQ", z[40:56])

    if cd_off >= blob_start:
        cd = blob[cd_off - blob_start: cd_off - blob_start + cd_size]
    else:                                   # central directory sits ahead of the fetched tail
        try:
            cd = _read_range(client, upload_id, cd_off, cd_off + cd_size - 1)
        except Exception as exc:  # noqa: BLE001
            raise UploadNotAvailable(f"{upload_id}: central-directory read failed: {exc}") from exc
    members = _parse_central_directory(cd)
    return {m.name: m for m in members}, total


# --- member extraction ---------------------------------------------------------

def _extract_from_segment(seg: bytes, seg_start: int, m: ZipMember) -> bytes | None:
    """Given a byte segment starting at file offset ``seg_start`` that contains member ``m``'s
    local header + data, return the member's file content (inflated for DEFLATE), or None if
    the segment does not cover it. CRC is verified by the caller."""
    rel = m.local_offset - seg_start
    if rel < 0 or rel + 30 > len(seg) or seg[rel:rel + 4] != _LFH_SIG:
        return None
    nlen, elen = struct.unpack("<HH", seg[rel + 26:rel + 30])
    data_start = rel + 30 + nlen + elen
    data = seg[data_start:data_start + m.comp_size]
    if len(data) < m.comp_size:
        return None                          # range was short (unexpectedly large local extra)
    if m.method == 0:
        return data
    if m.method == 8:
        try:
            return zlib.decompress(data, -15)
        except zlib.error:
            return None
    return None                              # unsupported compression


def _segments_from_response(body: bytes, content_type: str, content_range: str
                            ) -> list[tuple[int, bytes]]:
    """Turn a Range response into ``[(file_start_offset, segment_bytes), …]``.

    A single-range 206 is one segment (its start from ``Content-Range``); a multi-range 206 is
    ``multipart/byteranges`` — each part carries its own ``Content-Range``. Mapping by file
    offset (not by scanning for zip signatures) is robust to the server COALESCING adjacent
    requested ranges into one part, and to a member's compressed bytes happening to contain a
    zip signature."""
    if "multipart/byteranges" in content_type:
        boundary = content_type.split("boundary=", 1)[1].strip().encode()
        segs: list[tuple[int, bytes]] = []
        for part in body.split(b"--" + boundary):
            he = part.find(b"\r\n\r\n")
            if he < 0:
                continue
            headers = part[:he]
            mcr = _find_content_range(headers)
            if mcr is None:
                continue
            payload = part[he + 4:]
            if payload.endswith(b"\r\n"):
                payload = payload[:-2]
            segs.append((mcr, payload))
        return segs
    if "-" in content_range and "/" in content_range:      # single range: "bytes A-B/total"
        start = int(content_range.split()[-1].split("-")[0])
        return [(start, body)]
    return []


def _find_content_range(headers: bytes) -> int | None:
    for line in headers.split(b"\r\n"):
        if line.lower().startswith(b"content-range:"):
            try:
                return int(line.split(b"bytes", 1)[1].strip().split(b"-")[0])
            except (IndexError, ValueError):
                return None
    return None


def _verify_and_write(content: bytes, m: ZipMember, dest: Path) -> bool:
    """CRC-check the extracted file content and write it to ``dest``. Returns success."""
    if (zlib.crc32(content) & 0xFFFFFFFF) != (m.crc & 0xFFFFFFFF):
        logger.warning("CRC mismatch for %s (member %s)", dest, m.name)
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        tmp.write_bytes(content)
        tmp.replace(dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return True


def _fetch_one_big(client: "NomadClient", upload_id: str, m: ZipMember, dest: Path) -> bool:
    """Fetch a single large member on its own, streaming to disk (kept out of RAM). Its local
    header + data are contiguous from ``local_offset``; parse the header from the first chunk."""
    end = m.local_offset + 30 + len(m.name) + _LOCAL_EXTRA_PAD + m.comp_size - 1
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with client.upload_raw_get(upload_id, f"bytes={m.local_offset}-{end}", stream=True) as r:
            it = r.iter_content(1 << 20)
            head = b""
            # Accumulate the local header: first ≥30 bytes to read name/extra lengths, then
            # enough to cover the full header (a chunked server may split it small).
            for chunk in it:
                head += chunk
                if len(head) >= 30:
                    break
            if len(head) < 30:
                tmp.unlink(missing_ok=True)
                return False
            nlen, elen = struct.unpack("<HH", head[26:30])
            skip = 30 + nlen + elen
            while len(head) < skip:
                chunk = next(it, b"")
                if not chunk:
                    tmp.unlink(missing_ok=True)
                    return False
                head += chunk
            # For STORED we can stream+CRC without holding the whole file; DEFLATE is rare and
            # small enough to inflate in memory, so buffer it.
            if m.method == 0:
                crc = 0
                written = 0
                buf = head[skip:]
                with tmp.open("wb") as fh:
                    while True:
                        take = min(len(buf), m.comp_size - written)
                        if take:
                            fh.write(buf[:take])
                            crc = zlib.crc32(buf[:take], crc)
                            written += take
                        if written >= m.comp_size:
                            break
                        buf = next(it, b"")
                        if not buf:
                            break
                if written != m.comp_size or (crc & 0xFFFFFFFF) != (m.crc & 0xFFFFFFFF):
                    tmp.unlink(missing_ok=True)
                    return False
                tmp.replace(dest)
                return True
            body = head[skip:] + b"".join(it)
            content = zlib.decompress(body[:m.comp_size], -15) if m.method == 8 else None
            if content is None:
                tmp.unlink(missing_ok=True)
                return False
            tmp.unlink(missing_ok=True)
            return _verify_and_write(content, m, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def fetch_members(client: "NomadClient", upload_id: str,
                  items: list[tuple[ZipMember, Path]]) -> dict[Path, bool]:
    """Fetch the given ``(member, dest)`` items out of the upload's pre-packed zip.

    Batches members into multi-range requests (bounded by :data:`MAX_RANGES_PER_REQUEST` and
    :data:`MAX_BATCH_BYTES`), streaming an over-large member on its own to disk. Each written
    file is CRC-verified against the central directory. Returns ``{dest: ok}`` for every item;
    a False lets the caller fall back to the per-entry path for that member's entry — never a
    coverage loss. Raises only on an exhausted-retry transport failure (whole batch), which the
    caller also turns into a per-entry fallback.
    """
    results: dict[Path, bool] = {}
    batch: list[tuple[ZipMember, Path]] = []
    batch_bytes = 0

    def flush() -> None:
        nonlocal batch, batch_bytes
        if not batch:
            return
        by_offset = sorted(batch, key=lambda mp: mp[0].local_offset)
        ranges = []
        for m, _dest in by_offset:
            end = m.local_offset + 30 + len(m.name) + _LOCAL_EXTRA_PAD + m.comp_size - 1
            ranges.append(f"{m.local_offset}-{end}")
        with client.upload_raw_get(upload_id, "bytes=" + ",".join(ranges)) as r:
            segs = _segments_from_response(r.content, r.headers.get("Content-Type", ""),
                                           r.headers.get("Content-Range", ""))
        for m, dest in by_offset:
            content = None
            for seg_start, seg in segs:
                if seg_start <= m.local_offset < seg_start + len(seg):
                    content = _extract_from_segment(seg, seg_start, m)
                    if content is not None:
                        break
            results[dest] = bool(content is not None and _verify_and_write(content, m, dest))
        batch = []
        batch_bytes = 0

    for m, dest in items:
        if m.on_disk_size >= MAX_BATCH_BYTES:
            flush()
            try:
                results[dest] = _fetch_one_big(client, upload_id, m, dest)
            except Exception as exc:  # noqa: BLE001 - transport failure -> per-entry fallback
                logger.warning("big-member fetch failed for %s: %s", dest, exc)
                results[dest] = False
            continue
        if len(batch) >= MAX_RANGES_PER_REQUEST or batch_bytes + m.on_disk_size > MAX_BATCH_BYTES:
            flush()
        batch.append((m, dest))
        batch_bytes += m.on_disk_size
    flush()
    return results
