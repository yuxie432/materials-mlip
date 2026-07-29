"""Remote ZIP random access — enumerate and pull *individual* members over HTTP Range.

A ZIP file carries a *central directory* at its tail listing, for every member, the
name, compression method, compressed/uncompressed sizes, CRC-32, and — crucially — the
byte offset of that member's local header. So, given a server that honours HTTP Range
(Zenodo does; it answers 206 even without an ``Accept-Ranges`` header), we can:

1. read only the central directory (one Range read of the file tail) to enumerate every
   member without downloading the archive — this is what triage's ``peek_zip_filenames``
   already does for names; here we return the full structured record; and
2. fetch and decompress just *one* member (its local header + compressed data) with one
   more Range read, decompressing on the fly.

That lets :mod:`fetch` pull only the VASP-relevant files (``vasprun.xml``/``OUTCAR``/…)
out of a large archive instead of downloading the whole thing to extract a few files and
delete the rest (mentor/user 2026-07-29; validated by the third survey investigation,
which found targeted ZIP fetch the one worthwhile random-access play — tar has no index,
and compressed tars are non-seekable). Transfer, transient disk, and time all drop.

Scope: standard 32-bit ZIPs with STORED/DEFLATE members. ZIP64, unusual compression
(bzip2/lzma/zstd-in-zip), or encrypted entries are reported so the caller can fall back
to a whole-archive download — never a hard failure. Only a *target* member needs to be
32-bit/supported: a huge ZIP64 heavy file (CHGCAR/WAVECAR) we skip anyway does not block
targeting the small 32-bit VASP outputs beside it.
"""

from __future__ import annotations

import logging
import struct
import time
import zlib
from dataclasses import dataclass

import requests

from .client import _parse_retry_after

logger = logging.getLogger(__name__)

EOCD_SIG = b"\x50\x4b\x05\x06"   # end of central directory record
CDH_SIG = 0x02014b50             # central directory file header
LFH_SIG = 0x04034b50             # local file header
_ZIP64_SENTINEL = 0xFFFFFFFF     # a 32-bit field == this => the real value is in ZIP64 extra

# Compression methods we can decode when targeting a single member.
METHOD_STORED = 0
METHOD_DEFLATE = 8
_SUPPORTED_METHODS = frozenset({METHOD_STORED, METHOD_DEFLATE})

# General-purpose bit-flag bits.
_FLAG_ENCRYPTED = 0x1

# Range GETs for a zip peek/fetch can be rate-limited (429) like any request; retry a
# bounded number of times honouring Retry-After (mirrors triage's peek).
_MAX_ATTEMPTS = 3
# Local extra field is nearly always tiny (< tens of bytes); this margin lets us grab a
# member's local header + data in ONE Range read. A rare larger local extra triggers a
# precise second read (see :func:`open_member_reader`).
DEFAULT_EXTRA_MARGIN = 4096


def _close(resp: object) -> None:
    """Close a response if it can be, tolerating fakes/None (never raises)."""
    try:
        resp.close()  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive
        pass


@dataclass
class ZipEntry:
    """One central-directory record — everything needed to fetch the member remotely."""

    name: str
    method: int
    compressed_size: int
    uncompressed_size: int
    crc: int
    local_header_offset: int
    flag: int

    @property
    def is_dir(self) -> bool:
        return self.name.endswith("/")

    @property
    def encrypted(self) -> bool:
        return bool(self.flag & _FLAG_ENCRYPTED)

    @property
    def needs_zip64(self) -> bool:
        """Whether a size/offset overflowed 32 bits (real value hides in the ZIP64 extra).

        We do not parse the ZIP64 extra, so such a member cannot be targeted — but a
        *non-target* member being ZIP64 (a >4 GB CHGCAR we skip) is harmless.
        """
        return _ZIP64_SENTINEL in (self.compressed_size, self.uncompressed_size,
                                   self.local_header_offset)

    @property
    def targetable(self) -> bool:
        """Whether this member can be fetched+decoded individually right now."""
        return (self.method in _SUPPORTED_METHODS and not self.encrypted
                and not self.needs_zip64 and not self.is_dir)


def is_supported_method(method: int) -> bool:
    return method in _SUPPORTED_METHODS


# --------------------------------------------------------------------------- #
# Range primitives                                                            #
# --------------------------------------------------------------------------- #

def _ranged_get(session: requests.Session, url: str, range_header: str,
                max_attempts: int = _MAX_ATTEMPTS) -> requests.Response | None:
    """A single streaming Range GET, honouring 429 + Retry-After (bounded retries).

    Returns the response (caller checks for 206), or None once the 429 budget is spent.
    ``stream=True`` so a large member's body is not pulled into memory before the caller
    decides what to do with it.
    """
    for _ in range(max_attempts):
        r = session.get(url, headers={"Range": range_header}, timeout=120, stream=True)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After")) + 1
        logger.warning("zip range read rate limited; sleeping %ss", wait)
        time.sleep(wait)
    return None


def _range_bytes(session: requests.Session, url: str, start: int, end: int) -> bytes | None:
    """Fetch the exact byte slice ``[start, end]`` (inclusive), or None if not a 206."""
    r = _ranged_get(session, url, f"bytes={start}-{end}")
    if r is None:
        return None
    try:
        if r.status_code != 206:
            return None
        return r.content
    finally:
        _close(r)


# --------------------------------------------------------------------------- #
# Central-directory enumeration                                               #
# --------------------------------------------------------------------------- #

def _parse_central_directory(cd: bytes, total: int) -> list[ZipEntry]:
    entries: list[ZipEntry] = []
    p = 0
    for _ in range(total):
        if p + 46 > len(cd) or struct.unpack("<I", cd[p:p + 4])[0] != CDH_SIG:
            break
        flag, method = struct.unpack("<HH", cd[p + 8:p + 12])
        crc, csize, usize = struct.unpack("<III", cd[p + 16:p + 28])
        n_len, m_len, k_len = struct.unpack("<HHH", cd[p + 28:p + 34])
        (lho,) = struct.unpack("<I", cd[p + 42:p + 46])
        name = cd[p + 46:p + 46 + n_len].decode("utf-8", "replace")
        entries.append(ZipEntry(name, method, csize, usize, crc, lho, flag))
        p += 46 + n_len + m_len + k_len
    return entries


def remote_central_directory(url: str, session: requests.Session | None = None,
                             tail: int = 65_536) -> list[ZipEntry] | None:
    """Enumerate a remote ZIP's members by reading its central directory over Range.

    One suffix-range read grabs the file tail (+ the total size from ``Content-Range``);
    a second explicit read is issued only when the central directory sits ahead of that
    tail. Returns the member list, or None when the archive is not a plain 32-bit ZIP we
    can address (Range not honoured, ZIP64 end-of-central-directory, truncated/garbage,
    or a small-file suffix-range the server mishandles and we cannot recover) — in every
    such case the caller falls back to a whole-archive download.
    """
    session = session or requests.Session()
    try:
        r = _ranged_get(session, url, f"bytes=-{tail}")
        if r is None:
            return None
        if r.status_code != 206:
            _close(r)               # Range ignored (200) or unavailable
            return None
        cr = r.headers.get("Content-Range", "")
        size = int(cr.split("/")[-1]) if "/" in cr else None
        try:
            blob = r.content
        except requests.RequestException:
            # Zenodo mishandles a suffix range on a file SMALLER than the tail (the 206
            # body is undeliverable). Only such a small file hits this, so re-fetch it
            # whole; a broken read on a genuinely large file is a transient error we do
            # not chase (that is exactly the multi-GB pull the peek exists to avoid).
            _close(r)
            if size is None or size > tail:
                return None
            whole = session.get(url, timeout=120)
            if whole.status_code != 200:
                return None
            blob = whole.content
            size = len(blob)
        else:
            _close(r)
        if size is None:
            size = len(blob)
        blob_start = size - len(blob)   # absolute offset of blob[0] within the file

        idx = blob.rfind(EOCD_SIG)
        if idx == -1:
            return None
        eocd = blob[idx:idx + 22]
        _, _, _, _, total, cd_size, cd_off, _ = struct.unpack("<IHHHHIIH", eocd)
        if cd_off == _ZIP64_SENTINEL or cd_size == _ZIP64_SENTINEL or total == 0xFFFF:
            return None  # ZIP64 end of central directory — fall back to whole download

        cd: bytes | None
        if cd_off >= blob_start:        # central directory already inside the fetched tail
            cd = blob[cd_off - blob_start:cd_off - blob_start + cd_size]
        else:
            cd = _range_bytes(session, url, cd_off, cd_off + cd_size - 1)
        if not cd or len(cd) < cd_size:
            return None
        return _parse_central_directory(cd, total)
    except (requests.RequestException, struct.error, ValueError) as exc:
        logger.debug("remote central directory read failed for %s: %s", url, exc)
        return None


# --------------------------------------------------------------------------- #
# Single-member fetch + streaming decode                                      #
# --------------------------------------------------------------------------- #

class _StreamBuffer:
    """Buffered reader over a streaming response body (bounded memory)."""

    def __init__(self, resp: requests.Response, chunk: int = 1 << 20):
        self._resp = resp
        self._it = resp.iter_content(chunk)
        self._buf = bytearray()
        self._eof = False

    def _fill(self, need: int) -> None:
        while len(self._buf) < need and not self._eof:
            try:
                c = next(self._it)
            except StopIteration:
                self._eof = True
                break
            if c:
                self._buf.extend(c)

    def read_exact(self, k: int) -> bytes | None:
        self._fill(k)
        if len(self._buf) < k:
            return None
        out = bytes(self._buf[:k])
        del self._buf[:k]
        return out

    def discard(self, k: int) -> bool:
        self._fill(k)
        if len(self._buf) < k:
            return False
        del self._buf[:k]
        return True

    def read_up_to(self, n: int) -> bytes:
        if not self._buf:
            self._fill(min(n, 1 << 20))
        take = min(n, len(self._buf))
        out = bytes(self._buf[:take])
        del self._buf[:take]
        return out

    def close(self) -> None:
        _close(self._resp)


class MemberReader:
    """A ``.read(size)``-able view of ONE member's *decompressed* bytes.

    Pulls exactly ``entry.compressed_size`` compressed bytes from ``buf``, inflating
    (raw DEFLATE) or passing through (STORED) on the fly, and accumulating the CRC-32 so
    the caller can verify integrity against the central-directory value. Memory stays
    ~chunk-sized regardless of the member's uncompressed size, so this drops straight into
    :func:`fetch._copy_capped` in place of an archive member handle.
    """

    def __init__(self, buf: _StreamBuffer, entry: ZipEntry):
        self._buf = buf
        self._entry = entry
        self._decomp = None if entry.method == METHOD_STORED else zlib.decompressobj(-15)
        self._pending = bytearray()
        self._remaining_comp = entry.compressed_size
        self._crc = 0
        self.raw_out = 0
        self._done = False

    def _emit(self, data: bytes) -> None:
        if data:
            self._crc = zlib.crc32(data, self._crc)
            self.raw_out += len(data)
            self._pending.extend(data)

    def read(self, size: int) -> bytes:
        while len(self._pending) < size and not self._done:
            comp = self._buf.read_up_to(min(self._remaining_comp, 1 << 20)) \
                if self._remaining_comp > 0 else b""
            if comp:
                self._remaining_comp -= len(comp)
                self._emit(comp if self._decomp is None else self._decomp.decompress(comp))
            else:
                if self._decomp is not None:
                    self._emit(self._decomp.flush())
                self._done = True
        out = bytes(self._pending[:size])
        del self._pending[:size]
        return out

    @property
    def crc_ok(self) -> bool:
        return (self._crc & 0xFFFFFFFF) == (self._entry.crc & 0xFFFFFFFF)

    @property
    def complete(self) -> bool:
        """Whether the full member was received (right length AND CRC)."""
        return self._done and self.raw_out == self._entry.uncompressed_size and self.crc_ok

    def close(self) -> None:
        self._buf.close()


def open_member_reader(session: requests.Session, url: str, entry: ZipEntry,
                       margin: int = DEFAULT_EXTRA_MARGIN) -> MemberReader | None:
    """Open a :class:`MemberReader` for ``entry``, or None if Range is not honoured.

    Reads the member's local header (whose *own* filename/extra lengths give the exact
    data start — the local extra field may differ from the central one) and positions a
    reader at the compressed data. One Range read in the common case; a precise second
    read only if the local extra field exceeds ``margin``.
    """
    lho = entry.local_header_offset
    name_len = len(entry.name.encode("utf-8"))     # local filename length == central's
    want_header = 30 + name_len + margin
    hi = lho + want_header + entry.compressed_size - 1
    r = _ranged_get(session, url, f"bytes={lho}-{hi}")
    if r is None or r.status_code != 206:
        if r is not None:
            _close(r)
        return None
    buf = _StreamBuffer(r)
    head = buf.read_exact(30)
    if head is None or struct.unpack("<I", head[:4])[0] != LFH_SIG:
        buf.close()
        return None
    n_prime, m_prime = struct.unpack("<HH", head[26:30])
    if 30 + n_prime + m_prime > want_header:
        # Local extra field bigger than our margin: the data is not (fully) in this
        # response. Refetch just the data at its true start.
        buf.close()
        data_start = lho + 30 + n_prime + m_prime
        r = _ranged_get(session, url, f"bytes={data_start}-{data_start + entry.compressed_size - 1}")
        if r is None or r.status_code != 206:
            if r is not None:
                _close(r)
            return None
        buf = _StreamBuffer(r)
    elif not buf.discard(n_prime + m_prime):
        buf.close()
        return None
    return MemberReader(buf, entry)
