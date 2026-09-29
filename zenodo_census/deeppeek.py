"""Deeper peeks: list the archives that one head read or a zip's central directory could not settle.

Used for records the standard peeks leave unresolved: T2 (and the samples) have no fail-safe, so
VASP inside such an archive is otherwise lost; for a T1 fail-safe record a proof of "no VASP" saves
the whole download (whatever stays unresolved is still downloaded whole, no size cap — user decision
2026-09-29). In the census T1 triage (CSD3, 2026-09-28) 49% of the unresolved records held at least
one archive of the kinds below and 46% held nothing else — about half the unresolved bytes CAN be
listed without a download (live-checked 2026-09-29: a 2 GB tar listed in 10 reads, a 49 GB 7z in 3):

* **uncompressed tar** (:func:`tar_walk`, 0.52 TB): a member's size says where the next header
  starts, so the walk hops from header to header over HTTP Range — a tar of a few huge files lists
  in a few small reads, runs of small members arrive in one (adaptively larger) read. The hops follow
  tarfile's own rules (the shared fetch extracts with tarfile): a PAX ``size`` record sets the next
  member's size, links / directories carry no data, GNU sparse members are files;
* **7z** (:func:`sevenzip_peek`, 0.31 TB): the 32-byte start header points at the end header, which
  py7zr decodes (a packed header too) — in a CHILD process killed on a timeout / memory cap (a corrupt
  packed header can make py7zr spin forever), fed by the parent's paced, budgeted Range reads;
* **archives nested inside a zip** (:func:`nested_zip_peek`, 0.85 TB): the outer zip's central
  directory (read through the budget) locates every nested member; its local header and first bytes
  come in ONE read, are inflated when deflated, and are head-peeked (a tar stream), local-header-
  walked (a deflated inner zip) or listed through their own central directory / end header (a
  STORED inner zip / 7z / uncompressed tar, in place).

Compressed tar streams (gzip / bzip2 / xz / zstd) cannot be listed without reading them from the
start: they stay with the head-peek (and the T1 fail-safe). Every verdict is evidence in the
head-peek's shape (``names_evidence`` counts + ``complete``); ``complete`` marks an exact listing, and
only then is "no VASP" a proof. A walk stops early at its first VASP primary (that is enough to keep
the record). Transient failures report ``peek_failed: …`` (never cached); a spent budget leaves an
honest partial listing (``complete`` False); corrupt data gets a deterministic, cacheable status.
An independent review (2026-09-29) found — and tests pin — false proofs from nameless 7z entries, a
PAX-size member, GNU sparse / sized directory headers; a py7zr spin; budget leaks; inverted ranges.
"""

from __future__ import annotations

import errno
import io
import logging
import multiprocessing as mp
import re
import struct
import time
import zipfile
import zlib
from typing import Any, BinaryIO, cast

import requests

from materials_cloud_harvest.remote_zip import (
    RemoteZipError,
    ZipMember,
    _ranged_get,
    zip_evidence,
)
from zenodo_harvest.fetch import _PARSE_RE, _PRIMARY_ROLES, _is_junk_member, _nested_archive_kind, _unit_role

from .headpeek import (
    MAX_DECOMPRESSED,
    META_TYPES,
    REGULAR_TYPES,
    _header_ok,
    _tar_size,
    data_span,
    head_evidence,
    member_name,
    names_evidence,
    pax_records,
    pax_size,
)

try:
    import py7zr  # type: ignore
except ImportError:  # pragma: no cover - optional dep (the ``archives`` extra)
    py7zr = None  # type: ignore

logger = logging.getLogger(__name__)

DEEP_MAX_REQUESTS = 300          # Range reads per archive before its listing is left partial
DEEP_MAX_BYTES = 256 << 20       # bytes read per archive: a walk must never become a download
MAX_SINGLE_READ = 64 << 20       # one read larger than this (a consumer's read(-1)) is refused
TAR_MIN_CHUNK = 64 << 10         # tar walk read size: grows with the members seen ...
TAR_MAX_CHUNK = 8 << 20          # ... up to this
NESTED_MAX_MEMBERS = 40          # nested archives peeked per outer zip (VASP-hinting names first)
NESTED_HEAD_BYTES = 2 << 20      # compressed bytes read from each nested member (its leading
                                 # members show in the first MB, as a VASP run dir writes early)
SEVENZIP_TIMEOUT = 30.0          # s a 7z header decode may take (100k entries: ~1 s incl. start)
SEVENZIP_MEMORY = 2 << 30        # address-space cap of the decoding child (py7zr: ~1.2 KB/entry)
_LOCAL_SLACK = 1024              # read past a local header for its name + extra field in one go
_HINT = re.compile(r"vasp|dft|calc|relax|scf|outcar|vasprun|aimd|neb|phonon|defect|slab|bulk",
                   re.IGNORECASE)
# the name py7zr gives an entry stored WITHOUT one when the archive's own name is not known — any
# such entry makes the listing partial (fetch would name it after the file: unknown here)
_NAMELESS = "\x00census-nameless"
_LFH = b"PK\x03\x04"
_END_SIGS = (b"PK\x01\x02", b"PK\x05\x06", b"PK\x06\x06", b"PK\x06\x07")


class DeepPeekError(Exception):
    """A Range read failed (transient: the file stays unresolved and is re-peeked next run)."""


class BudgetSpent(DeepPeekError):
    """The per-archive request / byte budget ran out: the listing so far is partial, not wrong."""


class BadRange(Exception):
    """A read outside the file (a corrupt directory entry): deterministic, never sent."""


class _Budget:
    """Range reads and bytes left for one deep peek (shared by every read it makes, nested walks
    included): the byte cap keeps a walk over thousands of small members from turning into a
    download of the archive. Reads are streamed and cut at the bytes asked for, so a server that
    ignored the Range could never make one read pull the whole file."""

    def __init__(self, max_requests: int, max_bytes: int = DEEP_MAX_BYTES):
        self.left = max_requests
        self.used = 0
        self.bytes_left = max_bytes
        self.bytes_read = 0

    def fetch(self, session: requests.Session, url: str, start: int, end: int) -> bytes:
        if start < 0 or end < start:
            raise BadRange(f"range {start}-{end}")
        n = end - start + 1
        if self.left <= 0:
            raise BudgetSpent("request budget spent")
        if n > MAX_SINGLE_READ or n > self.bytes_left:
            raise BudgetSpent(f"read of {n} B over the byte budget")
        self.left -= 1
        self.used += 1
        chunks: list[bytes] = []
        got = 0
        try:
            with _ranged_get(session, url, f"bytes={start}-{end}", stream=True) as r:
                for chunk in r.iter_content(1 << 20):
                    chunks.append(chunk[:n - got])
                    got += len(chunks[-1])
                    if got >= n:
                        break
        except (RemoteZipError, requests.RequestException) as exc:
            raise DeepPeekError(f"range {start}-{end}: {exc}"[:200]) from exc
        finally:
            self.bytes_left -= got
            self.bytes_read += got
        return b"".join(chunks)


class RangeFile(io.RawIOBase):
    """A read-only, seekable view of bytes ``[start, start + length)`` of a remote file, served by
    HTTP Range reads of at least ``block`` bytes (the last read is cached) under a :class:`_Budget`.
    Explicit ranges only — never a suffix range (Zenodo breaks suffix ranges longer than the file).
    ``name`` is the archive's own file name: py7zr names an entry stored without one after it
    (7-Zip's ``-si``), exactly as it does for the file the shared fetch opens."""

    def __init__(self, session: requests.Session, url: str, size: int, budget: _Budget,
                 start: int = 0, length: int | None = None, block: int = TAR_MIN_CHUNK,
                 name: str | None = None):
        super().__init__()
        self.session, self.url, self.budget = session, url, budget
        self.base = start
        self.length = (size - start) if length is None else length
        self.block = block
        self.name = name
        self.pos = 0
        self._cache_at = -1
        self._cache = b""

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self.pos + offset
        elif whence == io.SEEK_END:
            pos = self.length + offset
        else:
            raise ValueError(f"bad whence {whence}")
        if pos < 0:
            # a real file raises OSError(EINVAL) here, and zipfile relies on it (its ZIP64 probe
            # seeks before the start of a tiny archive and catches only OSError)
            raise OSError(errno.EINVAL, "negative seek position")
        self.pos = pos
        return pos

    def read(self, n: int | None = -1) -> bytes:
        if n is None or n < 0:
            n = self.length - self.pos
        n = max(0, min(n, self.length - self.pos))
        if n == 0:
            return b""
        out = self.read_at(self.pos, n)
        self.pos += len(out)
        return out

    def readinto(self, b: Any) -> int:
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    def read_at(self, pos: int, n: int) -> bytes:
        n = max(0, min(n, self.length - pos))
        if n == 0:
            return b""
        at, c = self._cache_at, self._cache
        if at >= 0 and at <= pos and pos + n <= at + len(c):
            return c[pos - at:pos - at + n]
        end = min(self.length, pos + max(n, self.block))
        data = self.budget.fetch(self.session, self.url, self.base + pos, self.base + end - 1)
        self._cache_at, self._cache = pos, data
        return data[:n]


def _is_primary(name: str) -> bool:
    """A VASP output the shared fetch would parse (an AppleDouble ``._vasprun.xml`` sidecar is not)."""
    if _is_junk_member(name):
        return False
    base = name.rsplit("/", 1)[-1]
    return bool(_PARSE_RE.search(base)) and _unit_role(base) in _PRIMARY_ROLES


SMALL_MEMBER = 256 << 10         # a member below this is likely followed by more headers nearby


def _next_chunk(chunk: int, member_size: int) -> int:
    """Tar walk read size for the NEXT fetch: doubles along a run of small members (their headers
    come many to a read) and falls back to the minimum after a big one (the next header is far
    away, so reading past it would fetch member data for nothing)."""
    return min(TAR_MAX_CHUNK, chunk * 2) if member_size < SMALL_MEMBER else TAR_MIN_CHUNK


def _evidence(mode: str, names: list[str], complete: bool, budget: _Budget,
              **extra: Any) -> dict[str, Any]:
    return {"mode": mode, "status": "ok", **names_evidence(names), "complete": complete,
            "requests": budget.used, "bytes_read": budget.bytes_read, **extra}


def _walk(f: RangeFile, stop_on_primary: bool, names: list[str]) -> tuple[bool, str | None]:
    """The tar walk proper, appending to ``names`` (so a budget cut keeps what was listed):
    ``(complete, status_if_not_a_tar)``; raises the budget / Range errors of ``f``. Mirrors tarfile
    (``TarInfo._proc_member``): GNU long name / PAX headers set the next member's name and size; data
    follows only regular and unknown types; old-GNU sparse members with extension blocks end the
    walk (partial)."""
    total = f.length
    off = 0
    pax: dict[str, str] = {}
    long_name: str | None = None
    while True:
        if off + 512 > total:
            # exact only when the last member ended EXACTLY at the end of the file (a size field
            # pointing past it is a truncated or corrupt tar: what was read is partial)
            return off == total and bool(names), None
        f.seek(off)
        block = f.read(512)
        if len(block) < 512:
            return False, None
        if block == b"\0" * 512:
            return True, None
        if not _header_ok(block):
            return False, ("not_tar" if off == 0 else None)
        msize = _tar_size(block[124:136])
        if msize is None:
            return False, None
        typeflag = block[156:157]
        data_off = off + 512
        if typeflag in META_TYPES:
            if msize > (1 << 20):
                return False, None                       # absurd metadata member
            if typeflag in (b"L", b"x", b"X"):
                f.seek(data_off)
                data = f.read(msize)
                if len(data) < msize:
                    return False, None
                if typeflag == b"L":
                    long_name = data.split(b"\0", 1)[0].decode("utf-8", "replace")
                else:
                    pax = pax_records(data)
            off = data_off + (msize + 511) // 512 * 512
            continue
        if typeflag == b"S" and block[482]:
            return False, None                           # sparse extension blocks: stop
        real = pax_size(pax)
        size = msize if real is None else real
        if typeflag in REGULAR_TYPES:
            names.append(pax.get("path") or long_name or member_name(block))
            if stop_on_primary and _is_primary(names[-1]):
                return False, None
        pax, long_name = {}, None
        f.block = _next_chunk(f.block, size)
        off = data_off + data_span(typeflag, size)


def tar_walk(session: requests.Session, url: str, size: int, *,
             max_requests: int = DEEP_MAX_REQUESTS, max_bytes: int = DEEP_MAX_BYTES,
             stop_on_primary: bool = True, start: int = 0, length: int | None = None,
             budget: _Budget | None = None) -> dict[str, Any]:
    """List an UNCOMPRESSED tar by hopping from member header to member header over Range.

    ``complete`` when the end-of-archive block (or the exact end of the file at a member boundary)
    is reached; stops early at the first VASP primary when ``stop_on_primary``; a header whose
    checksum / size does not verify ends the walk (partial). ``start``/``length`` walk a tar stored
    inside another file (a zip member), charging the caller's ``budget``."""
    budget = budget or _Budget(max_requests, max_bytes)
    f = RangeFile(session, url, size, budget, start=start, length=length)
    names: list[str] = []
    try:
        complete, odd = _walk(f, stop_on_primary, names)
    except BudgetSpent:
        return _evidence("deep_tar", names, False, budget, budget_spent=True)
    except DeepPeekError as exc:
        return {"mode": "deep_tar", "status": f"peek_failed: {exc}"[:200],
                "requests": budget.used, "bytes_read": budget.bytes_read}
    except Exception as exc:  # noqa: BLE001 - corrupt data: a deterministic, cacheable failure
        return {"mode": "deep_tar", "status": f"walk_failed: {type(exc).__name__}"[:120],
                "requests": budget.used, "bytes_read": budget.bytes_read}
    if odd:
        return {"mode": "deep_tar", "status": odd, "requests": budget.used,
                "bytes_read": budget.bytes_read}
    return _evidence("deep_tar", names, complete, budget)


# --- 7z: py7zr in a killable child, fed by the parent's budgeted Range reads ------------------

class _PipeFile(io.RawIOBase):
    """The child's view of the archive: every read goes to the parent over a pipe."""

    def __init__(self, conn: Any, length: int, name: str | None):
        super().__init__()
        self.conn, self.length, self.name, self.pos = conn, length, name, 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        pos = {io.SEEK_SET: offset, io.SEEK_CUR: self.pos + offset,
               io.SEEK_END: self.length + offset}[whence]
        if pos < 0:
            raise OSError(errno.EINVAL, "negative seek position")
        self.pos = pos
        return pos

    def read(self, n: int | None = -1) -> bytes:
        if n is None or n < 0:
            n = self.length - self.pos
        n = max(0, min(n, self.length - self.pos))
        if n == 0:
            return b""
        self.conn.send(("read", self.pos, n))
        data = self.conn.recv()
        if data is None:
            raise OSError(errno.EIO, "read refused by the parent (budget)")
        self.pos += len(data)
        return data

    def readinto(self, b: Any) -> int:
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)


def _sevenzip_child(conn: Any, length: int, name: str | None, mem_cap: int) -> None:
    """Child process: list a 7z through the parent's reads (a corrupt packed header can make py7zr
    spin forever or balloon — the parent kills this process on a timeout; the rlimit bounds RAM)."""
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (mem_cap, mem_cap))
    except (ImportError, ValueError, OSError):  # pragma: no cover - platform without rlimits
        pass
    try:
        with py7zr.SevenZipFile(cast(BinaryIO, _PipeFile(conn, length, name)), mode="r") as z:
            names = [i.filename for i in z.list() if not i.is_directory]
        conn.send(("ok", names))
    except BaseException as exc:  # noqa: BLE001 - report every failure to the parent
        try:
            conn.send(("err", type(exc).__name__))
        except Exception:  # noqa: BLE001 - the parent has gone
            pass


def _mp_context() -> Any:
    try:
        return mp.get_context("forkserver")
    except ValueError:  # pragma: no cover - no forkserver on this platform
        return mp.get_context("spawn")


def _sevenzip_names(f: RangeFile, timeout: float | None = None,
                    mem_cap: int | None = None) -> tuple[list[str] | None, str]:
    """Regular-file names of the 7z archive ``f`` holds, decoded by py7zr in a child process that
    asks the parent for each read (so pacing and the budget stay here) and is killed after
    ``timeout`` s. ``(names, "ok")``, or ``(None, "encrypted" / "unreadable: …")``; raises the
    parent's budget / transient Range errors."""
    if py7zr is None:
        return None, "peek_failed: py7zr not installed"
    timeout = SEVENZIP_TIMEOUT if timeout is None else timeout
    mem_cap = SEVENZIP_MEMORY if mem_cap is None else mem_cap
    ctx = _mp_context()
    parent, child = ctx.Pipe()
    as_name = f.name or f"{_NAMELESS}.7z"
    proc = ctx.Process(target=_sevenzip_child, args=(child, f.length, as_name, mem_cap),
                       daemon=True)
    proc.start()
    child.close()
    deadline = time.monotonic() + timeout
    pending: BaseException | None = None
    try:
        while True:
            left = deadline - time.monotonic()
            if left <= 0 or not parent.poll(left):
                return None, "unreadable: timeout"
            try:
                msg = parent.recv()
            except EOFError:
                if pending is not None:
                    raise pending
                # an environment failure (the child could not start / was killed): retry next run
                raise DeepPeekError("7z decoder process died") from None
            if msg[0] == "read":
                try:
                    parent.send(f.read_at(int(msg[1]), int(msg[2])))
                except (DeepPeekError, BadRange) as exc:
                    pending = exc                   # the child fails its read, then reports
                    parent.send(None)
            elif msg[0] == "ok":
                names = list(msg[1])
                if any(n.rsplit("/", 1)[-1] == _NAMELESS for n in names):
                    return [n for n in names if n.rsplit("/", 1)[-1] != _NAMELESS], "nameless"
                return names, "ok"
            else:
                if pending is not None:
                    raise pending
                name = str(msg[1])
                return None, "encrypted" if "Password" in name else f"unreadable: {name}"[:120]
    finally:
        if proc.is_alive():
            proc.kill()
        proc.join(5)
        parent.close()


def sevenzip_peek(session: requests.Session, url: str, size: int, *, name: str | None = None,
                  max_requests: int = DEEP_MAX_REQUESTS,
                  max_bytes: int = DEEP_MAX_BYTES) -> dict[str, Any]:
    """List a 7z archive from its end header (two or three Range reads). ``name`` = the file's own
    name (for entries stored without one)."""
    budget = _Budget(max_requests, max_bytes)
    f = RangeFile(session, url, size, budget, block=256 << 10, name=name)
    try:
        names, status = _sevenzip_names(f)
    except BudgetSpent:
        return {"mode": "deep_7z", "status": "budget_spent", "requests": budget.used}
    except (DeepPeekError, BadRange) as exc:
        return {"mode": "deep_7z", "status": f"peek_failed: {exc}"[:200], "requests": budget.used}
    if names is None:
        return {"mode": "deep_7z", "status": status, "requests": budget.used}
    return _evidence("deep_7z", names, status == "ok", budget)


# --- zips: local-header walk, nested archives --------------------------------------------------

def _zip64_local_csize(extra: bytes, csize: int, usize: int) -> int | None:
    """The compressed size from a local header's ZIP64 extra field (usize then csize, each present
    only when its 32-bit field is saturated)."""
    i = 0
    while i + 4 <= len(extra):
        tag, ln = struct.unpack("<HH", extra[i:i + 4])
        body = extra[i + 4:i + 4 + ln]
        if tag == 0x0001:
            vals = [struct.unpack("<Q", body[j:j + 8])[0] for j in range(0, len(body) - 7, 8)]
            k = 0
            if usize == 0xFFFFFFFF:
                k += 1
            if csize == 0xFFFFFFFF:
                return vals[k] if k < len(vals) else None
            return csize
        i += 4 + ln
    return None


def _deflate_end(buf: bytes, start: int) -> int | None:
    """Where the raw deflate stream starting at ``buf[start]`` ends, or None (not in ``buf`` /
    corrupt / more than ``MAX_DECOMPRESSED`` of output — the work stays bounded)."""
    d = zlib.decompressobj(-15)
    view = memoryview(buf)[start:]
    produced = 0
    try:
        while view and not d.eof:
            chunk = view[:1 << 20]
            produced += len(d.decompress(chunk, 1 << 20))
            while d.unconsumed_tail and not d.eof:          # output-bound: drain it
                if produced > MAX_DECOMPRESSED:
                    return None
                produced += len(d.decompress(d.unconsumed_tail, 1 << 20))
            if produced > MAX_DECOMPRESSED:
                return None
            view = view[len(chunk):]
    except zlib.error:
        return None
    if not d.eof:
        return None
    return len(buf) - len(view) - len(d.unused_data)


def zip_local_names(buf: bytes) -> tuple[list[str], bool]:
    """Member names from the START of a zip byte string (its sequential local headers), for an inner
    zip that is deflated inside its outer zip (so its central directory cannot be read in place):
    ``(names, complete)`` — complete once the central directory signature is reached. A member with
    a trailing data descriptor is skipped by inflating it (deflate, bounded) — a STORED one with a
    descriptor has no findable end, which ends the walk."""
    names: list[str] = []
    pos = 0
    while pos + 30 <= len(buf):
        sig = buf[pos:pos + 4]
        if sig in _END_SIGS:
            return names, True
        if sig != _LFH:
            return names, False
        (_ver, flags, method, _tm, _dt, _crc, csize, usize,
         n_len, e_len) = struct.unpack("<HHHHHIIIHH", buf[pos + 4:pos + 30])
        data_at = pos + 30 + n_len + e_len
        if data_at > len(buf):
            return names, False
        raw = buf[pos + 30:pos + 30 + n_len]
        name = raw.decode("utf-8" if flags & 0x800 else "cp437", "replace")
        if not name.endswith("/"):
            names.append(name)
        if flags & 0x08:                                   # sizes follow the data
            if method != 8:
                return names, False
            end = _deflate_end(buf, data_at)
            if end is None:
                return names, False
            pos = end
            if buf[pos:pos + 4] == b"PK\x07\x08":
                pos += 4
            # crc + 32-bit sizes (12 bytes) or ZIP64 sizes (20): whichever lands on a signature
            nxt = pos + 12
            if buf[nxt:nxt + 4] not in (_LFH, *_END_SIGS) and buf[pos + 20:pos + 24] in (
                    _LFH, *_END_SIGS):
                nxt = pos + 20
            pos = nxt
            continue
        if csize == 0xFFFFFFFF or usize == 0xFFFFFFFF:
            c64 = _zip64_local_csize(buf[pos + 30 + n_len:data_at], csize, usize)
            if c64 is None:
                return names, False
            csize = c64
        pos = data_at + csize
    return names, False


def _members(session: requests.Session, url: str, size: int,
             budget: _Budget) -> list[ZipMember]:
    """The outer zip's members, its central directory read through the budget (zipfile over a
    RangeFile: ZIP64, prepended data and >65,535-entry directories as zipfile handles them).
    Encrypted members are left out (never listable) — they cannot then count toward a proof, since
    a proof needs EVERY nested archive listed and an encrypted one is counted as nested below."""
    f = RangeFile(session, url, size, budget, block=256 << 10)
    with zipfile.ZipFile(cast(BinaryIO, f)) as zf:
        return [ZipMember(i.filename, -1 if i.flag_bits & 0x1 else i.compress_type,
                          i.compress_size, i.file_size, i.CRC, i.header_offset)
                for i in zf.infolist()]


def _inner_evidence(session: requests.Session, url: str, size: int, m: ZipMember,
                    head_bytes: int, budget: _Budget) -> dict[str, Any]:
    """What one archive NESTED in a zip holds: its local header + first ``head_bytes`` in one read,
    inflated when deflated, then listed by kind."""
    base = m.name.rsplit("/", 1)[-1]
    kind = _nested_archive_kind(base)
    empty = {"n_members": 0, "n_primary": 0, "n_vasp_named": 0, "n_heavy": 0, "n_nested": 0,
             "complete": False}
    if m.method not in (0, 8):                            # -1 = encrypted, or an odd method
        return {**empty, "why": f"method {m.method}"}
    if m.local_offset + 30 > size:
        return {**empty, "why": "local header past the end"}
    take = min(m.comp_size, head_bytes)
    raw = budget.fetch(session, url, m.local_offset,
                       min(size, m.local_offset + 30 + _LOCAL_SLACK + take) - 1)
    if len(raw) < 30 or raw[:4] != _LFH:
        return {**empty, "why": "bad local header"}
    n_len, e_len = struct.unpack("<HH", raw[26:30])
    data_at = 30 + n_len + e_len
    data_start = m.local_offset + data_at
    if data_start + m.comp_size > size:
        return {**empty, "why": "member runs past the end"}
    if data_at + take > len(raw):                          # a long name / extra field
        raw = budget.fetch(session, url, m.local_offset, data_start + take - 1)
    data = raw[data_at:data_at + take]
    whole = m.comp_size <= head_bytes and len(data) == m.comp_size
    if m.method == 8:
        d = zlib.decompressobj(-15)
        try:
            body = d.decompress(data, MAX_DECOMPRESSED)
        except zlib.error:
            return {**empty, "why": "inflate failed"}
        whole = whole and d.eof
    else:
        body = data
    if kind == "zip":
        if m.method == 0:                                  # stored: its own central directory
            f = RangeFile(session, url, size, budget, start=data_start, length=m.comp_size,
                          block=256 << 10, name=base)
            try:
                with zipfile.ZipFile(cast(BinaryIO, f)) as zf:
                    infos = zf.infolist()
            except DeepPeekError:
                raise
            except Exception as exc:  # noqa: BLE001 - corrupt / odd inner zip: not listable
                return {**empty, "why": f"inner zip unreadable: {type(exc).__name__}"}
            names = [i.filename for i in infos if not i.filename.endswith("/")]
            ev = names_evidence(names)
            # an encrypted member cannot be extracted — nor listed further if it is an archive
            locked = any(i.flag_bits & 0x1 and _nested_archive_kind(i.filename.rsplit("/", 1)[-1])
                         for i in infos)
            return {**ev, "complete": not locked}
        names, done = zip_local_names(body)
        return {**names_evidence(names), "complete": whole and done}
    if kind == "sevenzip":
        if m.method != 0:
            return {**empty, "why": "deflated 7z"}
        f = RangeFile(session, url, size, budget, start=data_start, length=m.comp_size,
                      block=256 << 10, name=base)
        names7, status7 = _sevenzip_names(f)
        if names7 is None:
            return {**empty, "why": status7}
        return {**names_evidence(names7), "complete": status7 == "ok"}
    if kind in ("tar", "tarzst"):
        if m.method == 0 and _header_ok(body[:512]):
            # a stored UNCOMPRESSED tar: walk it in place (same budget) instead of its head only
            walked = tar_walk(session, url, size, start=data_start, length=m.comp_size,
                              budget=budget)
            if str(walked.get("status", "")).startswith("peek_failed"):
                raise DeepPeekError(str(walked["status"]))      # transient: never cached
            if walked.get("status") == "ok":
                return {k: walked[k] for k in ("n_members", "n_primary", "primary_sample",
                                               "n_vasp_named", "n_heavy", "n_nested",
                                               "nested_sample", "complete")}
        hev, status = head_evidence(body, len(body) if whole else 0, m.name)
        if hev is None:
            if status == "is_zip":                          # a zip named .tar*: walk its headers
                names, done = zip_local_names(body)
                return {**names_evidence(names), "complete": whole and done}
            return {**empty, "why": status}
        ev = hev.evidence()
        return {k: ev[k] for k in ("n_members", "n_primary", "primary_sample", "n_vasp_named",
                                   "n_heavy", "n_nested", "nested_sample", "complete")}
    return {**empty, "why": f"{kind} not peekable in place"}


def nested_zip_peek(session: requests.Session, url: str, size: int, *,
                    max_members: int = NESTED_MAX_MEMBERS, head_bytes: int = NESTED_HEAD_BYTES,
                    max_requests: int = DEEP_MAX_REQUESTS,
                    max_bytes: int = DEEP_MAX_BYTES) -> dict[str, Any]:
    """Look INSIDE the archives a zip holds (the outer zip itself listed no VASP primary).

    Nested members with a VASP-hinting name go first; the walk stops at the first one that holds
    a primary. ``complete`` only when every nested archive was listed exactly and none nests further
    — then "no VASP" is proof for the whole zip. Evidence comes from INSIDE the nested archives
    (the outer listing was already exact: its own VASP-named inputs were not evidence before and
    are not now)."""
    budget = _Budget(max_requests, max_bytes)
    try:
        members = _members(session, url, size, budget)
    except BudgetSpent:
        return {"mode": "deep_zip", "status": "budget_spent", "complete": False,
                "requests": budget.used, "bytes_read": budget.bytes_read}
    except DeepPeekError as exc:
        return {"mode": "deep_zip", "status": f"peek_failed: {exc}"[:200], "complete": False,
                "requests": budget.used}
    except Exception as exc:  # noqa: BLE001 - not readable as a zip: deterministic
        return {"mode": "deep_zip", "status": f"not_zip: {type(exc).__name__}"[:120],
                "complete": False, "requests": budget.used}
    outer = zip_evidence(members)
    nested = [m for m in members if not m.name.endswith("/") and not _is_junk_member(m.name)
              and _nested_archive_kind(m.name.rsplit("/", 1)[-1]) is not None]
    nested.sort(key=lambda m: _HINT.search(m.name) is None)            # stable: hinted first
    agg = {"n_primary": len(outer.primary), "n_vasp_named": 0, "n_heavy": 0, "inner_members": 0}
    samples = list(outer.primary[:5])
    peeked = exact = 0
    try:
        for m in nested[:max_members]:
            try:
                ev = _inner_evidence(session, url, size, m, head_bytes, budget)
            except DeepPeekError:
                raise
            except Exception as exc:  # noqa: BLE001 - that inner archive stays unlisted
                ev = {"n_members": 0, "n_primary": 0, "complete": False,
                      "why": f"inner: {type(exc).__name__}"}
            peeked += 1
            agg["n_primary"] += int(ev.get("n_primary") or 0)
            agg["n_vasp_named"] += int(ev.get("n_vasp_named") or 0)
            agg["n_heavy"] += int(ev.get("n_heavy") or 0)
            agg["inner_members"] += int(ev.get("n_members") or 0)
            if ev.get("complete") and not int(ev.get("n_nested") or 0):
                exact += 1
            for s in ev.get("primary_sample") or []:
                if len(samples) < 5:
                    samples.append(f"{m.name}::{s}")
            if int(ev.get("n_primary") or 0):
                break
    except BudgetSpent:
        pass
    except DeepPeekError as exc:
        return {"mode": "deep_zip", "status": f"peek_failed: {exc}"[:200], "complete": False,
                "requests": budget.used}
    return {"mode": "deep_zip", "status": "ok", "n_members": outer.n_members, **agg,
            "primary_sample": samples, "n_nested": len(nested), "n_nested_peeked": peeked,
            "n_nested_exact": exact, "complete": bool(nested) and exact == len(nested),
            "requests": budget.used, "bytes_read": budget.bytes_read}
