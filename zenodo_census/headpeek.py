"""Head-peek: the leading members of a tar-family archive from ONE HTTP Range read.

A zip's central directory lists every member from the file's tail; a tar has no index, and a
compressed tar is one non-seekable stream, so its members can only be listed by reading it from
the start. The census cannot afford whole downloads of every tarball in every plausible record
(TBs, mostly non-VASP), but the FIRST few MB of a stream are cheap (one request) and say a lot:
a tar of VASP run directories shows ``INCAR``/``POSCAR``/``OUTCAR``… within its first members,
because tools write one calc directory at a time. So the head is decompressed (compression sniffed
from the magic bytes, not the name — Materials Cloud had a zstd stream named ``.tar.xz``, a zip named
``.tgz`` and a gzip inside a gzip) and its tar headers are walked: ustar prefixes, GNU long names,
PAX ``path`` records and base-256 sizes are handled, and a header whose checksum does not verify
ends the walk (the rest is data we did not read, or not a tar at all).

Verdicts are EVIDENCE, never a proof of absence — unless the whole archive fitted in the head
(``complete``: every compressed byte read, every compressed stream ended, nothing cut by the output
cap, AND the tar's own end-of-archive block reached), in which case the listing is exact and "no
VASP" means no VASP. Only regular files are listed (not directories or links — fetch extracts
regular files only).
"""

from __future__ import annotations

import bz2
import io
import logging
import lzma
import re
import struct
import time
import zlib
from dataclasses import dataclass, field
from typing import Any

import requests

from zenodo_harvest.fetch import (
    _PARSE_RE,
    _PRIMARY_ROLES,
    _is_junk_member,
    _nested_archive_kind,
    _unit_role,
)
from zenodo_harvest.models import _VASP_RE

try:
    import zstandard  # type: ignore
except ImportError:  # pragma: no cover - optional dep (the ``archives`` extra)
    zstandard = None  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_HEAD_BYTES = 8 << 20          # compressed bytes read (one Range request)
MAX_DECOMPRESSED = 128 << 20          # decompressed bytes walked at most (bomb guard)
_RETRY_STATUS = {429, 500, 502, 503, 504}

_MAGICS = (
    (b"\x1f\x8b", "gzip"),
    (b"BZh", "bzip2"),
    (b"\xfd7zXZ\x00", "xz"),
    (b"\x28\xb5\x2f\xfd", "zstd"),
    (b"PK\x03\x04", "zip"),
    (b"PK\x05\x06", "zip"),
    (b"7z\xbc\xaf\x27\x1c", "7z"),
    (b"Rar!\x1a\x07", "rar"),
)


def sniff(head: bytes) -> str:
    """Container/compression of a stream from its first bytes: gzip/bzip2/xz/zstd, a zip, 7z or
    rar (not head-peekable here), ``tar`` (a valid ustar/GNU/v7 header at offset 0), else
    ``unknown``."""
    for magic, kind in _MAGICS:
        if head.startswith(magic):
            return kind
    if len(head) >= 512 and _header_ok(head[:512]):
        return "tar"
    return "unknown"


def _header_ok(block: bytes) -> bool:
    """A tar header's checksum (sum of the 512 bytes with the checksum field as spaces) — the
    unsigned sum, or the signed-char sum some old writers stored (``tarfile`` accepts both)."""
    if len(block) < 512 or block == b"\0" * 512:
        return False
    try:
        stored = int(block[148:156].split(b"\0", 1)[0].strip() or b"-1", 8)
    except ValueError:
        return False
    rest = block[:148] + block[156:512]
    unsigned = sum(rest) + 8 * 32
    signed = sum(b - 256 if b > 127 else b for b in rest) + 8 * 32
    return stored in (unsigned, signed)


_OCTAL = re.compile(rb"^[0-7]+$")


def _tar_size(field_: bytes) -> int | None:
    """A member's size field, or None when it is not a valid size (walk stops there)."""
    if field_[:1] == b"\x80":                           # GNU base-256, positive (files >= 8 GiB)
        return int.from_bytes(field_[1:], "big")
    if field_[:1] and field_[0] & 0x80:                 # base-256 negative: invalid
        return None
    txt = field_.split(b"\0", 1)[0].strip()
    if not txt:
        return 0
    return int(txt, 8) if _OCTAL.match(txt) else None


_REGULAR = (b"0", b"\0", b"7")


def walk_tar(buf: bytes) -> tuple[list[str], bool, bool]:
    """Regular-file member names readable from the start of an (uncompressed) tar byte string:
    ``(names, end_seen, valid)`` — ``end_seen`` if the end-of-archive block was reached, ``valid``
    False if even the first header is not a tar header. A header whose checksum or size field
    does not verify ends the walk (the rest was not read, or is not a tar)."""
    names: list[str] = []
    pos = 0
    pax_path: str | None = None
    long_name: str | None = None
    while pos + 512 <= len(buf):
        block = buf[pos:pos + 512]
        if block == b"\0" * 512:
            return names, True, True
        if not _header_ok(block):
            return names, False, pos > 0
        size = _tar_size(block[124:136])
        if size is None:
            return names, False, pos > 0
        typeflag = block[156:157]
        data_end = pos + 512 + size
        if typeflag in (b"L", b"K"):              # GNU long name / long link name
            if typeflag == b"L" and data_end <= len(buf):
                long_name = buf[pos + 512:data_end].split(b"\0", 1)[0].decode("utf-8", "replace")
        elif typeflag in (b"x", b"g"):            # PAX extended header (per-file / global)
            if typeflag == b"x" and data_end <= len(buf):
                pax_path = _pax_path(buf[pos + 512:data_end]) or pax_path
        else:
            name = block[0:100].split(b"\0", 1)[0].decode("utf-8", "replace")
            if block[257:262] == b"ustar":
                prefix = block[345:500].split(b"\0", 1)[0].decode("utf-8", "replace")
                if prefix:
                    name = f"{prefix}/{name}"
            if typeflag in _REGULAR:
                names.append(pax_path or long_name or name)
            pax_path = long_name = None
        pos += 512 + (size + 511) // 512 * 512    # size >= 0, so pos always advances
    return names, False, True


def _pax_path(data: bytes) -> str | None:
    """The ``path`` value of a PAX extended header (records are ``"<len> key=value\\n"``)."""
    i = 0
    while i < len(data):
        sp = data.find(b" ", i)
        if sp < 0:
            break
        try:
            n = int(data[i:sp])
        except ValueError:
            break
        rec = data[sp + 1:i + n - 1]
        k, _, v = rec.partition(b"=")
        if k == b"path":
            return v.decode("utf-8", "replace")
        if n <= 0:
            break
        i += n
    return None


def _gzip_fname(head: bytes) -> str | None:
    """The original file name stored in a gzip header (FNAME flag), if any."""
    if len(head) < 10 or not head.startswith(b"\x1f\x8b"):
        return None
    flags = head[3]
    i = 10
    if flags & 0x04:                                   # FEXTRA
        if len(head) < i + 2:
            return None
        (xlen,) = struct.unpack("<H", head[i:i + 2])
        i += 2 + xlen
    if flags & 0x08:                                   # FNAME
        end = head.find(b"\0", i)
        if end > i:
            return head[i:end].decode("latin-1", "replace")
    return None


def _decompress(kind: str, data: bytes, limit: int, whole: bool = False) -> tuple[bytes, bool]:
    """Decompress as much of ``data`` as is there (a truncated stream is expected) up to ``limit``
    bytes: ``(out, stream_end_reached)``. Concatenated gzip/bzip2/xz streams are followed; bytes
    after the last complete stream that do not start a new one (zero padding) end it. zstd output
    is bounded too (its end is taken as "the whole file was read"). Raises on a corrupt FIRST
    stream (zlib.error / OSError / lzma.LZMAError / zstd errors)."""
    out = bytearray()
    ended = False
    if kind in ("gzip", "bzip2", "xz"):
        rest = data
        first = True
        while rest and len(out) < limit:
            d: Any
            try:
                if kind == "gzip":
                    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    out += d.decompress(rest, limit - len(out))
                elif kind == "bzip2":
                    d = bz2.BZ2Decompressor()
                    out += d.decompress(rest, max_length=limit - len(out))
                else:
                    d = lzma.LZMADecompressor()
                    out += d.decompress(rest, max_length=limit - len(out))
            except (zlib.error, OSError, EOFError, lzma.LZMAError):
                if first:
                    raise
                ended = True            # trailing padding after the last stream
                break
            first = False
            if not d.eof:
                break                   # truncated head (or the output limit): not the end
            rest = d.unused_data
            ended = not rest
    elif kind == "zstd":
        reader = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data),
                                                            read_across_frames=True)
        try:
            while len(out) < limit:
                chunk = reader.read(min(1 << 20, limit - len(out)))
                if not chunk:
                    break
                out += chunk
        except zstandard.ZstdError:
            pass                        # a truncated frame: keep what decoded
        ended = whole and len(out) < limit
    else:
        out += data[:limit]
        ended = True
    return bytes(out[:limit]), ended


@dataclass
class HeadEvidence:
    kind: str                                  # tar / gzip / bzip2 / xz / zstd / single
    names: list[str] = field(default_factory=list)
    complete: bool = False                     # the listing is exact (whole archive read)
    total_size: int = 0
    bytes_read: int = 0
    decompressed: int = 0

    def evidence(self) -> dict[str, Any]:
        return names_evidence(self.names) | {
            "kind": self.kind, "complete": self.complete, "total_size": self.total_size,
            "bytes_read": self.bytes_read, "decompressed": self.decompressed}


# Heavy VASP outputs by exact name (a looser prefix rule matched e.g. "chgnet_0.3.0.pt").
_HEAVY_NAME = re.compile(r"^(?:chgcar|chg|wavecar|doscar|eigenval|procar|locpot|elfcar|aeccar\d|"
                         r"parchg)(?:[._-].*)?$", re.IGNORECASE)


def names_evidence(names: list[str], max_names: int = 5) -> dict[str, Any]:
    """Classify member names: primaries with the shared fetch's own rules (``_PARSE_RE`` +
    ``_unit_role`` — what fetch would parse), VASP-named files with the anchored triage rule
    (``models._VASP_RE``: ``INCAR``, ``POSCAR_1``, not ``incarnation.txt``), heavy outputs by exact
    name, sub-archives."""
    primary: list[str] = []
    nested: list[str] = []
    vasp_any = heavy = 0
    for n in names:
        if n.endswith("/") or _is_junk_member(n):
            continue
        base = n.rsplit("/", 1)[-1]
        if _PARSE_RE.search(base) and _unit_role(base) in _PRIMARY_ROLES:
            primary.append(n)
        if _VASP_RE.search(base):
            vasp_any += 1
        elif _HEAVY_NAME.match(base):
            heavy += 1
        if _nested_archive_kind(base) is not None:
            nested.append(n)
    return {"n_members": len(names), "n_primary": len(primary),
            "primary_sample": primary[:max_names], "n_vasp_named": vasp_any, "n_heavy": heavy,
            "n_nested": len(nested), "nested_sample": nested[:max_names]}


def head_peek(session: requests.Session, url: str, key: str = "",
              head_bytes: int = DEFAULT_HEAD_BYTES,
              max_decompressed: int = MAX_DECOMPRESSED,
              attempts: int = 3) -> tuple[HeadEvidence | None, str]:
    """Read the first ``head_bytes`` of ``url`` and list what it holds: ``(evidence, status)``.

    Status ``ok``; ``is_zip`` / ``is_7z`` / ``is_rar`` (the stream is another container — a zip
    goes to the zip peek); ``unknown_format``; ``decompress_failed: …``; ``peek_failed: …``
    (transient — never cached). A stream that decompresses to something that is not a tar is a
    single compressed FILE (``OUTCAR.gz``-style), named from its gzip header or the key."""
    body = b""
    total = 0
    last = ""
    for attempt in range(attempts):
        try:
            with session.get(url, headers={"Range": f"bytes=0-{head_bytes - 1}"},
                             stream=True, timeout=300) as r:
                if r.status_code in _RETRY_STATUS:
                    last = f"HTTP {r.status_code}"
                    ra = r.headers.get("Retry-After")
                    time.sleep(min(float(ra) + 1 if ra and ra.isdigit() else 2 ** (attempt + 1),
                                   120))
                    continue
                if r.status_code not in (200, 206):
                    return None, f"peek_failed: HTTP {r.status_code}"
                cr = r.headers.get("Content-Range", "")
                if r.status_code == 206:
                    # without Content-Range the total size is unknown (never "whole")
                    total = int(cr.rsplit("/", 1)[-1]) if "/" in cr and cr[-1] != "*" else 0
                else:
                    total = int(r.headers.get("Content-Length") or 0)
                chunks = []
                got = 0
                for chunk in r.iter_content(1 << 20):
                    chunks.append(chunk[:head_bytes - got])
                    got += len(chunks[-1])
                    if got >= head_bytes:
                        break
                body = b"".join(chunks)
                break
        except (requests.RequestException, ValueError) as exc:
            last = f"{type(exc).__name__}: {exc}"[:160]
            continue
    else:
        return None, f"peek_failed: {last}"
    if not body:
        return None, "peek_failed: empty body"
    whole = bool(total) and len(body) >= total
    kind = sniff(body)
    if kind in ("zip", "7z", "rar"):
        return None, f"is_{kind}"
    if kind == "unknown":
        return None, "unknown_format"
    if kind == "zstd" and zstandard is None:
        return None, "peek_failed: zstandard not installed"     # environment, not the file
    try:
        data, stream_end = _decompress(kind, body, max_decompressed, whole)
    except Exception as exc:  # noqa: BLE001 - corrupt stream of any codec
        return None, f"decompress_failed: {type(exc).__name__}"[:120]
    ev = HeadEvidence(kind=kind, total_size=total, bytes_read=len(body), decompressed=len(data))
    capped = len(data) >= max_decompressed
    if len(data) < 512 and not stream_end:
        # e.g. a bzip2 block larger than the head: nothing decodable yet — no verdict
        return None, "head_too_small"
    names, end_seen, valid = walk_tar(data) if len(data) >= 512 else ([], False, False)
    if not valid:
        if kind == "tar":
            return None, "unknown_format"
        # a single compressed file, not a tarball
        name = _gzip_fname(body) if kind == "gzip" else None
        if not name:
            base = key.rsplit("/", 1)[-1]
            name = base.rsplit(".", 1)[0] if "." in base else base
        ev.kind, ev.names = "single", [name]
        ev.complete = True
        return ev, "ok"
    ev.names = names
    # exact listing only when every compressed byte was read, every stream ended, nothing was cut
    # by the cap AND the tar's own end-of-archive block was reached (a checksum or size field that
    # did not verify also ends the walk — without an end block the listing may be partial)
    ev.complete = whole and stream_end and not capped and end_seen
    return ev, "ok"
