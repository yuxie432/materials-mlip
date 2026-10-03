"""Fast text reader for the harvest's ``shard-NNNNN.extxyz.gz`` files (no ``ase.Atoms``).

The statistics scan tens of millions of frames, so they cannot afford ``ase.io.read`` (which
indexes a whole shard and builds an ``Atoms`` per frame — the reason ``verify`` moved to a text
parser, see ``store.read_shard_frame_meta_lenient``). This module splits a decompressed shard into
frames, parses each comment line into raw ``key -> string`` pairs (ASE's quoting rules), and
locates the per-atom columns from the ``Properties=`` spec, so the numeric block of many frames can
be read with ONE ``numpy.loadtxt`` call. Same lenient handling of a crash-truncated gzip as the
store's reader.
"""

from __future__ import annotations

import gzip
import math
import re
import zlib
from pathlib import Path

from zenodo_harvest.store import _decompress_gzip_prefix

# key=value | key="quoted value" (ASE writes vectors, pbc and strings with spaces quoted).
_KV_RE = re.compile(r'([A-Za-z_][^\s="]*)=(?:"((?:[^"\\]|\\.)*)"|(\S*))')


def read_shard_text(path: str | Path) -> tuple[str, bool]:
    """Whole decompressed shard text and whether a torn gzip tail had to be dropped."""
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            return fh.read(), False
    except (EOFError, OSError, zlib.error):
        return _decompress_gzip_prefix(Path(path)), True


def parse_comment(line: str) -> dict[str, str]:
    """An extxyz comment line -> ``{key: raw string value}`` (quotes removed, bare keys dropped)."""
    out: dict[str, str] = {}
    for m in _KV_RE.finditer(line):
        val = m.group(2)
        out[m.group(1)] = val if val is not None else m.group(3)
    return out


def parse_properties(spec: str) -> tuple[dict[str, tuple[int, int, str]], int]:
    """``species:S:1:pos:R:3:REF_forces:R:3`` -> ``({name: (first column, n columns, type)}, total)``."""
    parts = spec.split(":")
    cols: dict[str, tuple[int, int, str]] = {}
    c = 0
    for i in range(0, len(parts) - 2, 3):
        name, typ, n = parts[i], parts[i + 1], int(parts[i + 2])
        cols[name] = (c, n, typ)
        c += n
    return cols, c


def frame_index(lines: list[str]) -> tuple[list[tuple[int, int]], bool]:
    """``[(header line index, natoms), ...]`` for every complete frame, and whether a torn or
    unparsable tail was cut off. Blank lines between/after frames are skipped."""
    idx: list[tuple[int, int]] = []
    i, n = 0, len(lines)
    while i < n:
        s = lines[i].strip()
        if not s:
            i += 1
            continue
        try:
            natoms = int(s)
        except ValueError:
            return idx, True
        if natoms < 0 or i + 2 + natoms > n:
            return idx, True
        idx.append((i, natoms))
        i += 2 + natoms
    return idx, False


def to_float(v: str | None) -> float:
    """A scalar info value -> float (NaN when absent or unparsable)."""
    if v is None:
        return math.nan
    try:
        return float(v)
    except ValueError:
        return math.nan


def to_floats(v: str | None) -> list[float] | None:
    """A quoted vector info value (``"1 2 3"``) -> floats, None when absent/unparsable."""
    if v is None:
        return None
    try:
        return [float(x) for x in v.split()]
    except ValueError:
        return None


def to_flag(v: str | None) -> int:
    """extxyz logical -> 1 / 0, and -1 when the key is absent (unknown)."""
    if v is None:
        return -1
    t = v.strip().upper()
    if t in ("T", "TRUE", "1"):
        return 1
    if t in ("F", "FALSE", "0"):
        return 0
    return -1
