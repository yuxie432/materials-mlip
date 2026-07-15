"""Stage 4 — storage.

Two sinks, joined by ``calc_id`` / ``frame_id`` (see docs/DESIGN.md §3):

* :class:`ShardedExtxyzWriter` — atomistic data as gzipped extxyz, ~N frames per
  shard so files stay a manageable size and parallel writers each own their
  shards. Frame headers stay small: energy/forces/stress (via an attached
  calculator) + a few quality tags + the ``frame_id``/``calc_id`` join keys.
* :class:`MetadataWriter` — one JSONL record per calculation holding the bulky,
  frame-invariant data: provenance, citation, full calc parameters, convergence,
  and availability flags.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import zlib
from pathlib import Path
from typing import Any

from ase import Atoms
from ase.io import read as ase_read
from ase.io import write as ase_write

from .manifest import JsonlWriter

logger = logging.getLogger(__name__)


class ShardedExtxyzWriter:
    """Append ASE frames to rotating ``shard-NNNNN.extxyz.gz`` files."""

    def __init__(self, out_dir: str | Path, frames_per_shard: int = 10_000,
                 prefix: str = "shard", start_index: int = 0):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.frames_per_shard = frames_per_shard
        self.prefix = prefix
        self._shard_index = start_index
        self._in_shard = 0
        self._fh: Any = None
        self.total = 0

    def _shard_path(self, i: int) -> Path:
        return self.out_dir / f"{self.prefix}-{i:05d}.extxyz.gz"

    def _open_shard(self) -> None:
        if self._fh is not None:
            self._fh.close()
        self._fh = gzip.open(self._shard_path(self._shard_index), "at")
        self._in_shard = 0

    def write(self, atoms: Atoms) -> str:
        """Write one frame; returns the shard filename it landed in."""
        if self._fh is None or self._in_shard >= self.frames_per_shard:
            if self._fh is not None:
                self._shard_index += 1
            self._open_shard()
        ase_write(self._fh, atoms, format="extxyz")
        self._in_shard += 1
        self.total += 1
        return self._shard_path(self._shard_index).name

    def flush(self) -> None:
        """Make everything written so far durable on disk.

        Callers must invoke this *after* a calc's frames are written and *before*
        recording that calc's metadata, so the crash-safety ordering the resume
        design assumes (frames-durable-then-metadata) actually holds: gzip's default
        ``flush`` emits a ``Z_SYNC_FLUSH`` point (making the committed frames a
        decompressible prefix even without the end-of-stream trailer), then
        ``os.fsync`` pushes the bytes past the OS page cache. Without this the frames
        sit in the gzip/OS buffer until :meth:`close`, so a crash could leave
        metadata referencing frame_ids that reached no shard.
        """
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "ShardedExtxyzWriter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class MetadataWriter:
    """One JSONL record per calculation (provenance + params + availability).

    Deliberately a thin wrapper over :class:`JsonlWriter` rather than an alias:
    the metadata record — not the extxyz frame — is what training-time selection
    keys off (e.g. "train only on calcs with a given functional / ENCUT / k-point
    density"). Keeping a named sink gives that metadata-specific logic (schema
    validation, derived selection fields, a Parquet mirror) one obvious home; add
    it here so callers and the frame writer are untouched. See docs/DESIGN.md §3.
    """

    def __init__(self, path: str | Path):
        self._w = JsonlWriter(path)

    def write(self, record: dict) -> None:
        self._w.write(record)

    @property
    def n(self) -> int:
        return self._w.n

    def close(self) -> None:
        self._w.close()

    def __enter__(self) -> "MetadataWriter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Resume support (safe re-runs during a long harvest).
#
# A run appends frames to shards, then writes the calc's metadata record. Two
# things must hold across a re-run: (1) frames already committed must not be
# written again (no duplicates), and (2) no shard may exceed frames_per_shard
# (no overflow). We get both by *never* re-opening an existing shard: each run
# starts at a fresh index (:func:`next_shard_index`), and the caller skips calcs
# already present in metadata. :func:`prune_uncommitted_frames` cleans up the
# only remaining hazard — frames from a calc that a crashed run wrote to a shard
# but never committed to metadata.
# ---------------------------------------------------------------------------

def _shard_index_of(path: Path) -> int:
    """``shard-00012.extxyz.gz`` -> ``12``."""
    return int(path.name.split(".", 1)[0].rsplit("-", 1)[-1])


def existing_shard_paths(out_dir: str | Path, prefix: str = "shard") -> list[Path]:
    """All shard files in ``out_dir``, sorted by numeric index (not lexically)."""
    return sorted(Path(out_dir).glob(f"{prefix}-*.extxyz.gz"), key=_shard_index_of)


def next_shard_index(out_dir: str | Path, prefix: str = "shard") -> int:
    """Index for a brand-new shard: one past the highest existing (0 if none)."""
    paths = existing_shard_paths(out_dir, prefix)
    return _shard_index_of(paths[-1]) + 1 if paths else 0


def _rewrite_shard(shard: Path, frames: list[Atoms]) -> None:
    tmp = shard.with_suffix(shard.suffix + ".tmp")
    with gzip.open(tmp, "wt") as fh:
        for atoms in frames:
            ase_write(fh, atoms, format="extxyz")
    tmp.replace(shard)  # atomic


def _decompress_gzip_prefix(path: Path) -> str:
    """Decompress as much of a possibly-truncated gzip as is intact (as text)."""
    d = zlib.decompressobj(zlib.MAX_WBITS | 16)  # 16 => gzip wrapper
    out: list[bytes] = []
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            try:
                out.append(d.decompress(chunk))
            except zlib.error:
                break
    return b"".join(out).decode("utf-8", "replace")


def read_shard_frames_lenient(path: Path) -> tuple[list[Atoms], bool]:
    """Read a shard's frames, tolerating a crash-truncated gzip / partial tail frame.

    A shard left open by a killed run has no gzip end-of-stream trailer (and its
    last frame may be half-written), so a plain :func:`ase.io.read` raises
    ``EOFError`` — which is exactly why the naive resume path aborted. Here we
    decompress the intact prefix, then parse whole extxyz frames one at a time and
    stop at the first incomplete/unparseable one. Returns ``(frames, truncated)``
    where ``truncated`` is True if any tail data had to be discarded (so the caller
    knows the shard must be rewritten to restore a valid trailer).
    """
    truncated = False
    try:
        with gzip.open(path, "rt") as fh:
            text = fh.read()
    except (EOFError, OSError, zlib.error):
        text = _decompress_gzip_prefix(path)
        truncated = True

    lines = text.splitlines()
    frames: list[Atoms] = []
    i = 0
    while i < len(lines):
        try:
            natoms = int(lines[i].strip())
        except (ValueError, IndexError):
            truncated = True
            break
        block = lines[i:i + 2 + natoms]
        if len(block) < 2 + natoms:  # last frame torn off mid-write
            truncated = True
            break
        try:
            fr = ase_read(io.StringIO("\n".join(block) + "\n"), index=0, format="extxyz")
        except Exception:  # a torn line inside the final block
            truncated = True
            break
        frames.append(fr[0] if isinstance(fr, list) else fr)
        i += 2 + natoms
    return frames, truncated


def prune_uncommitted_frames(
    out_dir: str | Path, committed_frame_ids: set[str], prefix: str = "shard",
) -> dict:
    """Drop orphan frames left in shards by a crashed run — without losing committed ones.

    An orphan is a frame whose ``frame_id`` is not in ``committed_frame_ids`` (its
    calc never reached metadata.jsonl). Because a run writes a calc's frames
    contiguously and only then its metadata, orphans are always the *tail* of the
    written stream, and only the single currently-open (highest) shard can be
    gzip-truncated. So we walk shards high→low:

    * a shard that is intact **and** has no orphans -> stop (older shards are clean);
    * a shard that is wholly orphan -> delete it and keep walking;
    * otherwise (has orphans, or is truncated even if all-committed) -> rewrite it
      keeping only committed frames (which also restores a valid gzip trailer) and
      stop, since this shard straddles the crashed calc's start.

    Committed frames are always re-written, never deleted, so a torn top shard is
    salvaged rather than discarded. Returns counts of what was cleaned.
    """
    stats = {"shards_rewritten": 0, "shards_deleted": 0, "frames_dropped": 0}
    for shard in reversed(existing_shard_paths(out_dir, prefix)):
        frames, truncated = read_shard_frames_lenient(shard)
        kept = [a for a in frames if a.info.get("frame_id") in committed_frame_ids]
        dropped = len(frames) - len(kept)
        if dropped == 0 and not truncated:
            break  # intact and fully committed -> nothing older can be orphaned
        stats["frames_dropped"] += dropped
        if not kept:
            shard.unlink()  # wholly orphan (or empty torn tail): keep walking down
            stats["shards_deleted"] += 1
            if truncated:
                logger.warning("deleted crash-truncated orphan shard %s", shard.name)
            continue
        _rewrite_shard(shard, kept)  # salvage committed frames + restore trailer
        stats["shards_rewritten"] += 1
        break  # boundary: this shard holds the crashed calc's start
    return stats
