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
from pathlib import Path
from typing import Any

from ase import Atoms
from ase.io import read as ase_read
from ase.io import write as ase_write

from .manifest import JsonlWriter


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


def prune_uncommitted_frames(
    out_dir: str | Path, committed_frame_ids: set[str], prefix: str = "shard",
) -> dict:
    """Drop orphan frames left in shards by a crashed run.

    An orphan is a frame whose ``frame_id`` is not in ``committed_frame_ids``
    (i.e. its calc never reached metadata.jsonl). Because a run writes a calc's
    frames contiguously and only then its metadata, orphans are always the *tail*
    of the written stream. So we walk shards high→low, rewriting each to keep only
    committed frames, and stop at the first shard that has no orphans (nothing
    older can be orphaned). Returns counts of what was cleaned.
    """
    stats = {"shards_rewritten": 0, "shards_deleted": 0, "frames_dropped": 0}
    for shard in reversed(existing_shard_paths(out_dir, prefix)):
        frames = ase_read(str(shard), index=":", format="extxyz")
        if not isinstance(frames, list):
            frames = [frames]
        kept = [a for a in frames if a.info.get("frame_id") in committed_frame_ids]
        dropped = len(frames) - len(kept)
        if dropped == 0:
            break  # reached the fully-committed region
        stats["frames_dropped"] += dropped
        if kept:
            _rewrite_shard(shard, kept)
            stats["shards_rewritten"] += 1
            break  # this shard straddles the crashed calc's start -> boundary reached
        shard.unlink()  # wholly orphan: the crashed calc spilled further, keep walking
        stats["shards_deleted"] += 1
    return stats
