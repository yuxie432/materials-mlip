"""Crash-recovery + locking tests for store.py — the safety-critical resume code.

Needs ``ase`` (real gzipped extxyz shards), so the whole module skips without it,
keeping ``test_harvest.py`` ase-free. Covers the dataset-dir lock (F2), the lenient
shard reader, ``prune_uncommitted_frames``, and numeric shard indexing.

Run: ``python -m pytest tests/test_store.py -q`` from the repo root.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("ase")

from ase import Atoms  # noqa: E402
from ase.io import write as ase_write  # noqa: E402

from zenodo_harvest.store import (  # noqa: E402
    DatasetLock,
    DatasetLockError,
    dataset_lock_is_live,
    existing_shard_paths,
    next_shard_index,
    prune_uncommitted_frames,
    read_shard_frames_lenient,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _frame(symbols: str, frame_id: str) -> Atoms:
    a = Atoms(symbols, cell=[10, 10, 10], pbc=True)
    a.info["frame_id"] = frame_id
    a.info["REF_energy"] = -1.0
    return a


def _write_valid_shard(dataset_dir: Path, index: int, frames: list[Atoms]) -> Path:
    """Write ``frames`` as a well-formed (trailer-closed) shard at ``index``."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    path = dataset_dir / f"shard-{index:05d}.extxyz.gz"
    with gzip.open(path, "wt") as fh:
        for a in frames:
            ase_write(fh, a, format="extxyz")
    return path


def _flushed_bytes(frames: list[Atoms]) -> tuple[bytes, list[int]]:
    """Serialise ``frames`` to gzip with a Z_SYNC_FLUSH boundary after each frame.

    Returns ``(full_bytes, offsets)`` where ``offsets[k]`` is the byte length of the
    decodable prefix holding frames ``0..k`` (mirrors parse's per-calc ``flush()``).
    ``full_bytes`` also has the closing trailer; slicing to an offset strips it.
    """
    bio = io.BytesIO()
    gz = gzip.GzipFile(fileobj=bio, mode="wb")
    text = io.TextIOWrapper(gz, encoding="utf-8")
    offsets: list[int] = []
    for a in frames:
        ase_write(text, a, format="extxyz")
        text.flush()
        gz.flush()  # Z_SYNC_FLUSH -> decodable boundary at bio.tell()
        offsets.append(bio.tell())
    text.close()  # writes the gzip trailer past the last offset
    return bio.getvalue(), offsets


def _ids(frames: list[Atoms]) -> list[str]:
    return [f.info.get("frame_id") for f in frames]


def _dead_pid() -> int:
    """A pid provably not running on this host (huge values >> any live pid)."""
    for pid in (2 ** 30, 2 ** 29, 4_000_000, 999_999):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return pid  # confirmed dead
        except OSError:
            continue
    pytest.skip("could not find a provably-dead pid on this host")
    raise AssertionError  # unreachable; keeps type checkers happy


# --------------------------------------------------------------------------- #
# F2 — dataset-dir lock                                                       #
# --------------------------------------------------------------------------- #

def test_lock_acquire_release_cycle(tmp_path):
    d = tmp_path / "ds"
    with DatasetLock(d) as lock:
        assert lock.path.is_file()
        info = json.loads(lock.path.read_text())
        assert info["pid"] == os.getpid() and info["hostname"] == socket.gethostname()
    assert not (d / ".parse.lock").exists()          # released on context exit
    with DatasetLock(d):                              # re-acquirable afterwards
        assert (d / ".parse.lock").is_file()


def test_lock_released_on_exception(tmp_path):
    d = tmp_path / "ds"
    with pytest.raises(RuntimeError):
        with DatasetLock(d):
            assert (d / ".parse.lock").is_file()
            raise RuntimeError("boom")
    assert not (d / ".parse.lock").exists()          # unlinked even on error


def test_lock_refuses_live_same_host_pid(tmp_path):
    d = tmp_path / "ds"
    d.mkdir()
    # A lock owned by THIS process => a provably-alive same-host pid.
    (d / ".parse.lock").write_text(json.dumps({
        "pid": os.getpid(), "hostname": socket.gethostname(),
        "started": datetime.now(timezone.utc).isoformat()}))
    with pytest.raises(DatasetLockError) as exc:
        DatasetLock(d).acquire()
    assert str(os.getpid()) in str(exc.value)
    assert (d / ".parse.lock").is_file()             # live lock left in place


def test_lock_reclaims_stale_dead_pid_same_host(tmp_path):
    d = tmp_path / "ds"
    d.mkdir()
    (d / ".parse.lock").write_text(json.dumps({
        "pid": _dead_pid(), "hostname": socket.gethostname(),
        "started": "2000-01-01T00:00:00+00:00"}))
    with DatasetLock(d):                             # stale same-host lock reclaimed
        info = json.loads((d / ".parse.lock").read_text())
        assert info["pid"] == os.getpid()            # now owned by us
    assert not (d / ".parse.lock").exists()


def test_lock_refuses_different_host(tmp_path):
    d = tmp_path / "ds"
    d.mkdir()
    # Even with a dead-looking pid, a DIFFERENT host is never assumed stale (a
    # remote node's pids can't be probed) -> fail safe.
    (d / ".parse.lock").write_text(json.dumps({
        "pid": _dead_pid(), "hostname": socket.gethostname() + "-OTHER-NODE",
        "started": "2000-01-01T00:00:00+00:00"}))
    with pytest.raises(DatasetLockError) as exc:
        DatasetLock(d).acquire()
    assert "DIFFERENT host" in str(exc.value)
    assert (d / ".parse.lock").is_file()             # left for manual removal


def test_dataset_lock_is_live_classification(tmp_path):
    d = tmp_path / "ds"
    d.mkdir()
    assert dataset_lock_is_live(d) is None           # no lock
    (d / ".parse.lock").write_text(json.dumps({
        "pid": os.getpid(), "hostname": socket.gethostname(), "started": "x"}))
    assert dataset_lock_is_live(d) is not None        # live same-host pid
    (d / ".parse.lock").write_text(json.dumps({
        "pid": _dead_pid(), "hostname": socket.gethostname(), "started": "x"}))
    assert dataset_lock_is_live(d) is None            # stale same-host -> not live
    (d / ".parse.lock").write_text(json.dumps({
        "pid": _dead_pid(), "hostname": "some-other-host", "started": "x"}))
    assert dataset_lock_is_live(d) is not None        # different host -> assume live


# --------------------------------------------------------------------------- #
# read_shard_frames_lenient                                                   #
# --------------------------------------------------------------------------- #

def test_lenient_reads_intact_shard(tmp_path):
    frames = [_frame("H2O", f"c#{i}") for i in range(3)]
    path = _write_valid_shard(tmp_path, 0, frames)
    got, truncated = read_shard_frames_lenient(path)
    assert truncated is False
    assert _ids(got) == ["c#0", "c#1", "c#2"]


def test_lenient_salvages_gzip_truncated_prefix(tmp_path):
    frames = [_frame("H2O", f"c#{i}") for i in range(4)]
    full, offsets = _flushed_bytes(frames)
    path = tmp_path / "shard-00000.extxyz.gz"
    path.write_bytes(full[: offsets[1]])             # cut mid-stream at a flush boundary
    got, truncated = read_shard_frames_lenient(path)
    assert truncated is True
    assert got and _ids(got) == _ids(frames)[: len(got)]   # a whole-frame prefix
    assert len(got) < 4                              # tail genuinely lost


def test_lenient_handles_torn_final_block(tmp_path):
    # A valid gzip whose LAST extxyz block is torn (count line claims atoms that
    # never follow). gzip decodes fine; the frame walker stops at the torn block.
    frames = [_frame("H2O", f"c#{i}") for i in range(2)]
    buf = io.StringIO()
    for a in frames:
        ase_write(buf, a, format="extxyz")
    text = buf.getvalue() + "3\n"                    # header claiming 3 atoms, nothing after
    path = tmp_path / "shard-00000.extxyz.gz"
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    got, truncated = read_shard_frames_lenient(path)
    assert truncated is True
    assert _ids(got) == ["c#0", "c#1"]               # complete prefix salvaged


# --------------------------------------------------------------------------- #
# prune_uncommitted_frames                                                    #
# --------------------------------------------------------------------------- #

def test_prune_deletes_wholly_orphan_top_shard(tmp_path):
    # (i) top shard is all-orphan -> deleted; walk continues down to the clean one.
    _write_valid_shard(tmp_path, 0, [_frame("H", "A0"), _frame("He", "A1")])
    _write_valid_shard(tmp_path, 1, [_frame("Li", "B0"), _frame("Be", "B1")])
    stats = prune_uncommitted_frames(tmp_path, committed_frame_ids={"A0", "A1"})
    assert stats == {"shards_deleted": 1, "shards_rewritten": 0, "frames_dropped": 2}
    assert [p.name for p in existing_shard_paths(tmp_path)] == ["shard-00000.extxyz.gz"]
    kept, _ = read_shard_frames_lenient(tmp_path / "shard-00000.extxyz.gz")
    assert _ids(kept) == ["A0", "A1"]                # older shard untouched


def test_prune_rewrites_mixed_boundary_shard(tmp_path):
    # (ii) boundary shard has committed C0 + orphan D0 -> rewritten to C0, walk stops.
    _write_valid_shard(tmp_path, 0, [_frame("H", "A0"), _frame("He", "A1")])
    _write_valid_shard(tmp_path, 1, [_frame("Li", "C0"), _frame("Be", "D0")])
    stats = prune_uncommitted_frames(tmp_path, committed_frame_ids={"A0", "A1", "C0"})
    assert stats == {"shards_deleted": 0, "shards_rewritten": 1, "frames_dropped": 1}
    top, _ = read_shard_frames_lenient(tmp_path / "shard-00001.extxyz.gz")
    assert _ids(top) == ["C0"]                       # orphan D0 dropped
    base, _ = read_shard_frames_lenient(tmp_path / "shard-00000.extxyz.gz")
    assert _ids(base) == ["A0", "A1"]                # older shard untouched


def test_prune_noop_when_all_committed_and_intact(tmp_path):
    # (iii) everything committed + intact -> complete no-op (top shard read, then stop).
    _write_valid_shard(tmp_path, 0, [_frame("H", "A0"), _frame("He", "A1")])
    _write_valid_shard(tmp_path, 1, [_frame("Li", "A2"), _frame("Be", "A3")])
    before = {p.name: p.read_bytes() for p in existing_shard_paths(tmp_path)}
    stats = prune_uncommitted_frames(tmp_path, committed_frame_ids={"A0", "A1", "A2", "A3"})
    assert stats == {"shards_deleted": 0, "shards_rewritten": 0, "frames_dropped": 0}
    after = {p.name: p.read_bytes() for p in existing_shard_paths(tmp_path)}
    assert after == before                           # byte-identical, nothing rewritten


def test_prune_rewrites_truncated_but_committed_top_shard(tmp_path):
    # (iv) top shard is all-committed but gzip-truncated -> rewritten to restore trailer.
    _write_valid_shard(tmp_path, 0, [_frame("H", "A0"), _frame("He", "A1")])
    top_frames = [_frame("Li", "A2"), _frame("Be", "A3")]
    full, offsets = _flushed_bytes(top_frames)
    top = tmp_path / "shard-00001.extxyz.gz"
    top.write_bytes(full[: offsets[-1]])             # all frames flushed, trailer stripped
    assert read_shard_frames_lenient(top)[1] is True  # precondition: currently truncated

    stats = prune_uncommitted_frames(tmp_path, committed_frame_ids={"A0", "A1", "A2", "A3"})
    assert stats == {"shards_deleted": 0, "shards_rewritten": 1, "frames_dropped": 0}
    got, truncated = read_shard_frames_lenient(top)
    assert truncated is False                        # valid trailer restored
    assert _ids(got) == ["A2", "A3"]


# --------------------------------------------------------------------------- #
# next_shard_index / existing_shard_paths — numeric (not lexical) ordering    #
# --------------------------------------------------------------------------- #

def test_shard_indexing_is_numeric_not_lexical(tmp_path):
    for idx in (2, 10):                              # lexical would sort "00010" < "00002"
        (tmp_path / f"shard-{idx:05d}.extxyz.gz").write_bytes(b"")
    assert [p.name for p in existing_shard_paths(tmp_path)] == [
        "shard-00002.extxyz.gz", "shard-00010.extxyz.gz"]
    assert next_shard_index(tmp_path) == 11          # one past the highest (10), not 3
