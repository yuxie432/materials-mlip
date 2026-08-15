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
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("ase")

from zenodo_harvest import parse as parse_mod  # noqa: E402
from zenodo_harvest.manifest import RejectionLogger, read_jsonl  # noqa: E402

from ase import Atoms  # noqa: E402
from ase.io import write as ase_write  # noqa: E402

from zenodo_harvest import store as store_mod  # noqa: E402
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
    assert dataset_lock_is_live(d) is not None        # different host, no job id -> assume live


# --------------------------------------------------------------------------- #
# F2b — SLURM-aware cross-host staleness (reclaim a wallclock-orphaned lock)   #
# --------------------------------------------------------------------------- #

class _FakeProc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def _patch_squeue(monkeypatch, *, present=True, result=None, raises=None):
    """Simulate the ``squeue`` probe inside store._slurm_job_active."""
    monkeypatch.setattr(store_mod.shutil, "which",
                        lambda name: "/usr/bin/squeue" if present else None)

    def fake_run(cmd, **kw):
        if raises is not None:
            raise raises
        return result

    monkeypatch.setattr(store_mod.subprocess, "run", fake_run)


def test_slurm_job_active_running(monkeypatch):
    _patch_squeue(monkeypatch, result=_FakeProc(0, "RUNNING\n"))
    assert store_mod._slurm_job_active("12345") is True


def test_slurm_job_active_pending(monkeypatch):
    _patch_squeue(monkeypatch, result=_FakeProc(0, "PENDING\n"))
    assert store_mod._slurm_job_active("12345") is True


def test_slurm_job_active_gone_empty_output_is_false(monkeypatch):
    # squeue ran cleanly but the job is not in the queue -> it has ended.
    _patch_squeue(monkeypatch, result=_FakeProc(0, "\n"))
    assert store_mod._slurm_job_active("12345") is False


def test_slurm_job_active_invalid_job_id_is_false(monkeypatch):
    _patch_squeue(monkeypatch, result=_FakeProc(
        1, "", "slurm_load_jobs error: Invalid job id specified"))
    assert store_mod._slurm_job_active("12345") is False


def test_slurm_job_active_transient_error_is_undeterminable(monkeypatch):
    _patch_squeue(monkeypatch, result=_FakeProc(1, "", "socket timeout talking to slurmctld"))
    assert store_mod._slurm_job_active("12345") is None


def test_slurm_job_active_no_squeue_binary_is_undeterminable(monkeypatch):
    _patch_squeue(monkeypatch, present=False)
    assert store_mod._slurm_job_active("12345") is None


def test_slurm_job_active_subprocess_raises_is_undeterminable(monkeypatch):
    _patch_squeue(monkeypatch, raises=OSError("boom"))
    assert store_mod._slurm_job_active("12345") is None


def test_slurm_job_active_malformed_id_never_shells_out(monkeypatch):
    # A non-numeric / missing id short-circuits to None before touching squeue.
    monkeypatch.setattr(store_mod.shutil, "which",
                        lambda name: pytest.fail("should not query squeue"))
    assert store_mod._slurm_job_active("not-a-job") is None
    assert store_mod._slurm_job_active(None) is None


def test_slurm_job_active_accepts_array_task_id(monkeypatch):
    _patch_squeue(monkeypatch, result=_FakeProc(0, "RUNNING\n"))
    assert store_mod._slurm_job_active("12345_7") is True


def _cross_host_lock(d: Path, job_id: str = "424242") -> None:
    d.mkdir(exist_ok=True)
    (d / ".parse.lock").write_text(json.dumps({
        "pid": _dead_pid(), "hostname": socket.gethostname() + "-OTHER-NODE",
        "slurm_job_id": job_id, "started": "2000-01-01T00:00:00+00:00"}))


def test_lock_records_slurm_job_id_when_env_set(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "778899")
    with DatasetLock(tmp_path / "ds") as lock:
        assert json.loads(lock.path.read_text())["slurm_job_id"] == "778899"


def test_lock_omits_slurm_job_id_when_env_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    with DatasetLock(tmp_path / "ds") as lock:
        assert "slurm_job_id" not in json.loads(lock.path.read_text())


def test_lock_reclaims_cross_host_when_slurm_job_ended(tmp_path, monkeypatch):
    # The wallclock-orphaned-lock case: a parse SIGKILLed on another node left the lock,
    # its owning batch job is gone, and the controller confirms it -> reclaim.
    monkeypatch.setattr(store_mod, "_slurm_job_active", lambda job: False)
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    d = tmp_path / "ds"
    _cross_host_lock(d)
    with DatasetLock(d):
        assert json.loads((d / ".parse.lock").read_text())["pid"] == os.getpid()
    assert not (d / ".parse.lock").exists()


def test_lock_refuses_cross_host_when_slurm_job_active(tmp_path, monkeypatch):
    monkeypatch.setattr(store_mod, "_slurm_job_active", lambda job: True)
    d = tmp_path / "ds"
    _cross_host_lock(d)
    with pytest.raises(DatasetLockError) as exc:
        DatasetLock(d).acquire()
    assert "424242" in str(exc.value) and "DIFFERENT host" in str(exc.value)
    assert (d / ".parse.lock").is_file()             # live lock left in place


def test_lock_refuses_cross_host_when_slurm_undeterminable(tmp_path, monkeypatch):
    # Uncertain (no squeue / transient error) must fail safe, never reclaim.
    monkeypatch.setattr(store_mod, "_slurm_job_active", lambda job: None)
    d = tmp_path / "ds"
    _cross_host_lock(d)
    with pytest.raises(DatasetLockError):
        DatasetLock(d).acquire()
    assert (d / ".parse.lock").is_file()


def test_dataset_lock_is_live_cross_host_consults_slurm(tmp_path, monkeypatch):
    d = tmp_path / "ds"
    _cross_host_lock(d)
    monkeypatch.setattr(store_mod, "_slurm_job_active", lambda job: True)
    assert dataset_lock_is_live(d) is not None        # job active -> live
    monkeypatch.setattr(store_mod, "_slurm_job_active", lambda job: False)
    assert dataset_lock_is_live(d) is None            # job ended -> stale


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

def test_stress_to_ase_voigt_matches_ase_convention():
    # parse._stress_to_ase_voigt must reproduce EXACTLY what ASE's own vasprun.xml
    # reader (ase.io.vasp.read_vasp_xml) does to the raw kBar <varray name="stress">:
    #   stress *= -0.1 * GPa ; stress = stress.reshape(9)[[0,4,8,5,2,1]]  (-> Voigt)
    # pymatgen returns that same raw kBar 3x3, so this is the correct REF_stress.
    import ase.units
    import numpy as np
    from zenodo_harvest.parse import _stress_to_ase_voigt
    kbar_3x3 = [[10.0, 1.0, 2.0], [1.0, -5.0, 3.0], [2.0, 3.0, 7.0]]  # symmetric, kBar
    got = _stress_to_ase_voigt(kbar_3x3)
    ase_ref = (np.array(kbar_3x3, dtype=float) * (-0.1 * ase.units.GPa)
               ).reshape(9)[[0, 4, 8, 5, 2, 1]]  # ASE read_vasp_xml formula, verbatim
    assert got.shape == (6,)
    assert np.allclose(got, ase_ref)
    # sanity: sign is flipped vs VASP and magnitude is ~1e-3 eV/A^3 per kBar
    assert got[0] < 0  # +10 kBar (VASP compressive) -> negative xx in ASE convention
    assert abs(got[0]) == pytest.approx(10 * 0.1 * ase.units.GPa)


def test_potcar_set_hash_deterministic_and_order_sensitive():
    from zenodo_harvest.parse import _potcar_set_hash
    a = _potcar_set_hash(["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"])
    assert a == _potcar_set_hash(["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"])  # stable
    assert a != _potcar_set_hash(["PAW_PBE O 08Apr2002", "PAW_PBE Fe_pv 06Sep2000"])  # order matters
    assert a != _potcar_set_hash(["PAW_PBE Fe 06Sep2000", "PAW_PBE O 08Apr2002"])     # variant matters
    assert _potcar_set_hash([]) is None
    assert _potcar_set_hash([None, ""]) is None  # blanks dropped -> None
    assert len(a) == 16


def test_outcar_potcar_titels_extracted_in_order(tmp_path):
    from zenodo_harvest.parse import _outcar_potcar_titels, _potcar_set_hash
    outcar = tmp_path / "OUTCAR"
    outcar.write_text(
        "  SOME HEADER\n"
        "   TITEL  = PAW_PBE Fe_pv 06Sep2000\n"
        "   LEXCH  = PE\n"
        "   TITEL  = PAW_PBE O 08Apr2002\n"
        "   TITEL  = PAW_PBE Fe_pv 06Sep2000\n"   # repeat -> de-duplicated
        "  ... rest of run ...\n"
    )
    titels = _outcar_potcar_titels(str(outcar))
    assert titels == ["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"]  # first-seen order, unique
    # and it feeds the same set-hash the vasprun path would compute from these titels
    assert _potcar_set_hash(titels) == _potcar_set_hash(
        ["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"])
    assert _outcar_potcar_titels(str(tmp_path / "nope")) == []  # missing file -> []


def test_shard_indexing_is_numeric_not_lexical(tmp_path):
    for idx in (2, 10):                              # lexical would sort "00010" < "00002"
        (tmp_path / f"shard-{idx:05d}.extxyz.gz").write_bytes(b"")
    assert [p.name for p in existing_shard_paths(tmp_path)] == [
        "shard-00002.extxyz.gz", "shard-00010.extxyz.gz"]
    assert next_shard_index(tmp_path) == 11          # one past the highest (10), not 3


def test_electronic_converged_none_is_omitted_not_written_as_true(tmp_path):
    # extxyz round-trip semantics: a bare key (no ``=value``) reads back as ``True``. So an
    # "unknown" (None) convergence verdict must be OMITTED from atoms.info, never written as
    # None — otherwise the frame reads back as converged=True (silently mislabelling an
    # unknown-convergence frame). True/False must survive unchanged. This is the store-layer
    # invariant parse.py relies on (see parse._frame / parse._parse_outcar_ase); the parse
    # side is covered by tests/test_parse_integration.py.
    import numpy as np
    from ase.io import read as ase_read

    from zenodo_harvest.store import ShardedExtxyzWriter
    ds = tmp_path / "ds"
    specs = [("known_true", True), ("known_false", False), ("unknown", None)]
    with ShardedExtxyzWriter(ds) as xyz:
        for fid, verdict in specs:
            a = Atoms("H", positions=[[0, 0, 0]], cell=[10, 10, 10], pbc=True)
            a.info["frame_id"] = fid
            a.info["REF_energy"] = -1.0
            a.arrays["REF_forces"] = np.zeros((1, 3))
            if verdict is not None:                 # mirror parse: write ONLY when known
                a.info["electronic_converged"] = verdict
            xyz.write(a)
        xyz.flush()
    back = {a.info["frame_id"]: a for a in
            ase_read(next(ds.glob("shard-*.extxyz.gz")), index=":", format="extxyz")}
    assert back["known_true"].info.get("electronic_converged") is True
    assert back["known_false"].info.get("electronic_converged") is False
    assert back["unknown"].info.get("electronic_converged") is None   # NOT True


# --------------------------------------------------------------------------- #
# Per-calc parse timeout (isolate a non-terminating pymatgen/ASE parse)        #
# --------------------------------------------------------------------------- #

# NB these run REAL child processes via the forkserver. Targets are builtins/stdlib so the
# forkserver never needs to import the test module; the timeout case uses 30 s vs a 1 s cap
# and must return in ~1 s (proving the child is killed, not waited on).

def test_run_with_timeout_returns_value_on_success():
    assert parse_mod._run_with_timeout(abs, (-7,), timeout=30) == ("ok", 7)


def test_run_with_timeout_kills_a_hanging_call():
    t0 = time.monotonic()
    status, val = parse_mod._run_with_timeout(time.sleep, (30,), timeout=1)
    assert (status, val) == ("timeout", None)
    assert time.monotonic() - t0 < 15          # killed, not waited out to 30 s


def test_run_with_timeout_reports_child_exception():
    status, msg = parse_mod._run_with_timeout(int, ("notanumber",), timeout=30)
    assert status == "exc" and "ValueError" in msg


def test_run_with_timeout_detects_a_dead_worker():
    # os._exit skips the finally that would send a result -> parent must see "died".
    assert parse_mod._run_with_timeout(os._exit, (0,), timeout=30) == ("died", None)


def test_parse_one_in_process_when_timeout_disabled(tmp_path, monkeypatch):
    seen = {}

    def fake_pcu(unit, base_meta, availability, rej, mpb):
        seen["args"] = (unit, mpb)
        return (["FRAME"], {"calc_id": "x"})

    monkeypatch.setattr(parse_mod, "parse_calc_unit", fake_pcu)
    # Any subprocess use here would be a bug: assert we never touch it.
    monkeypatch.setattr(parse_mod, "_run_with_timeout",
                        lambda *a, **k: pytest.fail("must stay in-process when timeout=0"))
    with RejectionLogger(tmp_path / "r.jsonl") as rej:
        out = parse_mod._parse_one({"outcar": "o"}, {}, {}, rej, 7, 0, "cid")
    assert out == (["FRAME"], {"calc_id": "x"}) and seen["args"] == ({"outcar": "o"}, 7)


def test_parse_one_timeout_logs_parse_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(parse_mod, "_run_with_timeout", lambda fn, args, t: ("timeout", None))
    with RejectionLogger(tmp_path / "r.jsonl") as rej:
        out = parse_mod._parse_one({}, {}, {}, rej, 0, 600, "zenodo:1:BS/vasprun.xml")
    assert out is None
    r = list(read_jsonl(tmp_path / "r.jsonl"))
    assert len(r) == 1 and r[0]["reason"] == "parse_timeout"
    assert r[0]["id"] == "zenodo:1:BS/vasprun.xml" and r[0]["timeout_s"] == 600


def test_parse_one_worker_died_logs_reason(tmp_path, monkeypatch):
    monkeypatch.setattr(parse_mod, "_run_with_timeout", lambda fn, args, t: ("died", None))
    with RejectionLogger(tmp_path / "r.jsonl") as rej:
        assert parse_mod._parse_one({}, {}, {}, rej, 0, 600, "cid") is None
    assert list(read_jsonl(tmp_path / "r.jsonl"))[0]["reason"] == "parse_worker_died"


def test_parse_one_replays_child_rejections_and_returns_result(tmp_path, monkeypatch):
    child_rej = [("parse", "zenodo:1:a/OUTCAR", "frames_no_energy", {"dropped": 2, "kept": 5})]
    monkeypatch.setattr(parse_mod, "_run_with_timeout",
                        lambda fn, args, t: ("ok", ((["FRAME"], {"calc_id": "y"}), child_rej)))
    with RejectionLogger(tmp_path / "r.jsonl") as rej:
        out = parse_mod._parse_one({}, {}, {}, rej, 0, 600, "cid")
    assert out == (["FRAME"], {"calc_id": "y"})
    r = list(read_jsonl(tmp_path / "r.jsonl"))[0]
    assert r["reason"] == "frames_no_energy" and r["dropped"] == 2 and r["kept"] == 5


# --------------------------------------------------------------------------- #
# Skip previously-rejected calcs on resume (don't re-run the parser)           #
# --------------------------------------------------------------------------- #

def test_rejected_calc_ids_selects_only_deterministic_parse_failures(tmp_path):
    rp = tmp_path / "rej.jsonl"
    with RejectionLogger(rp) as rej:
        rej.reject("parse", "zenodo:1:a/vasprun.xml", "vasprun_parse_error", detail="x")
        rej.reject("parse", "zenodo:1:b/OUTCAR", "outcar_parse_error", detail="x")
        rej.reject("parse", "zenodo:1:c/vaspout.h5", "vaspout_parse_error", detail="x")
        rej.reject("parse", "zenodo:1:d/OUTCAR", "no_frames")
        # retryable reasons must NOT be skipped:
        rej.reject("parse", "zenodo:1:e/vasprun.xml", "parse_timeout", timeout_s=600)
        rej.reject("parse", "zenodo:1:f/vasprun.xml", "primary_too_large")
        rej.reject("parse", "zenodo:1:g/OUTCAR", "parse_worker_died")
        rej.reject("parse", "zenodo:1:h/OUTCAR", "frames_no_energy", dropped=1, kept=2)
        rej.reject("fetch", "zenodo:1:i", "no_vasp_files_fetched")   # wrong stage
    assert parse_mod._rejected_calc_ids(rp) == {
        "zenodo:1:a/vasprun.xml", "zenodo:1:b/OUTCAR",
        "zenodo:1:c/vaspout.h5", "zenodo:1:d/OUTCAR"}


def test_rejected_calc_ids_empty_when_file_absent(tmp_path):
    assert parse_mod._rejected_calc_ids(tmp_path / "nope.jsonl") == set()


# --------------------------------------------------------------------------- #
# parse() feeds each calc its OWN per-calc availability (fix #1 wiring).        #
# --------------------------------------------------------------------------- #

def _write_manifest(path, rec):
    path.write_text(json.dumps(rec) + "\n")


def test_parse_uses_per_calc_availability(tmp_path, monkeypatch):
    # A record with two calc units in different dirs and a calc_availability list: parse must
    # hand EACH calc its own entry (indexed alignment), not the record-level union.
    seen = {}

    def fake_parse_one(unit, base_meta, availability, rej, mpb, timeout, calc_id):
        seen[calc_id] = availability
        return None  # return nothing -> no frames/metadata written (no VASP files needed)

    monkeypatch.setattr(parse_mod, "_parse_one", fake_parse_one)
    rec = {
        "recid": "R", "provenance": {"source": "zenodo", "record_id": "R"},
        "local_dir": "R",
        "calc_units": [
            {"dir": "R/extracted/c1", "vasprun": "R/extracted/c1/vasprun.xml"},
            {"dir": "R/extracted/c2", "vasprun": "R/extracted/c2/vasprun.xml"},
        ],
        "availability": {"charge_density": True, "dos": True},          # record union
        "calc_availability": [
            {"charge_density": True, "dos": False},                      # c1
            {"charge_density": False, "dos": True},                       # c2
        ],
    }
    man = tmp_path / "fetched.jsonl"
    _write_manifest(man, rec)
    parse_mod.parse(man, dataset_dir=tmp_path / "ds", rejections_path=tmp_path / "rej.jsonl",
                    raw_dir=tmp_path / "raw", parse_timeout_s=0)
    assert seen["zenodo:R:c1/vasprun.xml"] == {"charge_density": True, "dos": False}
    assert seen["zenodo:R:c2/vasprun.xml"] == {"charge_density": False, "dos": True}


def test_parse_falls_back_to_record_availability_without_calc_availability(tmp_path, monkeypatch):
    # Old manifests have no calc_availability -> every calc gets the record-level union (the
    # prior behaviour), so resuming a pre-fix harvest is byte-compatible.
    seen = {}

    def fake_parse_one(unit, bm, avail, rej, mpb, to, cid):
        seen[cid] = avail
        return None

    monkeypatch.setattr(parse_mod, "_parse_one", fake_parse_one)
    rec = {
        "recid": "R", "provenance": {"source": "zenodo", "record_id": "R"},
        "local_dir": "R",
        "calc_units": [{"dir": "R/extracted/c1", "vasprun": "R/extracted/c1/vasprun.xml"}],
        "availability": {"charge_density": True, "dos": False},
    }
    man = tmp_path / "fetched.jsonl"
    _write_manifest(man, rec)
    parse_mod.parse(man, dataset_dir=tmp_path / "ds", rejections_path=tmp_path / "rej.jsonl",
                    raw_dir=tmp_path / "raw", parse_timeout_s=0)
    assert seen["zenodo:R:c1/vasprun.xml"] == {"charge_density": True, "dos": False}
