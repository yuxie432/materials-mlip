"""Stage 4 — storage.

Two sinks, joined by ``calc_id`` / ``frame_id`` (see docs/DESIGN.md §3):

* :class:`ShardedExtxyzWriter` — atomistic data as gzipped extxyz, ~N frames per
  shard so files stay a manageable size and parallel writers each own their
  shards. Frame headers stay small: energy/forces/stress written directly under the
  ``REF_energy``/``REF_forces``/``REF_stress`` info/arrays keys (not via a
  ``SinglePointCalculator``), a few quality tags, and the ``frame_id``/``calc_id``
  join keys.
* :class:`MetadataWriter` — one JSONL record per calculation holding the bulky,
  frame-invariant data: provenance, citation, full calc parameters, convergence,
  and availability flags.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import zlib
from datetime import datetime, timezone
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
                 prefix: str = "shard", start_index: int = 0, compresslevel: int = 6):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.frames_per_shard = frames_per_shard
        self.prefix = prefix
        # gzip level 1..9 (1=fastest/largest, 9=slowest/smallest, 6=gzip default).
        # extxyz compresses well even at low levels; a lower level trades a little size
        # for much less CPU on a big cluster harvest. Only affects freshly-written
        # shards — merge moves opaque blobs and never recompresses.
        self.compresslevel = compresslevel
        self._shard_index = start_index
        self._in_shard = 0
        self._fh: Any = None
        self.total = 0

    def _shard_path(self, i: int) -> Path:
        return self.out_dir / f"{self.prefix}-{i:05d}.extxyz.gz"

    def _open_shard(self) -> None:
        if self._fh is not None:
            # fsync the shard we are rotating AWAY from before closing it. A calc's
            # frames can straddle a rotation, and flush() only fsyncs the *current*
            # (new) shard — so without this the filled shard's tail would still sit in
            # the OS page cache when the calc's metadata is committed, and a hard crash
            # (power/kernel, not a clean SIGTERM) could lose frames the metadata points
            # at (verify would then report them missing_on_disk). See flush().
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        self._fh = gzip.open(self._shard_path(self._shard_index), "at",
                             compresslevel=self.compresslevel)
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
#
# dataset_ops.merge_datasets rides on this same recovery machinery: it moves a
# source's shards into the destination (renamed) BEFORE appending that source's
# metadata records, so an interruption mid-source leaves at worst orphan frames
# (shards present, not yet referenced by any destination metadata record) at the
# TOP of the destination's shard sequence — never metadata pointing at absent
# frames. Those orphans are exactly what :func:`prune_uncommitted_frames` drops
# on the next parse resume (it walks high->low, deleting wholly-orphan top shards
# until it reaches an intact, fully-committed one), so merge needs no bespoke
# rollback. See dataset_ops for the source-completion marker that stops a re-run
# from double-appending an already-merged source.
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


# frame_id in an extxyz comment line is an ``info`` value; ASE quotes it because it
# contains ':'/'/'/'#' (e.g. frame_id="zenodo:123:.../vasprun.xml#0"). Fall back to an
# unquoted token just in case.
_FRAME_ID_RE = re.compile(r'frame_id=(?:"([^"]*)"|(\S+))')


def read_shard_frame_meta_lenient(path: Path) -> tuple[list[tuple[str | None, set[str]]], bool]:
    """Fast, low-overhead read of a shard's per-frame ``(frame_id, {symbols})`` WITHOUT
    building ``Atoms`` — the version :func:`~zenodo_harvest.dataset_ops.verify_dataset` needs.

    ``verify`` only wants each frame's ``frame_id`` (for the metadata↔shard bijection) and
    its element set (for coverage stats), yet :func:`read_shard_frames_lenient` constructs a
    full ``Atoms`` per frame via ``ase_read`` — ~1-3 ms each, i.e. HOURS for a 10M+-frame
    dataset (the cause of ``verify`` blowing its wallclock). Parsing the extxyz text directly
    is ~100x cheaper and allocates almost nothing per frame. Same lenient handling of a
    crash-truncated gzip / torn tail frame as :func:`read_shard_frames_lenient`.

    Assumes the ASE extxyz convention that the first column is the chemical species (which
    is how :class:`ShardedExtxyzWriter` writes them). Returns ``(metas, truncated)``.
    """
    truncated = False
    try:
        with gzip.open(path, "rt") as fh:
            text = fh.read()
    except (EOFError, OSError, zlib.error):
        text = _decompress_gzip_prefix(path)
        truncated = True

    lines = text.splitlines()
    n = len(lines)
    metas: list[tuple[str | None, set[str]]] = []
    i = 0
    while i < n:
        try:
            natoms = int(lines[i].strip())
        except (ValueError, IndexError):
            truncated = True
            break
        if i + 2 + natoms > n:  # last frame torn off mid-write
            truncated = True
            break
        m = _FRAME_ID_RE.search(lines[i + 1])
        frame_id = (m.group(1) if m and m.group(1) is not None else m.group(2)) if m else None
        symbols: set[str] = set()
        for j in range(i + 2, i + 2 + natoms):
            tok = lines[j].split(None, 1)
            if tok:
                symbols.add(tok[0])
        metas.append((frame_id, symbols))
        i += 2 + natoms
    return metas, truncated


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


# ---------------------------------------------------------------------------
# Dataset-dir lock (guards against two writers corrupting one dataset dir).
#
# Two parse tasks pointed at the SAME --dataset-dir interleave shard writes and
# metadata appends and corrupt both — the parallel model is one dataset dir PER
# array task, merged afterwards (see dataset_ops.merge_datasets). This advisory
# lockfile makes that mistake fail loudly rather than silently.
#
# Cross-node caveat: on an HPC cluster a lock written by another compute node
# names a pid THIS node cannot check (os.kill only sees local pids), so a
# different-hostname lock is NEVER assumed stale — we fail safe and make the user
# remove it by hand. Only a same-host lock whose pid is provably dead is
# reclaimed (a crashed prior run on this node).
# ---------------------------------------------------------------------------

LOCK_NAME = ".parse.lock"


class DatasetLockError(RuntimeError):
    """A dataset dir is already locked (or its lock is unsafe to reclaim)."""


def _pid_alive(pid: Any) -> bool:
    """Whether local process ``pid`` exists (signal 0 probes without delivering it)."""
    if not isinstance(pid, int) or pid <= 0:
        return False  # a malformed / missing pid can't be proven alive
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False  # no such process -> dead
    except PermissionError:
        return True   # exists but owned by another user -> alive
    except OSError:
        return True   # unknown error -> be conservative, treat as alive
    return True


# SLURM job states in which a lock's owning batch job is still live. Anything else — or
# the job being absent from the controller — means it has ended.
_SLURM_ACTIVE_STATES = frozenset({
    "RUNNING", "PENDING", "COMPLETING", "CONFIGURING", "RESIZING", "SUSPENDED",
    "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED", "SIGNALING", "STOPPED", "SPECIAL_EXIT",
})


def _slurm_job_active(job_id: Any) -> bool | None:
    """Whether SLURM job ``job_id`` is still active — ``None`` if undeterminable.

    This is what lets a *cross-host* lock be reclaimed safely: the SIGKILL-at-wallclock
    case leaves a lock the owner never released, and the resubmitted job almost always
    lands on a different node, where :func:`_pid_alive` cannot probe the owner. If the
    lock records a SLURM job id, the controller can answer across nodes. Returns:

    * ``True``  — an active/pending state (do NOT reclaim);
    * ``False`` — ``squeue`` ran and the job is gone / terminal (safe to reclaim);
    * ``None``  — undeterminable (no ``squeue`` binary, a transient error, a timeout, a
      malformed id) — the caller must then fail safe and treat the lock as live.

    Never raises: any failure collapses to ``None``, so off a SLURM cluster (no
    ``squeue``) lock handling degrades to the old pid-only conservative behaviour.
    """
    if not isinstance(job_id, (str, int)):
        return None
    jid = str(job_id).strip()
    if not re.fullmatch(r"\d+(?:_\d+)?", jid):  # numeric id, optionally an array task
        return None
    squeue = shutil.which("squeue")
    if squeue is None:
        return None
    try:
        proc = subprocess.run([squeue, "-h", "-j", jid, "-o", "%T"],
                              capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        # squeue answers "Invalid job id specified" once a job has left the controller —
        # i.e. it is not active. Any other non-zero exit is treated as undeterminable.
        return False if "invalid job id" in (proc.stderr or "").lower() else None
    states = proc.stdout.split()
    if not states:
        return False  # squeue ran cleanly and the job is not in the queue -> ended
    return any(s.upper() in _SLURM_ACTIVE_STATES for s in states)


def _lock_summary(info: dict) -> str:
    """One-line description of a lock record for log/error messages."""
    return (f"host {info.get('hostname')}, pid {info.get('pid')}, "
            f"slurm job {info.get('slurm_job_id', '-')}, started {info.get('started')}")


def _lock_is_stale(info: dict) -> bool:
    """Whether a present lock is provably abandoned and safe to reclaim.

    Reclaim only on PROOF the owner is gone. Same host: the pid is authoritative (a dead
    pid is stale — unchanged behaviour). Different host: pids can't be probed, so the lock
    is reclaimed only if it names a SLURM job the controller confirms has *ended*; a
    cross-host lock with no checkable job id (and an unreadable lock) is never reclaimed,
    failing safe exactly as before.
    """
    if not info:                                             # unreadable -> never reclaim
        return False
    if info.get("hostname") == socket.gethostname():
        return not _pid_alive(info.get("pid"))               # same host: pid is proof
    job = info.get("slurm_job_id")
    if job:
        return _slurm_job_active(job) is False               # cross host: SLURM is proof
    return False                                             # unprovable -> assume live


def _read_lock(path: Path) -> dict | None:
    """Parse a lock file: ``None`` if absent, ``{}`` sentinel if present-but-corrupt."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError:
        return {}  # present but unreadable -> caller fails safe (won't reclaim)
    try:
        info = json.loads(text)
    except ValueError:
        return {}
    return info if isinstance(info, dict) else {}


def _lock_age_seconds(info: dict) -> float | None:
    try:
        started = datetime.fromisoformat(info["started"])
        return (datetime.now(timezone.utc) - started).total_seconds()
    except (KeyError, ValueError, TypeError):
        return None


def dataset_lock_is_live(dataset_dir: str | Path) -> dict | None:
    """Return the lock record if ``dataset_dir`` is held by a LIVE lock, else ``None``.

    "Live" means anything not provably abandoned (see :func:`_lock_is_stale`): a same-host
    lock with a still-running pid; a different-host lock whose recorded SLURM job the
    controller reports still active (or that names no checkable job, since cross-node pids
    are unprobeable); or an unreadable lock (fail safe). Stale — a dead same-host pid, or a
    cross-host lock whose SLURM job has ended — is reported as ``None`` (not live). Used by
    merge-datasets to refuse a source dir a parse may still be writing.
    """
    info = _read_lock(Path(dataset_dir) / LOCK_NAME)
    if info is None:
        return None
    if not info:  # unreadable -> fail safe (treat as held)
        return {"_unreadable": True}
    return None if _lock_is_stale(info) else info


class DatasetLock:
    """Advisory lock on a dataset dir, released (unlinked) on context exit / error.

    Acquire creates ``<dataset_dir>/.parse.lock`` with ``O_CREAT|O_EXCL`` (atomic,
    so two racing acquirers can't both win). If it already exists it is reclaimed only
    when provably abandoned (:func:`_lock_is_stale`): a same-host dead pid, or a
    cross-host lock whose recorded SLURM job the controller reports as ended. Anything
    else — a same-host live pid, a cross-host lock still running (or with no checkable
    job id), or an unreadable lock — aborts with a clear message.

    The lock records ``slurm_job_id`` (from ``$SLURM_JOB_ID``) when set, which is what
    makes a cross-node stale lock reclaimable: a parse SIGKILLed at wallclock cannot
    release its lock, and the resubmitted job usually lands on a different node where the
    owner pid is unprobeable — but the controller can still confirm the old job has ended.
    """

    def __init__(self, dataset_dir: str | Path):
        self.dataset_dir = Path(dataset_dir)
        self.path = self.dataset_dir / LOCK_NAME
        self._held = False

    def _payload(self) -> bytes:
        rec: dict[str, Any] = {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started": datetime.now(timezone.utc).isoformat(),
        }
        job = os.environ.get("SLURM_JOB_ID")
        if job:
            rec["slurm_job_id"] = job
        return json.dumps(rec).encode()

    def acquire(self) -> "DatasetLock":
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        for attempt in range(2):  # attempt 0 may reclaim a stale lock, attempt 1 retries
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                info = _read_lock(self.path)
                if info is None:
                    continue  # vanished between open and read -> retry the create
                if attempt == 0 and _lock_is_stale(info):
                    logger.warning("reclaiming stale lock %s (%s)",
                                   self.path, _lock_summary(info))
                    self.path.unlink(missing_ok=True)
                    continue  # retry the create now the stale lock is gone
                raise DatasetLockError(self._refusal(info))
            else:
                os.write(fd, self._payload())
                os.close(fd)
                self._held = True
                return self
        raise DatasetLockError(
            f"could not acquire {self.path}: another writer keeps re-creating it")

    def _refusal(self, info: dict) -> str:
        if not info:
            return (f"{self.dataset_dir} holds an unreadable lock file {self.path}; "
                    f"refusing to write. Remove it manually if no parse is running there.")
        host, pid, started = info.get("hostname"), info.get("pid"), info.get("started")
        job = info.get("slurm_job_id")
        age = _lock_age_seconds(info)
        age_s = f", age {age:.0f}s" if age is not None else ""
        if host == socket.gethostname():
            return (f"{self.dataset_dir} is locked by a running parse (pid {pid} on {host}, "
                    f"started {started}{age_s}). Wait for it to finish or kill that process.")
        jobinfo = f", SLURM job {job}" if job else ""
        if job:
            hint = (f"SLURM job {job} still appears to be running (or could not be "
                    f"queried); if you are certain it is not, ")
        else:
            hint = ("Cross-node liveness cannot be checked; if you are certain no parse "
                    "runs there, ")
        return (f"{self.dataset_dir} holds a lock from a DIFFERENT host ({host}, pid {pid}"
                f"{jobinfo}, started {started}{age_s}). {hint}remove {self.path} manually.")

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False

    def __enter__(self) -> "DatasetLock":
        return self.acquire()

    def __exit__(self, *exc: Any) -> None:
        self.release()
