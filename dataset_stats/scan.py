"""Shard scan: one pass over every ``shard-*.extxyz.gz`` of a dataset -> a compact per-frame table,
per-calc structure descriptors and mergeable per-shard aggregates.

Output per shard: ``<out>/shard-NNNNN.npz`` with
  * ``frames`` — one :data:`FRAME_DTYPE` row per frame (energy, force/stress summaries, volume,
    net moment/charge, SCF tag, structure + frame hashes), joinable to the metadata by ``calc``
    (:func:`dataset_stats.common.calc_key` of the calc_id);
  * ``calcs``  — UTF-8 JSON lines, one per calc present in the shard: the descriptors of its first
    and last frame here (:func:`structure.describe`) and its frame count here (a calc whose frames
    straddle a shard boundary appears in both shards; the report keeps the lowest/highest step);
  * ``agg``    — UTF-8 JSON: the per-atom |F| histogram (per-atom forces are not kept), the
    ``Properties`` layouts and comment keys seen, pbc patterns, torn/bad frame counts.

Each shard is written atomically and skipped on a re-run, so a killed job resumes. Shards are
independent, so ``workers`` scans several at once (processes: the scan is CPU-bound — gzip, text
parsing, spglib).
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import struct
import time
from collections import Counter
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np

from zenodo_harvest.store import existing_shard_paths

from . import structure
from .common import EV_A3_TO_GPA, FORCE_EDGES, array_text, atomic_write_npz, calc_key, \
    jsonl_text, text_array
from .extxyz_fast import (
    frame_index,
    parse_comment,
    parse_properties,
    read_shard_text,
    to_flag,
    to_float,
    to_floats,
)

logger = logging.getLogger(__name__)

FRAME_DTYPE = np.dtype([
    ("calc", "u8"),       # calc_key(calc_id)
    ("step", "i4"),       # ionic-step index (the frame_id suffix)
    ("natoms", "i4"),
    ("energy", "f8"),     # REF_energy, eV (sigma->0)
    ("dfree", "f4"),      # E_free - REF_energy, eV (NaN without E_free)
    ("fmax", "f4"),       # max per-atom |F|, eV/A
    ("fmean", "f4"),      # mean per-atom |F|
    ("frms", "f4"),       # sqrt(mean |F|^2)
    ("fnet", "f4"),       # |sum_i F_i| (drift; a projected/constrained force does not sum to 0)
    ("pressure", "f4"),   # -(s_xx+s_yy+s_zz)/3, GPa (ASE sign: + = compressed cell)
    ("smax", "f4"),       # max |stress component|, GPa
    ("volume", "f4"),     # A^3
    ("mag", "f4"),        # total_magnetization, mu_B (NaN if absent)
    ("charge", "f4"),     # total_charge, e (NaN if absent)
    ("econv", "i1"),      # electronic_converged: 1 / 0 / -1 unknown
    ("scf_dE", "f4"),     # this step's SCF |dE| (NaN if absent)
    ("flags", "u2"),
    ("shash", "u8"),      # structure hash: species + cell + positions rounded to 1e-4 A
    ("fhash", "u8"),      # frame hash: structure hash + energy rounded to 1e-6 eV
])

F_FORCES = 1
F_STRESS = 2
F_EFREE = 4
F_E_NONFINITE = 8
F_F_NONFINITE = 16
F_S_NONFINITE = 32
F_PBC_PARTIAL = 64
F_POS_NONFINITE = 128
F_NO_CELL = 256
F_BAD = 512           # numeric block unreadable: row kept (frame counts stay exact), numbers NaN

# Upper bound on atom lines handed to one loadtxt call (bounds memory on huge shards).
MAX_RUN_LINES = 2_000_000
_VOIGT_FROM_3X3 = (0, 4, 8, 5, 2, 1)
_FLOAT_COLS = ("energy", "dfree", "fmax", "fmean", "frms", "fnet", "pressure", "smax", "volume",
               "mag", "charge", "scf_dE")


def _hash8(*parts: bytes) -> int:
    h = hashlib.blake2b(digest_size=8)
    for p in parts:
        h.update(p)
    return int.from_bytes(h.digest(), "little")


@dataclass(frozen=True)
class KeyMap:
    """Which extxyz keys hold what. The defaults are the harvest's own shards; a reference
    dataset in extxyz (e.g. Matbench Discovery's MPtrj zip) maps its keys onto the same rows.
    ``calc_fmt`` builds the trajectory id from info keys (default: ``calc_id`` / the frame_id
    prefix); ``group_key`` names a material/deposit id kept on the calc record (references)."""
    energy: str = "REF_energy"
    forces: str = "REF_forces"
    stress: str = "REF_stress"
    free: str = "E_free"
    mag: str = "total_magnetization"
    charge: str = "total_charge"
    calc_fmt: str | None = None
    step_key: str = "ionic_step"
    group_key: str | None = None


HARVEST_KEYS = KeyMap()
# Matbench Discovery's 2024-09-03 MPtrj extxyz: energy = uncorrected VASP energy, stress 3x3.
MPTRJ_KEYS = KeyMap(energy="energy", forces="forces", stress="stress", free="", mag="",
                    charge="", calc_fmt="{task_id}-{calc_id}", group_key="material_id")


def _stress_voigt(info: dict[str, str], key: str = "REF_stress") -> list[float] | None:
    s = to_floats(info.get(key))
    if s is None:
        return None
    if len(s) == 9:
        return [s[i] for i in _VOIGT_FROM_3X3]
    return s if len(s) == 6 else None


def _calc_and_step(info: dict[str, str], km: KeyMap = HARVEST_KEYS) -> tuple[str, int]:
    fid = info.get("frame_id") or ""
    if km.calc_fmt:
        try:
            cid = km.calc_fmt.format(**info)
        except KeyError:
            cid = fid
    else:
        cid = info.get("calc_id") or (fid.rsplit("#", 1)[0] if "#" in fid else fid)
    step_s = info.get(km.step_key)
    try:
        step = int(step_s) if step_s is not None else int(fid.rsplit("#", 1)[1])
    except (ValueError, IndexError):
        step = -1
    return cid, step


def _runs(props: list[str], natoms: list[int]) -> list[tuple[int, int]]:
    """Consecutive frame ranges sharing one Properties layout, each <= MAX_RUN_LINES atoms."""
    runs: list[tuple[int, int]] = []
    start, lines = 0, 0
    for i in range(len(props)):
        if i > start and (props[i] != props[start] or lines + natoms[i] > MAX_RUN_LINES):
            runs.append((start, i))
            start, lines = i, 0
        lines += natoms[i]
    if props:
        runs.append((start, len(props)))
    return runs


def _load_block(lines: list[str], usecols: tuple[int, ...]) -> np.ndarray:
    return np.loadtxt(lines, usecols=usecols, comments=None, ndmin=2, dtype=np.float64)


def scan_shard(shard: str | Path, *, describe: bool = True, keys: KeyMap = HARVEST_KEYS
               ) -> tuple[np.ndarray, list[dict], dict]:
    """Scan one shard -> (frames table, per-calc records, aggregates). Writes nothing."""
    shard = Path(shard)
    text, truncated = read_shard_text(shard)
    return scan_text(text, name=shard.name, truncated=truncated, describe=describe, keys=keys)


def scan_text(text: str, *, name: str, truncated: bool = False, describe: bool = True,
              keys: KeyMap = HARVEST_KEYS) -> tuple[np.ndarray, list[dict], dict]:
    """:func:`scan_shard` on already-decompressed extxyz text (shards, or zip members)."""
    t0 = time.time()
    lines = text.split("\n")
    del text
    idx, torn = frame_index(lines)
    nfr = len(idx)
    fcol = {name: np.full(nfr, np.nan, dtype=FRAME_DTYPE[name]) for name in _FLOAT_COLS}
    c_calc = np.zeros(nfr, dtype="u8")
    c_step = np.zeros(nfr, dtype="i4")
    c_nat = np.zeros(nfr, dtype="i4")
    c_econv = np.full(nfr, -1, dtype="i1")
    c_flags = np.zeros(nfr, dtype="u2")
    c_shash = np.zeros(nfr, dtype="u8")
    c_fhash = np.zeros(nfr, dtype="u8")

    props_seen: Counter = Counter()
    keys_seen: Counter = Counter()
    pbc_seen: Counter = Counter()
    bad_frames = fallback_runs = force_nonfinite_atoms = 0
    force_hist = np.zeros(len(FORCE_EDGES) - 1, dtype=np.int64)

    props: list[str] = []
    natoms_l: list[int] = []
    cells: list[np.ndarray | None] = []
    calc_ids: list[str] = []
    key_of: dict[str, int] = {}
    group_of: dict[str, str] = {}
    for fi, (hdr, nat) in enumerate(idx):
        info = parse_comment(lines[hdr + 1])
        keys_seen.update(info.keys())
        cid, step = _calc_and_step(info, keys)
        if keys.group_key and cid not in group_of:
            group_of[cid] = info.get(keys.group_key, "")
        calc_ids.append(cid)
        k = key_of.get(cid)
        if k is None:
            k = key_of[cid] = calc_key(cid)
        c_calc[fi] = k
        c_step[fi] = step
        c_nat[fi] = nat
        flags = 0
        e = to_float(info.get(keys.energy))
        fcol["energy"][fi] = e
        if not math.isfinite(e):
            flags |= F_E_NONFINITE
        if keys.free and keys.free in info:
            flags |= F_EFREE
            fcol["dfree"][fi] = to_float(info.get(keys.free)) - e
        s = _stress_voigt(info, keys.stress)
        if s is not None:
            flags |= F_STRESS
            if all(math.isfinite(x) for x in s):
                fcol["pressure"][fi] = -(s[0] + s[1] + s[2]) / 3.0 * EV_A3_TO_GPA
                fcol["smax"][fi] = max(abs(x) for x in s) * EV_A3_TO_GPA
            else:
                flags |= F_S_NONFINITE
        fcol["mag"][fi] = to_float(info.get(keys.mag)) if keys.mag else math.nan
        fcol["charge"][fi] = to_float(info.get(keys.charge)) if keys.charge else math.nan
        fcol["scf_dE"][fi] = to_float(info.get("scf_dE"))
        c_econv[fi] = to_flag(info.get("electronic_converged"))
        pbc = info.get("pbc", "T T T")
        pbc_seen[pbc] += 1
        if any(to_flag(t) != 1 for t in pbc.split()):
            flags |= F_PBC_PARTIAL
        lat = to_floats(info.get("Lattice"))
        cell = np.array(lat, dtype=float).reshape(3, 3) if lat and len(lat) == 9 else None
        geo = structure.cell_geometry(cell) if cell is not None else None
        if geo is None:
            flags |= F_NO_CELL
            cells.append(None)
        else:
            cells.append(cell)
            fcol["volume"][fi] = geo[0]
        c_flags[fi] = flags
        p = info.get("Properties", "")
        props.append(p)
        props_seen[p] += 1
        natoms_l.append(nat)

    # first/last frame of each calc in this shard (a calc's frames are written contiguously)
    first: dict[str, int] = {}
    last: dict[str, int] = {}
    for fi, cid in enumerate(calc_ids):
        first.setdefault(cid, fi)
        last[cid] = fi
    wanted = set(first.values()) | set(last.values())
    species_of: dict[str, tuple[list[str], bytes]] = {}
    descr: dict[int, dict] = {}

    for a, b in _runs(props, natoms_l):
        cols, _ncols = parse_properties(props[a])
        if "pos" not in cols:
            c_flags[a:b] |= F_BAD
            bad_frames += b - a
            continue
        usecols = tuple(range(cols["pos"][0], cols["pos"][0] + 3))
        has_f = keys.forces in cols and cols[keys.forces][1] == 3
        if has_f:
            fc = cols[keys.forces][0]
            usecols += (fc, fc + 1, fc + 2)
        block: list[str] = []
        for fi in range(a, b):
            hdr, nat = idx[fi]
            block.extend(lines[hdr + 2: hdr + 2 + nat])
        per_frame: list[np.ndarray | None] = []
        try:
            data = _load_block(block, usecols)
            off = 0
            for fi in range(a, b):
                nat = idx[fi][1]
                per_frame.append(data[off: off + nat])
                off += nat
        except ValueError:  # one bad line spoils the batch: retry frame by frame
            fallback_runs += 1
            for fi in range(a, b):
                hdr, nat = idx[fi]
                try:
                    per_frame.append(_load_block(lines[hdr + 2: hdr + 2 + nat], usecols))
                except ValueError:
                    per_frame.append(None)
        del block
        for fi, arr in zip(range(a, b), per_frame):
            hdr, nat = idx[fi]
            if arr is None or arr.shape[0] != nat:
                c_flags[fi] |= F_BAD
                bad_frames += 1
                continue
            pos = arr[:, 0:3]
            cid = calc_ids[fi]
            sp = species_of.get(cid)
            if sp is None:
                species = [lines[hdr + 2 + t].split(None, 1)[0] for t in range(nat)]
                sp = (species, ",".join(species).encode())
                species_of[cid] = sp
            if not np.isfinite(pos).all():
                c_flags[fi] |= F_POS_NONFINITE
            if has_f and nat:
                fvec = arr[:, 3:6]
                fn = np.sqrt(np.einsum("ij,ij->i", fvec, fvec))
                c_flags[fi] |= F_FORCES
                ok = np.isfinite(fn)
                if not ok.all():
                    c_flags[fi] |= F_F_NONFINITE
                    force_nonfinite_atoms += int((~ok).sum())
                fin = fn[ok]
                if len(fin):
                    fcol["fmax"][fi] = fin.max()
                    fcol["fmean"][fi] = fin.mean()
                    fcol["frms"][fi] = np.sqrt(np.mean(fin * fin))
                    fcol["fnet"][fi] = np.linalg.norm(fvec[ok].sum(axis=0))
                    force_hist += np.histogram(fin, FORCE_EDGES)[0]
            cell = cells[fi]
            cell_b = np.rint(cell * 1e4).astype(np.int64).tobytes() if cell is not None else b"-"
            pos_b = np.rint(np.nan_to_num(pos) * 1e4).astype(np.int64).tobytes()
            sh = _hash8(sp[1], cell_b, pos_b)
            c_shash[fi] = sh
            e = float(fcol["energy"][fi])
            e_b = struct.pack("<q", int(round(e * 1e6))) if math.isfinite(e) else b"nan"
            c_fhash[fi] = _hash8(sh.to_bytes(8, "little"), e_b)
            if describe and fi in wanted and cell is not None:
                d = structure.describe(cell, pos, sp[0])
                d["step"] = int(c_step[fi])
                d["shash"] = str(sh)
                descr[fi] = d

    rows = np.zeros(nfr, dtype=FRAME_DTYPE)
    rows["calc"], rows["step"], rows["natoms"] = c_calc, c_step, c_nat
    rows["econv"], rows["flags"], rows["shash"], rows["fhash"] = c_econv, c_flags, c_shash, c_fhash
    for name in _FLOAT_COLS:
        rows[name] = fcol[name]

    per_calc = Counter(calc_ids)
    calcs: list[dict[str, Any]] = [
        {"k": str(key_of[cid]), "id": cid, "n": per_calc[cid],
         "first": descr.get(f0), "last": descr.get(last[cid]) if last[cid] != f0 else None}
        for cid, f0 in first.items()]
    if keys.group_key:
        for c in calcs:
            c["g"] = group_of.get(str(c["id"]), "")
    agg: dict[str, Any] = {
        "shard": name, "frames": nfr, "atoms": int(sum(natoms_l)),
        "truncated": truncated, "torn_tail": torn, "bad_frames": bad_frames,
        "fallback_runs": fallback_runs, "force_nonfinite_atoms": force_nonfinite_atoms,
        "force_hist": force_hist.tolist(), "props": dict(props_seen),
        "info_keys": dict(keys_seen), "pbc": dict(pbc_seen),
        "elapsed_s": round(time.time() - t0, 3)}
    return rows, calcs, agg


def output_path(out_dir: Path, shard: Path) -> Path:
    return out_dir / (shard.name.split(".", 1)[0] + ".npz")


def _scan_task(args: tuple[str, str, bool]) -> dict:
    shard_s, out_s, describe = args
    shard, out = Path(shard_s), Path(out_s)
    try:
        rows, calcs, agg = scan_shard(shard, describe=describe)
        atomic_write_npz(out, frames=rows, calcs=text_array(jsonl_text(calcs)),
                         agg=text_array(json.dumps(agg)))
    except Exception as exc:  # noqa: BLE001 - report the shard, keep the pool alive
        logger.exception("scan failed for %s", shard)
        return {"shard": shard.name, "ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"shard": shard.name, "ok": True, "frames": agg["frames"], "atoms": agg["atoms"],
            "elapsed_s": agg["elapsed_s"], "bad_frames": agg["bad_frames"]}


def load_scan(path: str | Path) -> tuple[np.ndarray, list[dict], dict]:
    """Read one shard's scan output back: (frames, calc records, aggregates)."""
    with np.load(path, allow_pickle=False) as z:
        rows = z["frames"]
        calcs_txt = array_text(z["calcs"])
        agg = json.loads(array_text(z["agg"]))
    calcs = [json.loads(line) for line in io.StringIO(calcs_txt) if line.strip()]
    return rows, calcs, agg


def scan_outputs(out_dir: str | Path) -> list[Path]:
    return sorted(Path(out_dir).glob("shard-*.npz"))


def scan_dataset(dataset_dir: str | Path, out_dir: str | Path, *, workers: int = 1,
                 limit: int | None = None, force: bool = False, describe: bool = True) -> dict:
    """Scan every shard of ``dataset_dir`` into ``out_dir`` (resumable; largest shards first, so
    the long ones do not finish last on a single worker)."""
    dataset_dir, out_dir = Path(dataset_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shards = existing_shard_paths(dataset_dir)
    todo = [s for s in shards if force or not output_path(out_dir, s).is_file()]
    todo.sort(key=lambda p: p.stat().st_size, reverse=True)
    if limit is not None:
        todo = todo[:limit]
    logger.info("scan %s: %d shards, %d to do, %d workers", dataset_dir, len(shards), len(todo),
                workers)
    t0 = last_log = time.time()
    done = frames = 0
    errors: list[dict] = []
    tasks = [(str(s), str(output_path(out_dir, s)), describe) for s in todo]

    def _record(res: dict) -> None:
        nonlocal done, frames, last_log
        done += 1
        if res.get("ok"):
            frames += int(res.get("frames", 0))
        else:
            errors.append(res)
        now = time.time()
        if now - last_log >= 60 or done == len(tasks):
            logger.info("scanned %d/%d shards, %d frames (%.0f frames/s), %d failed", done,
                        len(tasks), frames, frames / max(now - t0, 1e-9), len(errors))
            last_log = now

    if workers <= 1:
        for task in tasks:
            _record(_scan_task(task))
    else:
        with get_context("fork").Pool(workers, maxtasksperchild=200) as pool:
            for res in pool.imap_unordered(_scan_task, tasks, chunksize=1):
                _record(res)
    summary = {"dataset_dir": str(dataset_dir), "out_dir": str(out_dir),
               "shards_total": len(shards), "shards_done_now": done - len(errors),
               "shards_failed": len(errors), "errors": errors[:50], "frames_scanned_now": frames,
               "complete": all(output_path(out_dir, s).is_file() for s in shards),
               "elapsed_s": round(time.time() - t0, 1)}
    with open(out_dir / "scan_summary.json", "w") as fh:
        json.dump(summary, fh, indent=1)
    return summary
