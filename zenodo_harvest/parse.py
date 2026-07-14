"""Stage 3 — parse.

Turn each fetched calculation into (a) per-ionic-step extxyz frames and (b) one
JSONL metadata record. pymatgen's ``Vasprun`` is the primary parser (mentor's
call); ``vaspout.h5`` (VASP's newer HDF5 output) is read via pymatgen's
``Vaspout`` — a ``Vasprun`` subclass with the same API — and ``OUTCAR``-only
calculations fall back to ASE's trajectory reader.

Per CLAUDE.md, every calc records electronic convergence *and its magnitude*
(ΔE between the last two SCF steps of the final ionic step), the run-type/
functional, the full INCAR + k-points + POTCAR spec, and availability flags for
heavy data we deliberately don't store (charge/spin density, eigenvalues, DOS).

What lands where (docs/DESIGN.md §3):
* extxyz frame  : positions, symbols, cell, energy, forces (+ site charges/magmoms
                  on the final frame if an OUTCAR provides them), small quality tags.
* metadata JSONL: provenance, citation, calc parameters, convergence, availability.

Stress is parsed but kept in metadata only (VASP reports kBar with a sign/scale
convention that must be confirmed before feeding stress to training).
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from ase import Atoms
from ase.calculators.singlepoint import SinglePointCalculator

from . import config
from .manifest import RejectionLogger, read_jsonl
from .store import (
    MetadataWriter,
    ShardedExtxyzWriter,
    next_shard_index,
    prune_uncommitted_frames,
)

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", module="pymatgen")


def _jsonable(obj: Any) -> Any:
    """Coerce numpy / pymatgen values into JSON-serialisable Python."""
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return _jsonable(obj.tolist())
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)  # enums, Kpoints style, etc.


def _scf_convergence(vasprun: Any) -> dict:
    """Electronic convergence bool + magnitude (ΔE of last two SCF steps)."""
    dE = None
    try:
        esteps = vasprun.ionic_steps[-1]["electronic_steps"]
        if len(esteps) >= 2:
            dE = abs(esteps[-1]["e_0_energy"] - esteps[-2]["e_0_energy"])
    except (IndexError, KeyError, TypeError):
        pass
    return {
        "electronic_converged": bool(vasprun.converged_electronic),
        "scf_dE": dE,
        "scf_dE_key": "e_0_energy",
        "ionic_converged": bool(vasprun.converged_ionic),
    }


def _calc_parameters(vasprun: Any) -> dict:
    incar = _jsonable(dict(vasprun.incar))
    params = vasprun.parameters
    kp = vasprun.kpoints
    kpoints = {
        "style": str(getattr(kp, "style", None)),
        "kpts": _jsonable(getattr(kp, "kpts", None)),
        "num_kpts": getattr(kp, "num_kpts", None),
        "kpts_shift": _jsonable(getattr(kp, "kpts_shift", None)),
    }
    ldau = None
    if incar.get("LDAU"):
        ldau = {k: incar.get(k) for k in ("LDAUTYPE", "LDAUL", "LDAUU", "LDAUJ") if k in incar}

    def _param(key: str) -> Any:
        # pymatgen's parameters dict occasionally reports ENCUT etc. as None even
        # when set; the INCAR is the authoritative user setting, so prefer it.
        v = incar.get(key)
        return v if v is not None else params.get(key)

    return {
        "code": "vasp",
        "code_version": getattr(vasprun, "vasp_version", None),
        "run_type": str(vasprun.run_type),          # functional + U/vdW flavour
        "functional": str(vasprun.run_type),
        "hubbard_u": ldau,
        "spin_polarized": bool(vasprun.is_spin),
        "encut": _param("ENCUT"),
        "ediff": _param("EDIFF"),
        "ismear": _param("ISMEAR"),
        "sigma": _param("SIGMA"),
        "ispin": _param("ISPIN"),
        "kpoints": kpoints,
        "potcar_symbols": _jsonable(vasprun.potcar_symbols),
        "potcar_spec": [{"titel": s.get("titel"), "hash": s.get("hash")}
                        for s in (vasprun.potcar_spec or [])],
        "incar": incar,
    }


def _site_props_from_outcar(outcar_path: str, natoms: int) -> dict:
    """Per-atom total charge / magnetic moment from an OUTCAR (end-of-run only)."""
    out: dict = {"magmoms": None, "charges": None}
    try:
        from pymatgen.io.vasp.outputs import Outcar
        oc = Outcar(outcar_path)
        if oc.magnetization and len(oc.magnetization) == natoms:
            out["magmoms"] = [m.get("tot") for m in oc.magnetization]
        if oc.charge and len(oc.charge) == natoms:
            out["charges"] = [c.get("tot") for c in oc.charge]
    except Exception as exc:  # OUTCAR parsing is fragile; availability still recorded
        logger.debug("OUTCAR site-props failed for %s: %s", outcar_path, exc)
    return out


def _frame(structure: Any, energy: float | None, forces: Any, *, calc_id: str,
           frame_id: str, ionic_step: int, conv: dict,
           magmoms: list | None = None, charges: list | None = None) -> Atoms:
    from pymatgen.io.ase import AseAtomsAdaptor
    atoms = AseAtomsAdaptor.get_atoms(structure)
    results: dict[str, Any] = {}
    if energy is not None:
        results["energy"] = float(energy)
    if forces is not None:
        results["forces"] = np.asarray(forces, dtype=float)
    if results:
        atoms.calc = SinglePointCalculator(atoms, **results)
    if magmoms is not None:
        atoms.set_initial_magnetic_moments(magmoms)
    if charges is not None:
        atoms.set_initial_charges(charges)
    atoms.info.update({
        "source": "zenodo",
        "calc_id": calc_id,
        "frame_id": frame_id,
        "ionic_step": ionic_step,
        "electronic_converged": conv["electronic_converged"],
    })
    if conv["scf_dE"] is not None:
        atoms.info["scf_dE"] = conv["scf_dE"]
    return atoms


def _frames_and_meta(v: Any, calc_id: str, outcar_path: str | None,
                     parser: str) -> tuple[list[Atoms], dict]:
    """Build frames + metadata from a parsed pymatgen object.

    Shared by :func:`parse_vasprun` and :func:`parse_vaspout`: ``Vaspout``
    subclasses ``Vasprun`` and exposes the same API, so only the constructor and
    the recorded ``parser`` tag differ.
    """
    conv = _scf_convergence(v)
    steps = v.ionic_steps
    natoms = len(v.final_structure)

    # Per-atom charges/spins (final geometry only) if an OUTCAR is alongside.
    site = {"magmoms": None, "charges": None}
    if outcar_path:
        site = _site_props_from_outcar(outcar_path, natoms)

    frames: list[Atoms] = []
    for i, st in enumerate(steps):
        last = i == len(steps) - 1
        frame = _frame(
            st["structure"], st.get("e_0_energy"), st.get("forces"),
            calc_id=calc_id, frame_id=f"{calc_id}#{i}", ionic_step=i, conv=conv,
            magmoms=site["magmoms"] if last else None,
            charges=site["charges"] if last else None,
        )
        # keep per-frame stress in the frame info (kBar; metadata notes the units).
        # Vasprun exposes it as "stress"; Vaspout as "stresses".
        stress = st.get("stress")
        if stress is None:
            stress = st.get("stresses")
        if stress is not None:
            frame.info["stress_kbar"] = _jsonable(stress)
        frames.append(frame)

    meta = {
        "calc_id": calc_id,
        "calc_parameters": _calc_parameters(v),
        "quality": {**conv, "n_ionic_steps": len(steps), "n_atoms": natoms,
                    "n_frames": len(frames)},
        "parser": parser,
        "stress_units": "kBar",
        "site_charges_present": site["charges"] is not None,
        "site_magmoms_present": site["magmoms"] is not None,
    }
    return frames, meta


def parse_vasprun(vasprun_path: str, calc_id: str, outcar_path: str | None) -> tuple[list[Atoms], dict]:
    """Parse one vasprun.xml into (frames, calc_metadata)."""
    from pymatgen.io.vasp.outputs import Vasprun
    v = Vasprun(vasprun_path, parse_dos=False, parse_eigen=False,
                parse_projected_eigen=False, parse_potcar_file=False,
                exception_on_bad_xml=False)
    return _frames_and_meta(v, calc_id, outcar_path, "pymatgen.Vasprun")


def parse_vaspout(vaspout_path: str, calc_id: str, outcar_path: str | None) -> tuple[list[Atoms], dict]:
    """Parse one vaspout.h5 (VASP's HDF5 output) into (frames, calc_metadata).

    Uses pymatgen's ``Vaspout`` (a ``Vasprun`` subclass). DOS/eigenvalues and
    POTCAR *contents* are skipped for speed and for storage parity with the
    vasprun.xml path (POTCAR titels/spec are still recorded via _calc_parameters).
    """
    from pymatgen.io.vasp.outputs import Vaspout
    v = Vaspout(vaspout_path, parse_dos=False, parse_eigen=False,
                parse_projected_eigen=False, store_potcar=False)
    return _frames_and_meta(v, calc_id, outcar_path, "pymatgen.Vaspout")


def _calc_id(unit: dict, base_meta: dict) -> str:
    """Stable id for a calc unit: ``zenodo:<recid>:<path-of-primary-file>``.

    Keyed on the primary file (vasprun.xml, else vaspout.h5, else OUTCAR) — the
    same precedence :func:`parse_calc_unit` parses with — so the id is identical
    whether we are parsing the unit or checking whether a prior run already did.
    """
    recid = base_meta["provenance"]["record_id"]
    root = Path(base_meta["_extracted_root"])
    primary = unit.get("vasprun") or unit.get("vaspout") or unit["outcar"]
    return f"zenodo:{recid}:{Path(primary).relative_to(root)}"


def parse_calc_unit(unit: dict, base_meta: dict, availability: dict,
                    rej: RejectionLogger) -> tuple[list[Atoms], dict] | None:
    """Parse one calc unit (a dir with vasprun.xml, vaspout.h5, and/or OUTCAR)."""
    vasprun = unit.get("vasprun")
    vaspout = unit.get("vaspout")
    outcar = unit.get("outcar")
    extracted_root = Path(base_meta["_extracted_root"])
    rel = (str(Path(unit["dir"]).relative_to(extracted_root))
           if (vasprun or vaspout or outcar) else unit["dir"])
    calc_id = _calc_id(unit, base_meta)

    if vasprun:
        try:
            frames, meta = parse_vasprun(vasprun, calc_id, outcar)
        except Exception as exc:
            rej.reject("parse", calc_id, "vasprun_parse_error", detail=f"{type(exc).__name__}: {exc}")
            return None
    elif vaspout:
        try:
            frames, meta = parse_vaspout(vaspout, calc_id, outcar)
        except Exception as exc:
            rej.reject("parse", calc_id, "vaspout_parse_error", detail=f"{type(exc).__name__}: {exc}")
            return None
    else:
        # OUTCAR-only fallback (ASE reads the ionic trajectory well).
        assert outcar is not None  # _find_calc_units guarantees a primary file
        try:
            frames, meta = _parse_outcar_ase(outcar, calc_id)
        except Exception as exc:
            rej.reject("parse", calc_id, "outcar_parse_error", detail=f"{type(exc).__name__}: {exc}")
            return None

    if not frames:
        rej.reject("parse", calc_id, "no_frames")
        return None

    # merge availability (from fetch listing) with what the parser could confirm
    avail = dict(availability)
    avail["spin_density"] = avail.get("charge_density", False) and meta["calc_parameters"].get("spin_polarized", False)
    avail["magnetisation"] = meta.get("site_magmoms_present", False) or meta["calc_parameters"].get("spin_polarized", False)
    meta["availability"] = avail

    meta["provenance"] = {**base_meta["provenance"],
                          "file_path": rel,
                          "parser": meta["parser"],
                          "harvested_at": datetime.now(timezone.utc).isoformat()}
    meta["frame_ids"] = [f.info["frame_id"] for f in frames]
    return frames, meta


def _parse_outcar_ase(outcar_path: str, calc_id: str) -> tuple[list[Atoms], dict]:
    """Fallback: read an OUTCAR ionic trajectory via ASE (no vasprun.xml).

    ASE's OUTCAR reader reaches into the working directory for a neighbouring
    CONTCAR/POSCAR (constraints), which crashes on uploads with nonstandard
    POTCAR-hash species lines (e.g. ``La_GW/21d20268``). Reading a lone copy in a
    temp dir sidesteps that and is format-pinned to avoid filename sniffing.
    """
    import os
    import shutil
    import tempfile

    from ase.io import read
    with tempfile.TemporaryDirectory() as td:
        lone = os.path.join(td, "OUTCAR")
        shutil.copy(outcar_path, lone)
        traj = read(lone, format="vasp-out", index=":")
    frames = []
    for i, atoms in enumerate(traj):
        atoms.info.update({"source": "zenodo", "calc_id": calc_id,
                           "frame_id": f"{calc_id}#{i}", "ionic_step": i,
                           "electronic_converged": None})
        frames.append(atoms)
    meta = {
        "calc_id": calc_id,
        "calc_parameters": {"code": "vasp", "run_type": None, "functional": None,
                            "note": "parsed from OUTCAR only; parameters limited"},
        "quality": {"electronic_converged": None, "scf_dE": None,
                    "ionic_converged": None, "n_ionic_steps": len(frames),
                    "n_atoms": len(frames[0]) if frames else 0, "n_frames": len(frames)},
        "parser": "ase.OUTCAR",
        "site_charges_present": False, "site_magmoms_present": False,
    }
    return frames, meta


def _load_committed(metadata_path: Path) -> tuple[set[str], set[str]]:
    """From an existing metadata.jsonl: (calc_ids done, frame_ids committed)."""
    done: set[str] = set()
    frame_ids: set[str] = set()
    if metadata_path.is_file():
        for rec in read_jsonl(metadata_path):
            if rec.get("calc_id"):
                done.add(rec["calc_id"])
            frame_ids.update(rec.get("frame_ids", []))
    return done, frame_ids


def parse(
    in_path: str | Path,
    dataset_dir: str | Path = config.DATASET_DIR,
    rejections_path: str | Path = config.MANIFEST_DIR / "rejections.jsonl",
    frames_per_shard: int = 10_000,
    max_records: int | None = None,
) -> dict:
    """Parse every fetched record into extxyz shards + a metadata JSONL.

    Safe to re-run mid-harvest: calcs already recorded in metadata.jsonl are
    skipped (no duplicate frames), each run writes to a *fresh* shard index so an
    existing shard is never reopened (no overflow), and orphan frames from a
    previously crashed run — written to a shard but never committed to metadata —
    are pruned before writing resumes.
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    done_calc_ids, committed_frame_ids = _load_committed(metadata_path)
    pruned = prune_uncommitted_frames(dataset_dir, committed_frame_ids)
    start_index = next_shard_index(dataset_dir)  # after pruning may drop shards
    if done_calc_ids or pruned["frames_dropped"]:
        logger.info("parse resume: %d calc(s) already done, pruned %s, new shards from %05d",
                    len(done_calc_ids), pruned, start_index)

    rej = RejectionLogger(rejections_path)
    stats: dict[str, Any] = {"records": 0, "calc_units": 0, "calcs_parsed": 0,
                             "skipped_existing": 0, "frames": 0}

    with ShardedExtxyzWriter(dataset_dir, frames_per_shard, start_index=start_index) as xyz, \
            MetadataWriter(metadata_path) as meta_w:
        for rec in read_jsonl(in_path):
            stats["records"] += 1
            base_meta = {"provenance": rec["provenance"],
                         "_extracted_root": str(Path(rec["local_dir"]) / "extracted")}
            for unit in rec["calc_units"]:
                stats["calc_units"] += 1
                calc_id = _calc_id(unit, base_meta)
                if calc_id in done_calc_ids:
                    stats["skipped_existing"] += 1
                    continue
                result = parse_calc_unit(unit, base_meta, rec.get("availability", {}), rej)
                if not result:
                    continue
                frames, meta = result
                shards: set[str] = set()
                for fr in frames:
                    shards.add(xyz.write(fr))
                    stats["frames"] += 1
                meta["shards"] = sorted(shards)
                meta_w.write(_jsonable(meta))
                done_calc_ids.add(calc_id)  # a duplicate unit later in THIS run is skipped too
                stats["calcs_parsed"] += 1
            if max_records and stats["records"] >= max_records:
                break
    rej.close()
    stats["rejections"] = rej.n
    stats["pruned"] = pruned
    logger.info("parse: %s", stats)
    return {"dataset_dir": str(dataset_dir), **stats}
