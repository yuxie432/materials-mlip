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
* extxyz frame  : positions, symbols, cell, energy (``REF_energy``) + forces
                  (``REF_forces``) (+ per-site DFT charges/magmoms as
                  ``dft_charge``/``dft_magmom`` on the final frame if an OUTCAR
                  provides them), small quality tags.
* metadata JSONL: provenance, citation, calc parameters, convergence, availability.

Energy/forces use the ``REF_energy``/``REF_forces`` keys (MACE's defaults). They are
written into ``atoms.info``/``atoms.arrays`` directly rather than via an ASE
``SinglePointCalculator`` because ASE re-absorbs the reserved ``energy``/``forces``
keys into a calculator on read-back, removing them from ``info``/``arrays`` — the
``REF_`` keys survive the round-trip and stay queryable.

Stress is parsed but NOT emitted as a training label: the raw VASP 3×3 tensor is
kept in the frame info (``stress_kbar``, in kBar) because VASP's kBar sign/scale
convention must be confirmed before feeding stress to training.
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from ase import Atoms

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
    if isinstance(obj, np.bool_):  # before np.integer: np.bool_ must stay JSON true/false
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return _jsonable(obj.tolist())
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)  # enums, Kpoints style, etc.


def _corrected_e0_energy(step: dict) -> float | None:
    """Sigma→0 energy of one ionic step, with pymatgen's vasprun.xml bugfix applied.

    The ionic-step ``<energy>`` block's ``e_0_energy`` (what ``step["e_0_energy"]``
    holds) is unreliable in some vasprun.xml files — the same bug pymatgen corrects
    in :pyattr:`Vasprun.final_energy` (outputs.py) but only for the *final* step. We
    apply that identical correction to *every* step: recompute the σ→0 energy from
    the last electronic step's (e_0 − e_fr) shift added to the ionic free energy, and
    prefer it when it differs from the raw value by > 1e-7 eV (or the raw is absent).
    Returns None only if no energy can be recovered (e.g. GW/response runs).
    """
    raw = step.get("e_0_energy")
    esteps = step.get("electronic_steps") or []
    if esteps:
        last = esteps[-1]
        try:
            fixed = round((last["e_0_energy"] - last["e_fr_energy"]) + step["e_fr_energy"], 8)
        except (KeyError, TypeError):
            return raw
        if raw is None or abs(raw - fixed) > 1e-7:
            return fixed
    return raw


def _scf_convergence(vasprun: Any) -> dict:
    """Electronic convergence bool + magnitude (ΔE of last two SCF steps).

    ``vaspout.h5`` exposes no per-SCF electronic steps, so pymatgen's
    ``converged_electronic`` would report an unconditional ``True`` there — which
    would silently admit an unconverged calc untagged. When electronic steps are
    absent we record ``electronic_converged=None`` (unknown) instead.
    """
    try:
        esteps = vasprun.ionic_steps[-1]["electronic_steps"]
    except (IndexError, KeyError, TypeError):
        esteps = []
    if not esteps:  # e.g. vaspout.h5 (no SCF trace) — don't fabricate a convergence verdict
        try:
            ionic = bool(vasprun.converged_ionic)
        except (KeyError, TypeError):
            ionic = None
        return {"electronic_converged": None, "scf_dE": None, "scf_dE_key": "e_0_energy",
                "ionic_converged": ionic, "scf_note": "electronic steps unavailable"}
    dE = None
    if len(esteps) >= 2:
        try:
            dE = abs(esteps[-1]["e_0_energy"] - esteps[-2]["e_0_energy"])
        except (KeyError, TypeError):
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

    run_type = str(vasprun.run_type)                # functional + U/vdW flavour
    return {
        "code": "vasp",
        "code_version": getattr(vasprun, "vasp_version", None),
        "run_type": run_type,
        # base XC alone (run_type minus the +U/+vdW suffixes) so `functional` is a
        # distinct, coarser selection key rather than a duplicate of run_type.
        "functional": run_type.split("+")[0],
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
    # Write labels under MACE's default REF_* keys, straight into info/arrays. A
    # SinglePointCalculator would emit the reserved `energy`/`forces` keys, which
    # ASE re-absorbs into a calculator on read-back (removing them from info/arrays);
    # the REF_* keys survive the round-trip and stay queryable. Per-atom DFT outputs
    # go under explicit output names (dft_charge/dft_magmom) rather than ASE's
    # `initial_*` input fields, which would mislabel computed outputs as inputs.
    if energy is not None:
        atoms.info["REF_energy"] = float(energy)
    if forces is not None:
        atoms.arrays["REF_forces"] = np.asarray(forces, dtype=float)
    if magmoms is not None:
        atoms.arrays["dft_magmom"] = np.asarray(magmoms, dtype=float)
    if charges is not None:
        atoms.arrays["dft_charge"] = np.asarray(charges, dtype=float)
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
            st["structure"], _corrected_e0_energy(st), st.get("forces"),
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

    # Primary parse: pymatgen Vasprun/Vaspout. On failure, if an OUTCAR is present
    # in the same unit, fall back to the ASE OUTCAR reader rather than dropping the
    # whole calc — real uploads carry vasprun.xml files pymatgen refuses (e.g. a
    # Fortran field-overflow ``LAMBDA_D_K=****`` in the parameters block) yet a
    # perfectly readable OUTCAR sits beside them.
    frames: list[Atoms] | None = None
    meta: dict | None = None
    fallback_from: str | None = None
    if vasprun:
        try:
            frames, meta = parse_vasprun(vasprun, calc_id, outcar)
        except Exception as exc:
            if not outcar:
                rej.reject("parse", calc_id, "vasprun_parse_error", detail=f"{type(exc).__name__}: {exc}")
                return None
            logger.warning("vasprun parse failed for %s (%s); falling back to OUTCAR", calc_id, exc)
            fallback_from = "vasprun"
    elif vaspout:
        try:
            frames, meta = parse_vaspout(vaspout, calc_id, outcar)
        except Exception as exc:
            if not outcar:
                rej.reject("parse", calc_id, "vaspout_parse_error", detail=f"{type(exc).__name__}: {exc}")
                return None
            logger.warning("vaspout parse failed for %s (%s); falling back to OUTCAR", calc_id, exc)
            fallback_from = "vaspout"

    if frames is None:  # OUTCAR-only unit, or a primary-parse fallback
        assert outcar is not None  # _find_calc_units guarantees a primary file
        try:
            frames, meta = _parse_outcar_ase(outcar, calc_id)
        except Exception as exc:
            reason = f"{fallback_from}_parse_error" if fallback_from else "outcar_parse_error"
            rej.reject("parse", calc_id, reason, detail=f"{type(exc).__name__}: {exc}")
            return None
        if fallback_from:
            meta["fallback_from"] = fallback_from  # provenance: primary was present but unparseable

    assert meta is not None
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
    temp dir sidesteps that and is format-pinned to avoid filename sniffing. A
    compressed OUTCAR (``OUTCAR.gz``/``.bz2``/``.xz``) is decompressed first so the
    lone temp copy is always plain text.
    """
    import bz2
    import gzip
    import lzma
    import os
    import shutil
    import tempfile

    from ase.io import read
    _openers: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open, ".lzma": lzma.open}
    opener = next((fn for suf, fn in _openers.items() if outcar_path.lower().endswith(suf)), None)
    with tempfile.TemporaryDirectory() as td:
        lone = os.path.join(td, "OUTCAR")
        if opener is None:
            shutil.copy(outcar_path, lone)
        else:
            with opener(outcar_path, "rb") as src, open(lone, "wb") as dst:
                shutil.copyfileobj(src, dst)
        traj = read(lone, format="vasp-out", index=":")
    if not isinstance(traj, list):
        traj = [traj]
    frames = []
    for i, atoms in enumerate(traj):
        # ASE's vasp-out reader attaches a calculator with energy/forces AND stress
        # (eV/Å³). Lift energy/forces onto the REF_* keys used everywhere else, keep
        # the stress only under a non-reserved key (parity with the vasprun path,
        # which withholds a trainable stress), then drop the calculator so no reserved
        # `energy`/`forces`/`stress` key leaks into extxyz.
        res = dict(atoms.calc.results) if atoms.calc is not None else {}
        atoms.calc = None
        if "energy" in res:
            atoms.info["REF_energy"] = float(res["energy"])
        if "forces" in res:
            atoms.arrays["REF_forces"] = np.asarray(res["forces"], dtype=float)
        if "stress" in res:  # ASE Voigt stress, eV/Å³ (units differ from vasprun path's kBar)
            atoms.info["stress_ase_evA3"] = _jsonable(res["stress"])
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
                # durability ordering: frames must be on disk BEFORE their metadata,
                # or a crash could leave metadata pointing at frames in no shard.
                xyz.flush()
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
