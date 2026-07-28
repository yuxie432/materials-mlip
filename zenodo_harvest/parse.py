"""Stage 3 — parse.

Turn each fetched calculation into (a) per-ionic-step extxyz frames and (b) one
JSONL metadata record. pymatgen's ``Vasprun`` is the primary parser (mentor's
call); ``vaspout.h5`` (VASP's newer HDF5 output) is read via pymatgen's
``Vaspout`` — a ``Vasprun`` subclass with the same API — and ``OUTCAR``-only
calculations fall back to ASE's trajectory reader.

Electronic convergence is recorded at two granularities. Per frame, the info keys
``electronic_converged``/``scf_dE`` carry *that ionic step's OWN* SCF verdict and
magnitude (vasprun.xml exposes ``electronic_steps`` for every ionic step, so each
frame is tagged independently — see :func:`_step_scf`). Calc-level ``quality`` keeps
the FINAL-step verdict under the same keys with pymatgen's ``converged_electronic``
semantics (unchanged), plus ``n_frames_scf_unconverged``. The run-type/functional,
full INCAR + k-points + POTCAR spec, and availability flags for heavy data we don't
store (charge/spin density, eigenvalues, DOS) are recorded too.

No-energy-frame policy: a frame whose corrected σ→0 energy is unrecoverable (e.g.
GW/response steps) is DROPPED — it is dead weight that can break MACE loaders.
Energy-only frames (no forces) are KEPT. Kept frames keep their ORIGINAL ionic-step
index in ``frame_id``/``ionic_step`` (never renumbered). ``quality`` records
``n_frames`` (stored), ``n_frames_with_forces``, and ``n_frames_dropped_no_energy``;
a calc that drops some frames logs one ``frames_no_energy`` audit line.

What lands where (docs/DESIGN.md §3):
* extxyz frame  : positions, symbols, cell, energy (``REF_energy``) + forces
                  (``REF_forces``) + stress (``REF_stress``) (+ per-site DFT
                  charges/magmoms as ``dft_charge``/``dft_magmom`` on the final frame
                  if an OUTCAR provides them), the force-consistent free energy
                  ``E_free`` and (vasprun path) the electronic-entropy term
                  ``entropy_TS``, small quality tags.
* metadata JSONL: provenance, citation, calc parameters (incl. ``potcar_set_hash``),
                  convergence, availability.

Energy-reference bookkeeping: the label is the σ→0 energy E0 (``REF_energy``), but
VASP's forces/stress are consistent with the FREE energy F, so every frame also stores
F as ``E_free`` (and the vasprun path the exact term T·S = E − F as ``entropy_TS``);
``quality.max_abs_free_minus_e0_per_atom`` summarises the worst |F − E0|/atom so a
train-time filter can drop frames where the entropy correction makes E0 an unreliable
label for the stored forces. ``calc_parameters.potcar_set_hash`` fingerprints the
POTCAR set (from TITEL strings) as a cross-record consistency key.

Energy/forces use the ``REF_energy``/``REF_forces`` keys (MACE's defaults). They are
written into ``atoms.info``/``atoms.arrays`` directly rather than via an ASE
``SinglePointCalculator`` because ASE re-absorbs the reserved ``energy``/``forces``
keys into a calculator on read-back, removing them from ``info``/``arrays`` — the
``REF_`` keys survive the round-trip and stay queryable.

Stress IS a training label (mentor decision 2026-07-20): per-frame stress is stored
under MACE's ``REF_stress`` key in **ASE's convention** — a Voigt-6 vector
``[xx, yy, zz, yz, xz, xy]`` in eV/Å³ with ASE's sign. The vasprun/vaspout path
converts VASP's raw kBar 3×3 tensor via :func:`_stress_to_ase_voigt` (exactly ASE's
own vasprun.xml reader: ``× −0.1 × ase.units.GPa`` then Voigt reorder); the OUTCAR
path takes ASE's ``vasp-out`` stress, which is already in this convention. Both paths
therefore emit an identical, MACE-trainable ``REF_stress``.
"""

from __future__ import annotations

import hashlib
import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import ase.units
from ase import Atoms
from ase.stress import full_3x3_to_voigt_6_stress

from . import config
from .manifest import RejectionLogger, read_jsonl
from .store import (
    DatasetLock,
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


def _potcar_set_hash(titels: list[str]) -> str | None:
    """Deterministic fingerprint of a calc's POTCAR set from its ordered TITEL strings.

    VASP's *real* POTCAR content hash needs the POTCAR file, which we never have
    (copyright + usually absent) — but the TITEL line (e.g. ``PAW_PBE Fe_pv
    06Sep2000``) already pins the functional, element variant, and generation date, so
    a hash of the ordered titels uniquely identifies the pseudopotential *set*. That
    makes POTCAR a usable cross-record consistency key: two calcs with the same
    ``potcar_set_hash`` used identical pseudopotentials (a real comparability axis for
    absolute energies — see the energy-reference notes). Returns None if no titels.
    """
    titels = [t for t in titels if t]
    if not titels:
        return None
    return hashlib.sha1("\n".join(titels).encode("utf-8", "replace")).hexdigest()[:16]


def _outcar_potcar_titels(outcar_path: str) -> list[str]:
    """Ordered, de-duplicated POTCAR TITEL strings from a (plain-text) OUTCAR.

    The OUTCAR header repeats ``TITEL  = PAW_PBE Fe_pv 06Sep2000`` once per species;
    we keep first-seen order so :func:`_potcar_set_hash` is stable. Best-effort — a
    read failure just yields ``[]`` (POTCAR hash stays None on that OUTCAR calc).
    """
    titels: list[str] = []
    try:
        with open(outcar_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "TITEL" in line and "=" in line:
                    t = line.split("=", 1)[1].strip()
                    if t and t not in titels:
                        titels.append(t)
    except OSError:
        pass
    return titels


def _stress_to_ase_voigt(stress_kbar_3x3: Any) -> np.ndarray | None:
    """VASP raw stress (kBar 3×3, from vasprun.xml/vaspout.h5) → ASE convention.

    Returns a Voigt-6 vector ``[xx, yy, zz, yz, xz, xy]`` in eV/Å³ with ASE's sign
    (or ``None`` if the input is not a 3×3 tensor — e.g. a malformed vaspout entry —
    so a bad stress is skipped rather than crashing the parse). This reproduces
    *exactly* ASE's own vasprun.xml reader (``ase.io.vasp.read_vasp_xml``:
    ``stress *= -0.1 * GPa`` then a Voigt reorder), so a ``REF_stress`` computed here is
    identical to what ASE would attach if it read the same file — and matches the OUTCAR
    path, where ASE's ``vasp-out`` reader already yields this convention. VASP reports
    stress in kBar with the sign *opposite* to ASE's; ``1 kBar = 0.1 GPa`` and
    ``ase.units.GPa`` converts GPa → eV/Å³. Mentor decision 2026-07-20: stress is a
    training label, stored in ASE's convention.
    """
    arr = np.asarray(stress_kbar_3x3, dtype=float)
    if arr.shape != (3, 3):
        return None
    return full_3x3_to_voigt_6_stress(arr * (-0.1 * ase.units.GPa))


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


def _entropy_ts(step: dict) -> float | None:
    """Electronic-entropy term ``T·S = E − F`` (eV) of one ionic step (a pure helper).

    From VASP's ``e_wo_entrp`` (E, energy without entropy) and ``e_fr_energy`` (F, the
    free/force-consistent energy); ``None`` if either is missing (the OUTCAR/ASE path
    exposes F but not E). Physically ≥ 0 since ``F = E − T·S`` with ``T·S ≥ 0``. A large
    value means our σ→0 energy label sits far from the force-consistent free energy, so
    the frame's energy and forces are mutually inconsistent (train-time filter signal).
    """
    e_wo = step.get("e_wo_entrp")
    e_fr = step.get("e_fr_energy")
    if e_wo is None or e_fr is None:
        return None
    return float(e_wo - e_fr)


def _step_scf(step: dict, nelm: int | None) -> tuple[float | None, bool | None]:
    """Per-ionic-step SCF magnitude + convergence verdict (a pure helper).

    Mirrors pymatgen's ``Vasprun.converged_electronic`` (io/vasp/outputs.py) applied
    to ONE ionic step: an SCF loop is "converged" iff it ran *fewer* electronic steps
    than NELM (i.e. it reached self-consistency before hitting the cap) —
    ``len(electronic_steps) < NELM``. pymatgen's ALGO=CHI / LEPSILON / ALGO=Exact
    +NELM==1 special cases hinge on INCAR fields not present in a single step dict,
    so they stay calc-level (see :func:`_scf_convergence`); this per-step helper
    implements only the general NELM rule.

    Returns ``(scf_dE, converged)``:
    * ``scf_dE`` = ``|e_0_energy[-1] − e_0_energy[-2]|`` of this step's electronic
      steps (None if < 2 e-steps, or a value is missing).
    * ``converged`` = ``len(e-steps) < NELM`` (None if there are no e-steps or NELM
      is unknown — don't fabricate a verdict, mirroring the vaspout.h5 handling).
    """
    esteps = step.get("electronic_steps") or []
    dE: float | None = None
    if len(esteps) >= 2:
        try:
            dE = abs(esteps[-1]["e_0_energy"] - esteps[-2]["e_0_energy"])
        except (KeyError, TypeError):
            dE = None
    converged: bool | None = None
    if esteps and nelm:
        converged = len(esteps) < nelm
    return dE, converged


def _nelm(v: Any) -> int:
    """Max SCF steps NELM. pymatgen reads ``parameters["NELM"]``; 60 is VASP's default."""
    for src in ("parameters", "incar"):
        try:
            nelm = getattr(v, src).get("NELM")
        except (AttributeError, TypeError):
            nelm = None
        if nelm is not None:
            return int(nelm)
    return 60


def _select_frame_steps(steps: list) -> tuple[list[tuple[int, float]], int]:
    """Partition ionic steps into ``(kept, n_dropped_no_energy)`` (a pure helper).

    A step whose corrected σ→0 energy is unrecoverable (``_corrected_e0_energy``
    returns None — e.g. GW/response steps) is DROPPED: an energyless frame is dead
    weight that can break MACE loaders. Energy-only steps (energy but no forces) are
    KEPT. Each kept step carries its ORIGINAL ionic-step index so frame_ids
    (``<calc_id>#<i>``) and the ``ionic_step`` tag stay stable/meaningful — kept
    frames are never renumbered.
    """
    kept: list[tuple[int, float]] = []
    dropped = 0
    for i, st in enumerate(steps):
        e = _corrected_e0_energy(st)
        if e is None:
            dropped += 1
        else:
            kept.append((i, e))
    return kept, dropped


def _scf_convergence(vasprun: Any) -> dict:
    """Calc-level electronic convergence bool + magnitude (ΔE of last two SCF steps).

    This is the FINAL-ionic-step verdict, using pymatgen's ``converged_electronic``
    semantics unchanged (per-frame verdicts are separate — see :func:`_step_scf`).
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
        # Fingerprint of the POTCAR *set* from its titels — a real cross-record
        # consistency key (VASP's own content hash needs the absent POTCAR file).
        "potcar_set_hash": _potcar_set_hash(
            [s.get("titel") for s in (vasprun.potcar_spec or [])]),
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
           frame_id: str, ionic_step: int, electronic_converged: bool | None,
           scf_dE: float | None,
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
        # Per-frame (this ionic step's own) SCF verdict/magnitude — key names
        # unchanged, semantics now per-frame (see _step_scf / module docstring).
        "electronic_converged": electronic_converged,
    })
    if scf_dE is not None:
        atoms.info["scf_dE"] = scf_dE
    return atoms


def _frames_and_meta(v: Any, calc_id: str, outcar_path: str | None,
                     parser: str) -> tuple[list[Atoms], dict]:
    """Build frames + metadata from a parsed pymatgen object.

    Shared by :func:`parse_vasprun` and :func:`parse_vaspout`: ``Vaspout``
    subclasses ``Vasprun`` and exposes the same API, so only the constructor and
    the recorded ``parser`` tag differ.
    """
    conv = _scf_convergence(v)  # calc-level final-step verdict (keys/semantics unchanged)
    steps = v.ionic_steps
    natoms = len(v.final_structure)
    nelm = _nelm(v)

    # Per-atom charges/spins (final geometry only) if an OUTCAR is alongside.
    site = {"magmoms": None, "charges": None}
    if outcar_path:
        site = _site_props_from_outcar(outcar_path, natoms)

    # Drop steps with no recoverable energy (keep original indices); count them.
    kept_steps, n_dropped = _select_frame_steps(steps)

    frames: list[Atoms] = []
    n_unconverged = 0
    n_with_forces = 0
    n_with_stress = 0
    max_ts_gap = 0.0  # max |E_free - E0| per atom across frames (label<->force consistency)
    for pos, (i, energy) in enumerate(kept_steps):
        st = steps[i]
        last = pos == len(kept_steps) - 1  # site props attach to the last KEPT frame
        scf_dE, econv = _step_scf(st, nelm)  # this step's OWN convergence
        if econv is False:
            n_unconverged += 1
        forces = st.get("forces")
        frame = _frame(
            st["structure"], energy, forces,
            calc_id=calc_id, frame_id=f"{calc_id}#{i}", ionic_step=i,
            electronic_converged=econv, scf_dE=scf_dE,
            magmoms=site["magmoms"] if last else None,
            charges=site["charges"] if last else None,
        )
        if forces is not None:
            n_with_forces += 1
        # Electronic-entropy bookkeeping. Our energy label is E0 (sigma->0), but VASP's
        # forces/stress are consistent with the FREE energy F, so a large entropy term
        # means E0 is not the exact potential of the stored forces. Record F (E_free) on
        # every frame so a train-time filter can check |F - E0|; on the vasprun path
        # also record the exact term T*S = E - F (= e_wo_entrp - e_fr_energy).
        e_free = st.get("e_fr_energy")
        if e_free is not None:
            frame.info["E_free"] = float(e_free)
            if natoms:
                max_ts_gap = max(max_ts_gap, abs(e_free - energy) / natoms)
        ts = _entropy_ts(st)
        if ts is not None:
            frame.info["entropy_TS"] = ts
        # Per-frame stress as a training label under MACE's REF_stress key, converted
        # to ASE's convention (eV/Å³ Voigt, sign-flipped — see _stress_to_ase_voigt).
        # Vasprun exposes the raw kBar tensor as "stress"; Vaspout as "stresses".
        stress = st.get("stress")
        if stress is None:
            stress = st.get("stresses")
        if stress is not None:
            voigt = _stress_to_ase_voigt(stress)
            if voigt is not None:
                frame.info["REF_stress"] = voigt
                n_with_stress += 1
        frames.append(frame)

    meta = {
        "calc_id": calc_id,
        "calc_parameters": _calc_parameters(v),
        "quality": {**conv, "n_ionic_steps": len(steps), "n_atoms": natoms,
                    "n_frames": len(frames),
                    "n_frames_scf_unconverged": n_unconverged,
                    "n_frames_with_forces": n_with_forces,
                    "n_frames_with_stress": n_with_stress,
                    "n_frames_dropped_no_energy": n_dropped,
                    # max over frames of |E_free - E0|/atom (eV/atom): how far the
                    # sigma->0 label sits from the force-consistent free energy.
                    "max_abs_free_minus_e0_per_atom": max_ts_gap},
        "parser": parser,
        "stress_units": "eV/A^3 (ASE Voigt [xx,yy,zz,yz,xz,xy], REF_stress label)",
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


def _resolve(raw_dir: Path, stored: str) -> Path:
    """Resolve a fetched-manifest path against ``raw_dir``.

    fetch.py stores calc-unit paths RELATIVE to ``raw_dir`` so staged data can move
    between cluster scratch areas without invalidating the manifest. Older manifests
    stored absolute paths; pathlib's ``raw_dir / "/abs"`` yields ``/abs`` unchanged,
    so this single join handles both the new relative and old absolute forms. Because
    :func:`_calc_id` keys off the primary file's path *relative to* ``<local_dir>/
    extracted``, the recid prefix cancels and the calc_id is byte-identical whichever
    form the manifest used (and regardless of where raw_dir now points).
    """
    return raw_dir / stored


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


_PRIMARY_ROLES_ORDER = ("vasprun", "vaspout", "outcar")


def _oversized_primaries(unit: dict, max_primary_bytes: int) -> list[str]:
    """Primary-output roles in ``unit`` whose file exceeds ``max_primary_bytes``.

    pymatgen holds a run's whole ionic trajectory in memory, so a multi-GB
    ``vasprun.xml``/``OUTCAR`` (fetch stages these deliberately — long AIMD runs are the
    frame-richest data) can need many times its own size in RAM. Under a batch
    scheduler that is not a caught ``MemoryError`` but a cgroup SIGKILL of the whole
    job, losing the fetch progress too. This lets a long unattended run cap what it
    will attempt (0 = no cap) and log the skip instead.
    """
    if not max_primary_bytes:
        return []
    over = []
    for role in _PRIMARY_ROLES_ORDER:
        path = unit.get(role)
        if not path:
            continue
        try:
            if Path(path).stat().st_size > max_primary_bytes:
                over.append(role)
        except OSError:
            continue  # missing/unreadable is the existing parse paths' problem, not ours
    return over


def parse_calc_unit(unit: dict, base_meta: dict, availability: dict,
                    rej: RejectionLogger, max_primary_bytes: int = 0) -> tuple[list[Atoms], dict] | None:
    """Parse one calc unit (a dir with vasprun.xml, vaspout.h5, and/or OUTCAR).

    ``max_primary_bytes`` (0 = no cap) skips a primary output too big to parse within
    the job's memory — see :func:`_oversized_primaries`. A unit whose *every* primary is
    oversized is rejected (``primary_too_large``); if a smaller sibling remains (e.g. a
    huge vasprun.xml beside a modest OUTCAR) the oversized one is dropped from the unit
    and the normal precedence picks the next parseable primary.
    """
    # calc_id is keyed off the ORIGINAL unit's primary (precedence vasprun>vaspout>outcar)
    # and is computed ONCE here, BEFORE any oversized-primary trimming below — so it is
    # stable across a trim. It has to be: the caller (`parse`) derives the resume/skip key
    # and the `done_calc_ids` entry from this same untrimmed unit, and stores this id (plus
    # `<calc_id>#<step>` frame_ids) in metadata.jsonl, which `_load_committed` reads back on
    # resume. If trimming away a huge vasprun.xml re-keyed calc_id to the surviving OUTCAR,
    # a resumed run would not recognise the calc as already done, would re-parse it, and
    # would emit duplicate frames + a duplicate metadata record — and `verify`'s frame_id
    # bijection would then report the whole dataset corrupt. Under the recommended
    # `--max-primary-bytes` this is the common case (long-AIMD vasprun.xml beside a modest
    # OUTCAR), so calc_id must NOT depend on which primary survives trimming.
    calc_id = _calc_id(unit, base_meta)
    if max_primary_bytes:
        over = _oversized_primaries(unit, max_primary_bytes)
        if over:
            trimmed = {k: v for k, v in unit.items() if k not in over}
            if not any(trimmed.get(r) for r in _PRIMARY_ROLES_ORDER):
                # Reject under the ORIGINAL unit's calc_id, so the audit line names the
                # same calc a later (bigger-memory, or uncapped) re-run would parse.
                rej.reject("parse", calc_id, "primary_too_large",
                           roles=over, max_primary_bytes=max_primary_bytes)
                return None
            logger.warning("skipping oversized primary(s) %s (> %d B) in %s; using a "
                           "smaller sibling output", over, max_primary_bytes, unit.get("dir"))
            unit = trimmed
    vasprun = unit.get("vasprun")
    vaspout = unit.get("vaspout")
    outcar = unit.get("outcar")
    extracted_root = Path(base_meta["_extracted_root"])
    rel = (str(Path(unit["dir"]).relative_to(extracted_root))
           if (vasprun or vaspout or outcar) else unit["dir"])

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

    # Audit: if some (but not all) frames were dropped for lack of a recoverable
    # energy, log ONE line for the calc (not one per frame). The calc is still kept.
    n_dropped = meta["quality"].get("n_frames_dropped_no_energy", 0)
    if n_dropped:
        rej.reject("parse", calc_id, "frames_no_energy", dropped=n_dropped, kept=len(frames))

    # merge availability (from fetch listing) with what the parser could confirm
    avail = dict(availability)
    avail["spin_density"] = avail.get("charge_density", False) and meta["calc_parameters"].get("spin_polarized", False)
    avail["magnetization"] = meta.get("site_magmoms_present", False) or meta["calc_parameters"].get("spin_polarized", False)
    meta["availability"] = avail

    # provenance.parser dropped: top-level meta["parser"] is the single source.
    meta["provenance"] = {**base_meta["provenance"],
                          "file_path": rel,
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
        potcar_titels = _outcar_potcar_titels(lone)  # read TITEL lines before td closes
    if not isinstance(traj, list):
        traj = [traj]
    frames = []
    n_dropped = 0
    n_with_forces = 0
    n_with_stress = 0
    max_ts_gap = 0.0
    for i, atoms in enumerate(traj):
        # ASE's vasp-out reader attaches a calculator with energy/forces AND stress
        # (already ASE convention: eV/Å³ Voigt). Lift energy/forces/stress onto the
        # REF_* keys used everywhere else, then drop the calculator so no reserved
        # `energy`/`forces`/`stress` key leaks into extxyz (ASE would re-absorb them).
        # The OUTCAR-path stress is already in ASE units, so — unlike the vasprun path
        # (raw kBar) — it needs no conversion; both end up as the same REF_stress label.
        res = dict(atoms.calc.results) if atoms.calc is not None else {}
        atoms.calc = None
        if "energy" not in res:
            n_dropped += 1  # drop energyless frames (parity with the vasprun path)
            continue
        atoms.info["REF_energy"] = float(res["energy"])
        if "free_energy" in res:  # F (force-consistent) vs E0 label; no e_wo_entrp here
            atoms.info["E_free"] = float(res["free_energy"])
            if len(atoms):
                max_ts_gap = max(max_ts_gap, abs(res["free_energy"] - res["energy"]) / len(atoms))
        if "forces" in res:
            atoms.arrays["REF_forces"] = np.asarray(res["forces"], dtype=float)
            n_with_forces += 1
        if "stress" in res:  # ASE Voigt stress, eV/Å³ (already ASE convention)
            atoms.info["REF_stress"] = np.asarray(res["stress"], dtype=float)
            n_with_stress += 1
        atoms.info.update({"source": "zenodo", "calc_id": calc_id,
                           "frame_id": f"{calc_id}#{i}", "ionic_step": i,
                           "electronic_converged": None})
        frames.append(atoms)
    meta = {
        "calc_id": calc_id,
        "calc_parameters": {"code": "vasp", "run_type": None, "functional": None,
                            # POTCAR titels ARE recoverable from the OUTCAR header, so
                            # the set-hash consistency key works on this path too.
                            "potcar_titels": potcar_titels or None,
                            "potcar_set_hash": _potcar_set_hash(potcar_titels),
                            "note": "parsed from OUTCAR only; parameters limited"},
        # No SCF trace from OUTCAR here, so per-frame convergence stays None (never
        # False) -> n_frames_scf_unconverged is 0. n_ionic_steps counts all steps
        # read; n_frames counts those actually stored (after the no-energy drop).
        # max_abs_free_minus_e0_per_atom uses ASE's free_energy (F) vs energy (E0);
        # entropy_TS itself is unavailable here (ASE exposes no e_wo_entrp).
        "quality": {"electronic_converged": None, "scf_dE": None,
                    "ionic_converged": None, "n_ionic_steps": len(traj),
                    "n_atoms": len(frames[0]) if frames else 0, "n_frames": len(frames),
                    "n_frames_scf_unconverged": 0,
                    "n_frames_with_forces": n_with_forces,
                    "n_frames_with_stress": n_with_stress,
                    "n_frames_dropped_no_energy": n_dropped,
                    "max_abs_free_minus_e0_per_atom": max_ts_gap},
        "parser": "ase.OUTCAR",
        "stress_units": "eV/A^3 (ASE Voigt [xx,yy,zz,yz,xz,xy], REF_stress label)",
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
    raw_dir: str | Path = config.RAW_DIR,
    gzip_level: int = 6,
    max_primary_bytes: int = 0,
) -> dict:
    """Parse every fetched record into extxyz shards + a metadata JSONL.

    Safe to re-run mid-harvest: calcs already recorded in metadata.jsonl are
    skipped (no duplicate frames), each run writes to a *fresh* shard index so an
    existing shard is never reopened (no overflow), and orphan frames from a
    previously crashed run — written to a shard but never committed to metadata —
    are pruned before writing resumes.

    ``raw_dir`` is where fetch staged the files; manifest paths are stored relative
    to it and resolved back here (see :func:`_resolve`), so relocated scratch data
    still parses. Absolute paths in older manifests pass through unchanged.

    ``max_primary_bytes`` (0 = no cap) refuses to *attempt* a primary output bigger than
    this, logging a ``primary_too_large`` rejection instead. pymatgen holds a whole ionic
    trajectory in memory, so on a batch scheduler one huge ``vasprun.xml`` can get the
    job cgroup-killed (taking the fetch progress with it). Set it for long unattended
    cluster runs, sized against the job's ``--mem``; leave it 0 to attempt everything.
    """
    dataset_dir = Path(dataset_dir)
    raw_dir = Path(raw_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    stats: dict[str, Any] = {"records": 0, "calc_units": 0, "calcs_parsed": 0,
                             "skipped_existing": 0, "frames": 0}

    # Hold the dataset-dir lock across prune + all writes: two parse tasks sharing
    # one --dataset-dir would interleave shard/metadata writes and corrupt both
    # (the parallel model is one dataset dir per array task; merge afterwards).
    with DatasetLock(dataset_dir):
        done_calc_ids, committed_frame_ids = _load_committed(metadata_path)
        pruned = prune_uncommitted_frames(dataset_dir, committed_frame_ids)
        start_index = next_shard_index(dataset_dir)  # after pruning may drop shards
        if done_calc_ids or pruned["frames_dropped"]:
            logger.info("parse resume: %d calc(s) already done, pruned %s, new shards from %05d",
                        len(done_calc_ids), pruned, start_index)

        with RejectionLogger(rejections_path) as rej, \
                ShardedExtxyzWriter(dataset_dir, frames_per_shard, start_index=start_index,
                                    compresslevel=gzip_level) as xyz, \
                MetadataWriter(metadata_path) as meta_w:
            for rec in read_jsonl(in_path):
                stats["records"] += 1
                base_meta = {"provenance": rec["provenance"],
                             "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
                for unit in rec["calc_units"]:
                    stats["calc_units"] += 1
                    # Resolve stored (relative, or legacy absolute) paths against raw_dir.
                    unit = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
                    calc_id = _calc_id(unit, base_meta)
                    if calc_id in done_calc_ids:
                        stats["skipped_existing"] += 1
                        continue
                    result = parse_calc_unit(unit, base_meta, rec.get("availability", {}),
                                             rej, max_primary_bytes)
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
    stats["rejections"] = rej.n
    stats["pruned"] = pruned
    logger.info("parse: %s", stats)
    return {"dataset_dir": str(dataset_dir), **stats}
