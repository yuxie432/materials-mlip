"""Stage 3 — parse.

Turn each fetched calculation into (a) per-ionic-step extxyz frames and (b) one
JSONL metadata record. pymatgen's ``Vasprun`` is the primary parser (mentor's
call); ``vaspout.h5`` (VASP's newer HDF5 output) is read via pymatgen's
``Vaspout`` — a ``Vasprun`` subclass with the same API — and ``OUTCAR``-only
calculations fall back to ASE's trajectory reader.

Electronic convergence is recorded at two granularities. Per frame, the info keys
``electronic_converged``/``scf_dE`` carry *that ionic step's OWN* SCF verdict and
magnitude (vasprun.xml exposes ``electronic_steps`` for every ionic step, so each
frame is tagged independently — see :func:`_step_scf`). ``scf_dE`` is |E[-1] − E[-2]|
of the ionic step's last two *electronic* steps (``e_0_energy``); calc-level
``quality.scf_dE`` is the same magnitude for the FINAL ionic step (see
:func:`_scf_convergence`). Both ``electronic_converged`` and ``scf_dE`` are written to a
frame ONLY when known: a None value ("unknown" — the OUTCAR/vaspout paths, or a step with
no electronic trace) would serialise in extxyz as a bare key that ASE reads back as
``True``, so it is omitted instead → read-back None (correctly "unknown"). Calc-level
``quality`` keeps the FINAL-step verdict under the same keys with pymatgen's
``converged_electronic`` semantics (unchanged), plus ``n_frames_scf_unconverged``. The run-type/functional,
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
                  (``REF_forces``) + stress (``REF_stress``), the calc's net magnetic
                  moment ``total_magnetization`` (μ_B) + net charge ``total_charge`` (e)
                  broadcast onto every frame, the force-consistent free energy ``E_free``
                  and (vasprun path) the electronic-entropy term ``entropy_TS``, small
                  quality tags. (Per-atom charges/magmoms are NOT stored — only the totals.)
* metadata JSONL: provenance, citation, calc parameters (incl. ``potcar_set_hash``),
                  convergence, availability, and the ``electronic`` block mirroring the
                  net moment/charge (see :mod:`electronic`).

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
import multiprocessing as mp
import re
import threading
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import ase.units
from ase import Atoms
from ase.stress import full_3x3_to_voigt_6_stress

from . import config
from .convergence import (converged_ionic_from_params, cparam, outcar_nelm,
                          outcar_scf_convergence)
from .electronic import electronic_from_object, electronic_from_outcar, empty_block, is_magnetic
from .manifest import RejectionLogger, read_jsonl
from .outcar_params import build_calc_parameters, read_header_lines
from .vasprun_params import resolved_parameters
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
    val = _vparam(v, "NELM")
    return int(val) if val is not None else 60


def _vparam(v: Any, key: str) -> Any:
    """A VASP tag from a pymatgen Vasprun/Vaspout object: resolved ``parameters`` first (what
    pymatgen's ``converged_ionic`` reads), then the user ``incar``. None if set in neither.
    (The stored-dict analogue is :func:`convergence.cparam`.)"""
    for src in ("parameters", "incar"):
        try:
            val = getattr(v, src).get(key)
        except (AttributeError, TypeError):
            val = None
        if val is not None:
            return val
    return None


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
    # VASP's RESOLVED/effective parameters (defaults filled), the authoritative analog of the
    # OUTCAR header's effective block. Storing them (the "better fix" in the metadata-gaps
    # memory) lets the resolver read authoritative values instead of guessing defaults, and
    # keeps the vasprun and OUTCAR `parameters` blocks on one schema. `resolved_parameters`
    # renames vasprun's ENMAX->ENCUT and restricts to the shared allow-list
    # (outcar_params.EFFECTIVE_TAGS) — the SAME function the vasprun recovery uses, so a live
    # parse and a recovered record emit a byte-identical block. (Without the ENMAX->ENCUT
    # rename the cutoff was silently dropped here: vasprun.parameters has no "ENCUT" key.)
    effective = resolved_parameters(params)
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
        "parameters": effective,
    }


# --- embedded electronic-structure availability probe ---------------------------------
# DOS and eigenvalues (and PROCAR-style projections) are frequently written straight INTO
# vasprun.xml / vaspout.h5, with no standalone DOSCAR/EIGENVAL/PROCAR file for the fetch
# stage's filename scan to catch — so those calcs were flagged False despite the data being
# present (the availability under-count in the metadata-gaps memo). These probes look for
# the data IN the primary output. They deliberately do NOT parse it (pymatgen is called with
# parse_dos/parse_eigen off precisely because the arrays are large): the vasprun probe is a
# streaming byte scan for the opening tags (holds ~one chunk, never the arrays), and the
# vaspout probe only lists the top-level ``/results`` group names. Both are best-effort — any
# read error yields all-False, leaving the fetch-stage flags untouched. Results are OR'd into
# availability (they only ever turn a flag ON), so they refine, never contradict, the scan.
_VASPRUN_ES_TAGS = {b"<dos": "dos", b"<eigenvalues": "eigenvalues", b"<projected": "projected"}


def _scan_vasprun_electronic(path: str) -> dict:
    """Stream a vasprun.xml (optionally gz/bz2/xz) and report which of dos / eigenvalues /
    projected it embeds, by finding their opening tags without building the XML tree.

    ``<dos>``/``<eigenvalues>``/``<projected>`` appear only as element tags in vasprun.xml
    (attribute *values* are quoted and closing tags read ``</dos`` etc.), so a substring
    search for ``<dos`` / ``<eigenvalues`` / ``<projected`` is unambiguous. Reads in 1 MiB
    chunks, carrying a small tail so a tag split across a chunk boundary is still found, and
    stops as soon as all three are seen. Peak memory is one chunk regardless of file size.
    """
    import bz2
    import gzip
    import lzma

    found = {"dos": False, "eigenvalues": False, "projected": False}
    openers: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open,
                               ".xz": lzma.open, ".lzma": lzma.open}
    opener = next((fn for suf, fn in openers.items() if path.lower().endswith(suf)), open)
    try:
        with opener(path, "rb") as fh:
            tail = b""
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                buf = tail + chunk
                for pat, kind in _VASPRUN_ES_TAGS.items():
                    if not found[kind] and pat in buf:
                        found[kind] = True
                if all(found.values()):
                    break
                tail = buf[-16:]  # > len(longest tag) - 1, to catch a boundary-split tag
    except OSError as exc:
        logger.debug("vasprun electronic-structure probe failed for %s: %s", path, exc)
    return found


def _probe_vaspout_electronic(path: str) -> dict:
    """Report which of dos / eigenvalues / projected a vaspout.h5 embeds, from the names of
    the ``/results`` group's immediate children (cheap — no dataset is read).

    VASP's HDF5 output nests electronic-structure results under ``/results`` (e.g.
    ``electron_dos``, ``electron_eigenvalues``, projector groups). A fuzzy substring match on
    the child names is used because the exact spelling varies by VASP version, and vaspout is
    a tiny fraction of the corpus (~345 calcs) so a defensive probe is enough. Needs h5py; if
    it is absent or the file cannot be opened, returns all-False (fetch-stage flags stand).
    """
    found = {"dos": False, "eigenvalues": False, "projected": False}
    try:
        import h5py  # type: ignore
    except ImportError:  # pragma: no cover - h5py is optional
        return found
    try:
        with h5py.File(path, "r") as f:
            grp = f["results"] if "results" in f else f
            names = [str(k).lower() for k in grp.keys()]
            found["dos"] = any("dos" in n for n in names)
            found["eigenvalues"] = any("eigen" in n for n in names)
            found["projected"] = any("proj" in n for n in names)
    except Exception as exc:  # noqa: BLE001 - h5 layouts drift; probe is best-effort
        logger.debug("vaspout electronic-structure probe failed for %s: %s", path, exc)
    return found


def _embedded_electronic_structure(vasprun: str | None, vaspout: str | None) -> dict:
    """Which of dos / eigenvalues / projected are embedded in a unit's primary output.

    Probes the vasprun.xml when present, else the vaspout.h5 (an OUTCAR embeds none of these
    the way a vasprun does — DOS/EIGENVAL there are separate files, caught by the scan). Uses
    the ORIGINAL primary path even when ``--max-primary-bytes`` trimmed it from the parse
    unit: the probe is a streaming scan and never holds the trajectory in memory, so it is
    safe on a file too large for pymatgen. Returns all-False when there is no vasprun/vaspout.
    """
    if vasprun:
        return _scan_vasprun_electronic(vasprun)
    if vaspout:
        return _probe_vaspout_electronic(vaspout)
    return {"dos": False, "eigenvalues": False, "projected": False}


def _merge_embedded_availability(base_avail: dict, vasprun: str | None,
                                 vaspout: str | None) -> dict:
    """Base (per-calc, filename-derived) availability OR'd with the embedded content probe.

    Returns a NEW dict: a copy of ``base_avail`` with ``dos``/``eigenvalues``/``projected``
    turned ON where the primary output embeds them (the probe only ever adds). Spin-derived
    flags (``spin_density``/``magnetization``) are NOT set here — they depend on the calc's
    ``spin_polarized``/``site_magmoms_present`` and are added by the caller. This is the SINGLE
    source of the fetch↔parse availability merge, shared with ``availability_recover`` so a
    live parse and a recovered record compute byte-identical flags.
    """
    avail = dict(base_avail)
    for kind, present in _embedded_electronic_structure(vasprun, vaspout).items():
        if present:
            avail[kind] = True
    return avail


# --- spin-flag pre-scan (decides parse_eigen without a full parse) --------------------
# The vasprun-only net-moment path (electronic.net_magnetization_from_object) needs the
# eigenvalue occupancies, i.e. Vasprun(parse_eigen=True) — which is more expensive than the
# default parse. But it is only needed for a *collinear spin-polarised* calc with no OUTCAR
# beside it (ISPIN=1 has zero moment; non-collinear needs the projected magnetization we do
# not parse; an OUTCAR gives the moment directly). So we cheaply pre-scan ISPIN /
# LNONCOLLINEAR / LSORBIT from the vasprun.xml prefix (they live in <parameters>, before the
# trajectory) and enable parse_eigen ONLY when it will actually be used. The ISPIN=1 majority
# — and every OUTCAR-accompanied calc — parses exactly as before.
_SPIN_FLAG_RES: dict[str, Any] = {
    "ispin": re.compile(rb'name="ISPIN">\s*(\d+)'),
    "lnoncollinear": re.compile(rb'name="LNONCOLLINEAR">\s*([TF])'),
    "lsorbit": re.compile(rb'name="LSORBIT">\s*([TF])'),
}


def _scan_vasprun_spin_flags(path: str) -> dict:
    """Cheap streaming read of ``ispin`` (int|None) + ``noncollinear`` (bool) from a vasprun.xml.

    Reads only the prefix up to the first ``<calculation>`` (all three tags sit in the
    ``<parameters>`` block that precedes the trajectory), in 256 KiB chunks with a small tail
    overlap so a tag split across a boundary is still matched. Best-effort — any read error
    yields ``{"ispin": None, "noncollinear": False}`` (the caller then parses without eigen,
    exactly the old behaviour). Peak memory is one chunk regardless of file size.
    """
    import bz2
    import gzip
    import lzma

    flags: dict[str, Any] = {"ispin": None, "noncollinear": False}
    openers: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open,
                               ".xz": lzma.open, ".lzma": lzma.open}
    opener = next((fn for suf, fn in openers.items() if path.lower().endswith(suf)), open)
    try:
        with opener(path, "rb") as fh:
            tail = b""
            while True:
                chunk = fh.read(1 << 18)
                if not chunk:
                    break
                buf = tail + chunk
                if flags["ispin"] is None:
                    m = _SPIN_FLAG_RES["ispin"].search(buf)
                    if m:
                        flags["ispin"] = int(m.group(1))
                for key in ("lnoncollinear", "lsorbit"):
                    mm = _SPIN_FLAG_RES[key].search(buf)
                    if mm and mm.group(1).upper() == b"T":
                        flags["noncollinear"] = True
                if b"<calculation" in buf:  # trajectory started -> parameters block is behind us
                    break
                tail = buf[-64:]  # > longest tag, to catch a boundary-split match
    except OSError as exc:
        logger.debug("vasprun spin-flag scan failed for %s: %s", path, exc)
    return flags




def _frame(structure: Any, energy: float | None, forces: Any, *, calc_id: str,
           frame_id: str, ionic_step: int, electronic_converged: bool | None,
           scf_dE: float | None, source: str = "zenodo",
           total_magnetization: float | None = None,
           total_charge: float | None = None) -> Atoms:
    from pymatgen.io.ase import AseAtomsAdaptor
    atoms = AseAtomsAdaptor.get_atoms(structure)
    # Write labels under MACE's default REF_* keys, straight into info/arrays. A
    # SinglePointCalculator would emit the reserved `energy`/`forces` keys, which
    # ASE re-absorbs into a calculator on read-back (removing them from info/arrays);
    # the REF_* keys survive the round-trip and stay queryable.
    if energy is not None:
        atoms.info["REF_energy"] = float(energy)
    if forces is not None:
        atoms.arrays["REF_forces"] = np.asarray(forces, dtype=float)
    # Per-calc net scalars broadcast onto every frame (net charge is frame-invariant; net
    # magnetization is the calc's converged value). Written ONLY when known — a None put into
    # atoms.info would serialise as a bare extxyz key ASE reads back as True (same trap as
    # electronic_converged/scf_dE below); omitting it -> read-back absent ("unavailable").
    if total_magnetization is not None:
        atoms.info["total_magnetization"] = float(total_magnetization)
    if total_charge is not None:
        atoms.info["total_charge"] = float(total_charge)
    atoms.info.update({
        "source": source,
        "calc_id": calc_id,
        "frame_id": frame_id,
        "ionic_step": ionic_step,
    })
    # Per-frame (this ionic step's OWN) SCF verdict + magnitude — same key names, per-frame
    # semantics (see _step_scf / module docstring). Both are written ONLY when known: a
    # None value ("unknown" — e.g. the OUTCAR/vaspout paths, or a step with no electronic
    # trace) put into atoms.info serialises in extxyz as a BARE key with no value, which
    # ASE reads back as ``True`` — silently relabelling an unknown-convergence frame as
    # converged (the exact opposite of "tag, don't silently include"). Omitting it makes
    # atoms.info.get("electronic_converged") return None on read-back (correctly "unknown"),
    # matching metadata.jsonl's null. scf_dE was already guarded this way.
    if electronic_converged is not None:
        atoms.info["electronic_converged"] = electronic_converged
    if scf_dE is not None:
        atoms.info["scf_dE"] = scf_dE
    return atoms


def _frames_and_meta(v: Any, calc_id: str, outcar_path: str | None,
                     parser: str, source: str = "zenodo", *,
                     eigen_parsed: bool = False,
                     atominfo_path: str | None = None,
                     outcar_scf: list[tuple[float | None, bool | None]] | None = None
                     ) -> tuple[list[Atoms], dict]:
    """Build frames + metadata from a parsed pymatgen object.

    Shared by :func:`parse_vasprun` and :func:`parse_vaspout`: ``Vaspout``
    subclasses ``Vasprun`` and exposes the same API, so only the constructor and
    the recorded ``parser`` tag differ. ``source`` (``zenodo``/``nomad``/…) is the
    provenance tag written into each frame's ``info["source"]`` (see :func:`_frame`).

    ``eigen_parsed`` says whether ``v`` was parsed with ``parse_eigen=True`` (so the net
    moment can use the occupancy method); ``atominfo_path`` is the vasprun.xml path (for the
    net-charge valence read — None for vaspout.h5, which has no atominfo XML). The net
    magnetization + net charge (:mod:`electronic`) are computed once per calc and broadcast
    onto every frame (``total_magnetization`` / ``total_charge``) + mirrored into ``meta``.

    ``outcar_scf`` (per ionic step, file order, ``(scf_dE, electronic_converged)`` from
    :func:`convergence.outcar_scf_convergence`) supplies the SCF convergence for a ``vaspout``
    calc with a co-located OUTCAR: ``vaspout.h5`` has no per-electronic-step data, so pymatgen
    leaves ``electronic_steps`` empty and :func:`_step_scf` returns ``(None, None)``; the OUTCAR
    fills that gap (basis ``free_energy``, see :mod:`convergence`). It is only consulted for a
    step the object itself could not tag, so a vasprun (native σ→0 ``scf_dE``) is unaffected.
    """
    conv = _scf_convergence(v)  # calc-level final-step verdict (keys/semantics unchanged)
    steps = v.ionic_steps
    natoms = len(v.final_structure)
    nelm = _nelm(v)
    # vaspout+OUTCAR: the object exposes no SCF trace, so lift the calc-level final-step verdict
    # from the OUTCAR too (mirrors the per-frame enrichment below). A vasprun already has it.
    if conv.get("electronic_converged") is None and outcar_scf:
        f_dE, f_conv = outcar_scf[-1]
        conv["electronic_converged"] = f_conv
        conv["scf_dE"] = f_dE
        conv["scf_dE_key"] = "free_energy"
        conv.pop("scf_note", None)

    # Net magnetic moment + net charge (per-calc scalars; see electronic.py). Magnetization
    # from the OUTCAR line when present, else the occupancy method (needs eigen_parsed).
    electronic = electronic_from_object(v, atominfo_path=atominfo_path,
                                        outcar_path=outcar_path, eigen_parsed=eigen_parsed)
    net_mag = electronic["net_magnetization"]
    net_charge = electronic["net_charge"]

    # Drop steps with no recoverable energy (keep original indices); count them.
    kept_steps, n_dropped = _select_frame_steps(steps)

    frames: list[Atoms] = []
    n_unconverged = 0
    n_with_forces = 0
    n_with_stress = 0
    max_ts_gap = 0.0  # max |E_free - E0| per atom across frames (label<->force consistency)
    for pos, (i, energy) in enumerate(kept_steps):
        st = steps[i]
        scf_dE, econv = _step_scf(st, nelm)  # this step's OWN convergence
        if scf_dE is None and econv is None and outcar_scf and i < len(outcar_scf):
            scf_dE, econv = outcar_scf[i]    # vaspout+OUTCAR: fill from the OUTCAR SCF trace
        if econv is False:
            n_unconverged += 1
        forces = st.get("forces")
        frame = _frame(
            st["structure"], energy, forces,
            calc_id=calc_id, frame_id=f"{calc_id}#{i}", ionic_step=i,
            electronic_converged=econv, scf_dE=scf_dE, source=source,
            total_magnetization=net_mag, total_charge=net_charge,
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
        # Net magnetic moment (mu_B) + net charge (e) for this calc — same values written to
        # each frame's info; kept here for querying without opening shards. See electronic.py.
        "electronic": electronic,
    }
    return frames, meta


def _outcar_scf_for(outcar_path: str | None
                    ) -> list[tuple[float | None, bool | None]] | None:
    """Per-ionic-step ``(scf_dE, electronic_converged)`` from an OUTCAR, or None.

    Reads the header once for NELM and streams the body for the SCF trace
    (:func:`convergence.outcar_scf_convergence`). Returns None when there is no OUTCAR or its
    header/body cannot be read — a best-effort enrichment that never raises. Shared by the
    ``vaspout`` parse path and the net-properties recovery so both derive identical values.
    """
    if not outcar_path:
        return None
    try:
        header = read_header_lines(outcar_path)
        conv = outcar_scf_convergence(outcar_path, outcar_nelm(header))
    except OSError as exc:
        logger.debug("OUTCAR SCF scan failed for %s: %s", outcar_path, exc)
        return None
    return conv or None


def parse_vasprun(vasprun_path: str, calc_id: str, outcar_path: str | None,
                  source: str = "zenodo") -> tuple[list[Atoms], dict]:
    """Parse one vasprun.xml into (frames, calc_metadata).

    ``parse_eigen`` is enabled ONLY when the net moment needs the occupancy method — a
    collinear spin-polarised calc (ISPIN=2, cheap pre-scan) with no OUTCAR beside it. With an
    OUTCAR the moment comes from its magnetization line; ISPIN=1 has none; non-collinear needs
    the projected magnetization we do not parse. So the ISPIN=1 majority and every
    OUTCAR-accompanied calc parse exactly as before (no eigen cost).
    """
    from pymatgen.io.vasp.outputs import Vasprun
    parse_eigen = False
    if outcar_path is None:
        flags = _scan_vasprun_spin_flags(vasprun_path)
        parse_eigen = flags.get("ispin") == 2  # occupancy method: collinear spin-polarised only
    v = Vasprun(vasprun_path, parse_dos=False, parse_eigen=parse_eigen,
                parse_projected_eigen=False, parse_potcar_file=False,
                exception_on_bad_xml=False)
    return _frames_and_meta(v, calc_id, outcar_path, "pymatgen.Vasprun", source,
                            eigen_parsed=parse_eigen, atominfo_path=vasprun_path)


def parse_vaspout(vaspout_path: str, calc_id: str, outcar_path: str | None,
                  source: str = "zenodo") -> tuple[list[Atoms], dict]:
    """Parse one vaspout.h5 (VASP's HDF5 output) into (frames, calc_metadata).

    Uses pymatgen's ``Vaspout`` (a ``Vasprun`` subclass). DOS/eigenvalues and
    POTCAR *contents* are skipped for speed and for storage parity with the
    vasprun.xml path (POTCAR titels/spec are still recorded via _calc_parameters).
    ``parse_eigen`` is enabled when there is no OUTCAR (so a spin-polarised vaspout's net
    moment can use the occupancy method); vaspout is a tiny fraction of the corpus, so the
    cost is negligible and the non-spin gate lives in ``electronic``.

    SCF convergence: ``vaspout.h5`` carries no per-electronic-step data (pymatgen leaves
    ``electronic_steps`` empty), so a co-located OUTCAR is scanned for the per-frame
    ``scf_dE``/``electronic_converged`` (:mod:`convergence`); without one they stay ``None``
    (genuinely unavailable — VASP does not write the SCF trace into the HDF5 output).
    """
    from pymatgen.io.vasp.outputs import Vaspout
    parse_eigen = outcar_path is None
    v = Vaspout(vaspout_path, parse_dos=False, parse_eigen=parse_eigen,
                parse_projected_eigen=False, store_potcar=False)
    outcar_scf = _outcar_scf_for(outcar_path)
    return _frames_and_meta(v, calc_id, outcar_path, "pymatgen.Vaspout", source,
                            eigen_parsed=parse_eigen, atominfo_path=None, outcar_scf=outcar_scf)


def electronic_block_for_unit(unit: dict) -> dict:
    """The ``electronic`` block (net moment + net charge) for a calc unit — WITHOUT its frames.

    The net-properties recovery (:mod:`net_properties_recover`) needs each calc's net
    moment/charge but not its trajectory, so this computes just the block, using the SAME
    primary precedence (vasprun > vaspout > outcar) and ``parse_eigen`` gating as
    :func:`parse_vasprun`/:func:`parse_vaspout` — so a recovered value is identical to what a
    fresh parse writes. ``unit`` maps roles to absolute paths. Falls back to a co-located OUTCAR
    if the vasprun/vaspout parse fails (mirroring :func:`parse_calc_unit`), and to
    :func:`empty_block` if nothing is parseable.
    """
    vasprun, vaspout, outcar = unit.get("vasprun"), unit.get("vaspout"), unit.get("outcar")
    try:
        from pymatgen.io.vasp.outputs import Vasprun, Vaspout
        if vasprun:
            try:
                parse_eigen = (outcar is None
                               and _scan_vasprun_spin_flags(vasprun).get("ispin") == 2)
                v = Vasprun(vasprun, parse_dos=False, parse_eigen=parse_eigen,
                            parse_projected_eigen=False, parse_potcar_file=False,
                            exception_on_bad_xml=False)
                return electronic_from_object(v, atominfo_path=vasprun, outcar_path=outcar,
                                              eigen_parsed=parse_eigen)
            except Exception:
                if not outcar:
                    raise            # no fallback -> report as empty below
        elif vaspout:
            try:
                parse_eigen = outcar is None
                v = Vaspout(vaspout, parse_dos=False, parse_eigen=parse_eigen,
                            parse_projected_eigen=False, store_potcar=False)
                return electronic_from_object(v, atominfo_path=None, outcar_path=outcar,
                                              eigen_parsed=parse_eigen)
            except Exception:
                if not outcar:
                    raise
        if outcar:
            try:
                lines = read_header_lines(outcar)
            except OSError:
                lines = []
            cp = build_calc_parameters(lines) if lines else None
            return electronic_from_outcar(cp, lines, outcar)
    except Exception as exc:  # noqa: BLE001 - a bad primary must not abort the recovery
        logger.warning("electronic block failed for unit %s: %s", unit.get("dir"), exc)
    return empty_block()


def convergence_block_for_unit(unit: dict) -> dict | None:
    """Per-frame SCF convergence for a calc unit whose stored parser is the OUTCAR reader.

    The net-properties recovery calls this ONLY for calcs parsed by :func:`_parse_outcar_ase`
    (``parser == "ase.OUTCAR"``) — those never got ``scf_dE``/``electronic_converged`` (the
    original OUTCAR path stored neither). Returns:

    * ``per_step`` — ``{str(ionic_step): [scf_dE, electronic_converged]}`` keyed by file-order
      ionic index, matching the stored ``frame_id``/``ionic_step`` so Phase 2 attaches each
      frame its own step's verdict (see :mod:`convergence` for the free-energy basis).
    * ``quality`` — the final-ionic-step verdict + ``n_frames_scf_unconverged`` to overwrite the
      calc's ``quality`` (which held ``electronic_converged=None`` before this recovery).

    ``None`` when the unit has no staged OUTCAR or no SCF trace could be parsed (leaving those
    frames' convergence untouched — i.e. still ``None``). A vasprun-parsed calc is never passed
    here: it already carries per-frame σ→0 ``scf_dE`` from the live parse.
    """
    outcar = unit.get("outcar")
    if not outcar or not Path(outcar).exists():
        return None
    try:
        header = read_header_lines(outcar)
    except OSError:
        header = []
    conv = outcar_scf_convergence(outcar, outcar_nelm(header))
    if not conv:
        return None
    per_step = {str(i): [dE, cv] for i, (dE, cv) in enumerate(conv)}
    final_dE, final_cv = conv[-1]
    n_unconv = sum(1 for _dE, cv in conv if cv is False)
    return {
        "per_step": per_step,
        "quality": {"electronic_converged": final_cv, "scf_dE": final_dE,
                    "scf_dE_key": "free_energy", "n_frames_scf_unconverged": n_unconv},
    }


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


def _source_of(base_meta: dict) -> str:
    """Provenance source (``zenodo``/``nomad``/…) for calc_id namespacing and the frame
    ``source`` tag.

    Read from the authoritative ``provenance.source`` the fetch stage wrote (Zenodo's
    ``_fetched_entry`` writes ``"zenodo"``, NOMAD's ``build_fetched_entry`` writes
    ``"nomad"``). Defaults to ``"zenodo"`` for older manifests predating the field, so
    their output stays byte-identical. This is what lets a second source (NOMAD) namespace
    its calc_ids as ``nomad:<id>:…`` and tag frames ``source="nomad"`` without any collision
    against the Zenodo dataset at ``merge-datasets`` time.
    """
    return (base_meta.get("provenance") or {}).get("source") or "zenodo"


def _calc_id(unit: dict, base_meta: dict) -> str:
    """Stable id for a calc unit: ``<source>:<recid>:<path-of-primary-file>``.

    Keyed on the primary file (vasprun.xml, else vaspout.h5, else OUTCAR) — the
    same precedence :func:`parse_calc_unit` parses with — so the id is identical
    whether we are parsing the unit or checking whether a prior run already did.
    The ``<source>`` prefix (``zenodo``/``nomad``/…) namespaces ids per data source.
    """
    recid = base_meta["provenance"]["record_id"]
    root = Path(base_meta["_extracted_root"])
    primary = unit.get("vasprun") or unit.get("vaspout") or unit["outcar"]
    return f"{_source_of(base_meta)}:{recid}:{Path(primary).relative_to(root)}"


_PRIMARY_ROLES_ORDER = ("vasprun", "vaspout", "outcar")


def _effective_primary_size(path: str) -> int:
    """Size to compare against ``--max-primary-bytes``: the UNCOMPRESSED size when it is
    cheaply known, else the on-disk size.

    pymatgen/ASE decompress a primary in memory, so peak RSS tracks the *uncompressed*
    size, not the bytes on disk — and fetch stages many primaries still gzip-compressed
    (``vasprun.xml.gz``/``OUTCAR.gz``; pymatgen and the OUTCAR fallback both read them
    directly). Guarding on the on-disk size would let a compressed long-AIMD output slip
    under the cap and still cgroup-kill the job — the exact failure the cap exists to
    prevent. For a gzip file the uncompressed size is the 4-byte little-endian ISIZE
    trailer (mod 2**32; exact for < 4 GiB), so use ``max(on_disk, ISIZE)``. A wrap
    (uncompressed >= 4 GiB) is far over any sane cap, so if a sizeable gzip reports an
    ISIZE below its own compressed size — impossible without a wrap — treat it as >= 4 GiB.
    Other codecs (.bz2/.xz/.zst) carry no cheap uncompressed size, so they fall back to the
    on-disk size (a documented limitation; gzip is by far the common case on Zenodo).
    Returns 0 if the file is missing/unreadable (the normal parse paths report that).
    """
    p = Path(path)
    try:
        size = p.stat().st_size
    except OSError:
        return 0
    if p.suffix.lower() == ".gz":
        try:
            with p.open("rb") as fh:
                fh.seek(-4, 2)  # 2 == SEEK_END; ISIZE is the last four bytes of a gzip
                isize = int.from_bytes(fh.read(4), "little")
        except OSError:
            return size
        if size >= (1 << 29) and isize < size:  # ISIZE wrapped -> uncompressed >= 4 GiB
            return max(size, 1 << 32)
        return max(size, isize)
    return size


def _oversized_primaries(unit: dict, max_primary_bytes: int) -> list[str]:
    """Primary-output roles in ``unit`` whose (effective) size exceeds ``max_primary_bytes``.

    pymatgen holds a run's whole ionic trajectory in memory, so a multi-GB
    ``vasprun.xml``/``OUTCAR`` (fetch stages these deliberately — long AIMD runs are the
    frame-richest data) can need many times its own size in RAM. Under a batch
    scheduler that is not a caught ``MemoryError`` but a cgroup SIGKILL of the whole
    job, losing the fetch progress too. This lets a long unattended run cap what it
    will attempt (0 = no cap) and log the skip instead. Sizing is on the *uncompressed*
    footprint for gzip primaries (see :func:`_effective_primary_size`), since RAM tracks
    that, not the compressed bytes on disk.
    """
    if not max_primary_bytes:
        return []
    over = []
    for role in _PRIMARY_ROLES_ORDER:
        path = unit.get(role)
        if not path:
            continue
        if _effective_primary_size(path) > max_primary_bytes:
            over.append(role)
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
    source = _source_of(base_meta)  # frame `source` tag; matches calc_id's prefix
    # Primary paths for the embedded electronic-structure probe, captured BEFORE any
    # oversized-primary trim: the probe streams the file (no trajectory in RAM), so it is
    # safe even on a primary too large for pymatgen, and it must not lose the vasprun to a trim.
    probe_vasprun, probe_vaspout = unit.get("vasprun"), unit.get("vaspout")
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
            frames, meta = parse_vasprun(vasprun, calc_id, outcar, source)
        except Exception as exc:
            if not outcar:
                rej.reject("parse", calc_id, "vasprun_parse_error", detail=f"{type(exc).__name__}: {exc}")
                return None
            logger.warning("vasprun parse failed for %s (%s); falling back to OUTCAR", calc_id, exc)
            fallback_from = "vasprun"
    elif vaspout:
        try:
            frames, meta = parse_vaspout(vaspout, calc_id, outcar, source)
        except Exception as exc:
            if not outcar:
                rej.reject("parse", calc_id, "vaspout_parse_error", detail=f"{type(exc).__name__}: {exc}")
                return None
            logger.warning("vaspout parse failed for %s (%s); falling back to OUTCAR", calc_id, exc)
            fallback_from = "vaspout"

    if frames is None:  # OUTCAR-only unit, or a primary-parse fallback
        assert outcar is not None  # _find_calc_units guarantees a primary file
        try:
            frames, meta = _parse_outcar_ase(outcar, calc_id, source)
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

    # merge availability (from the fetch listing — now per-calc, scoped to this calc's
    # directory) with what the parser can confirm from the primary output itself: DOS /
    # eigenvalues / projected embedded IN the vasprun.xml / vaspout.h5 (no standalone
    # DOSCAR/EIGENVAL/PROCAR file for the fetch scan to see) are OR'd on so those calcs are
    # no longer under-counted. The same helper backs availability_recover (no drift).
    avail = _merge_embedded_availability(availability, probe_vasprun, probe_vaspout)
    avail["spin_density"] = avail.get("charge_density", False) and meta["calc_parameters"].get("spin_polarized", False)
    # magnetization availability = the calc carries magnetization data (spin-polarised OR
    # non-collinear). Per-atom magmoms are no longer stored, so this replaces the old
    # `site_magmoms_present OR spin_polarized`; is_magnetic also catches non-collinear runs.
    avail["magnetization"] = is_magnetic(meta["calc_parameters"])
    meta["availability"] = avail

    # provenance.parser dropped: top-level meta["parser"] is the single source.
    meta["provenance"] = {**base_meta["provenance"],
                          "file_path": rel,
                          "harvested_at": datetime.now(timezone.utc).isoformat()}
    meta["frame_ids"] = [f.info["frame_id"] for f in frames]
    return frames, meta


def _parse_outcar_ase(outcar_path: str, calc_id: str,
                      source: str = "zenodo") -> tuple[list[Atoms], dict]:
    """Fallback: read an OUTCAR ionic trajectory via ASE (no vasprun.xml).

    ASE's OUTCAR reader reaches into the working directory for a neighbouring
    CONTCAR/POSCAR (constraints), which crashes on uploads with nonstandard
    POTCAR-hash species lines (e.g. ``La_GW/21d20268``). Reading a lone copy in a
    temp dir sidesteps that and is format-pinned to avoid filename sniffing. A
    compressed OUTCAR (``OUTCAR.gz``/``.bz2``/``.xz``) is decompressed first so the
    lone temp copy is always plain text.

    ASE recovers only the *trajectory* (positions/energy/forces/stress); the calc
    *parameters* (functional/run_type/INCAR/effective values/k-points/POTCAR) come from a
    separate read of the OUTCAR **header** (:func:`outcar_params.read_header_lines` +
    :func:`outcar_params.build_calc_parameters`), so the ``calc_parameters`` block matches the
    vasprun path rather than being near-empty. This closes the historical OUTCAR metadata gap
    (functional was ``null`` on 25.6% of frames — see the metadata-gaps memory /
    docs/EVALUATION.md). The same header lines feed :func:`electronic.electronic_from_outcar`
    (net moment from the OUTCAR magnetization line; net charge from ZVAL x ions per type).
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
    # Recover the full calc parameters from the OUTCAR HEADER (functional/run_type/INCAR/
    # effective parameters/k-points/POTCAR) — parity with the vasprun path, so an OUTCAR-only
    # calc is no longer a metadata black hole (the 25.6%-unknown-functional gap). Read the
    # header lines ONCE and reuse them for the net magnetization/charge scan (electronic.py).
    # Fall back to a minimal dict only if the header is unreadable (frames were already
    # recovered by ASE, so we still keep the calc).
    try:
        header_lines = read_header_lines(outcar_path)
    except OSError:
        header_lines = []
    if header_lines:
        calc_parameters = build_calc_parameters(header_lines)
        electronic = electronic_from_outcar(calc_parameters, header_lines, outcar_path)
    else:
        calc_parameters = {
            "code": "vasp", "run_type": None, "functional": None,
            "potcar_symbols": None, "potcar_set_hash": None,
            "parsed_from": "outcar_header_unreadable",
        }
        electronic = empty_block()
    net_mag = electronic["net_magnetization"]
    net_charge = electronic["net_charge"]

    # Per-ionic-step SCF convergence from the OUTCAR body — vasprun parity (scf_dE +
    # electronic_converged per frame). ASE surfaces none of the SCF trace, so scan the OUTCAR
    # directly (:mod:`convergence`); scf_conv[i] is the (scf_dE, converged) of ionic step i in
    # file order, aligned with the trajectory index i below. Basis is the free energy F (the
    # OUTCAR prints σ→0 only per ionic step, not per SCF step) — recorded as scf_dE_key.
    scf_conv = outcar_scf_convergence(outcar_path, outcar_nelm(header_lines))

    frames = []
    n_dropped = 0
    n_unconverged = 0
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
        atoms.info.update({"source": source, "calc_id": calc_id,
                           "frame_id": f"{calc_id}#{i}", "ionic_step": i})
        # Per-calc net moment + net charge broadcast onto every frame (guarded against None,
        # which would serialise as a bare extxyz key read back as True — see _frame).
        if net_mag is not None:
            atoms.info["total_magnetization"] = float(net_mag)
        if net_charge is not None:
            atoms.info["total_charge"] = float(net_charge)
        # This ionic step's OWN SCF verdict + magnitude, from the OUTCAR trace (keyed by the
        # trajectory index i = file-order ionic step). Both written ONLY when known — a None put
        # into atoms.info serialises as a bare extxyz key that reads back as True (see _frame);
        # omitting it makes read-back None ("unknown"), matching metadata.jsonl's null.
        if i < len(scf_conv):
            scf_dE_i, econv_i = scf_conv[i]
            if econv_i is not None:
                atoms.info["electronic_converged"] = econv_i
                if econv_i is False:
                    n_unconverged += 1
            if scf_dE_i is not None:
                atoms.info["scf_dE"] = scf_dE_i
        frames.append(atoms)
    # Calc-level (final-ionic-step) SCF verdict from the OUTCAR trace — mirrors the vasprun
    # path's _scf_convergence, but the magnitude is free-energy-based (scf_dE_key). None when
    # the trace could not be parsed.
    final_scf_dE, final_econv = scf_conv[-1] if scf_conv else (None, None)
    # Calc-level ionic convergence — parity with the vasprun/vaspout paths, which get it from
    # pymatgen's converged_ionic. ASE's vasp-out reader exposes no ionic flag, so reimplement
    # pymatgen's rule from NSW/IBRION/EDIFFG (in the OUTCAR header calc_parameters) + the ionic-
    # step count. (Its *magnitude* — the last-two-frames ΔE — is deliberately not stored: it is
    # not a per-frame label-quality signal for training, unlike the electronic scf_dE.)
    ionic_converged = converged_ionic_from_params(
        cparam(calc_parameters, "NSW"), cparam(calc_parameters, "IBRION"),
        cparam(calc_parameters, "EDIFFG"), len(traj))
    meta = {
        "calc_id": calc_id,
        "calc_parameters": calc_parameters,
        # n_ionic_steps counts all steps read; n_frames counts those actually stored (after the
        # no-energy drop). max_abs_free_minus_e0_per_atom uses ASE's free_energy (F) vs energy
        # (E0); entropy_TS itself is unavailable here (ASE exposes no e_wo_entrp).
        "quality": {"electronic_converged": final_econv, "scf_dE": final_scf_dE,
                    "scf_dE_key": "free_energy",
                    "ionic_converged": ionic_converged, "n_ionic_steps": len(traj),
                    "n_atoms": len(frames[0]) if frames else 0, "n_frames": len(frames),
                    "n_frames_scf_unconverged": n_unconverged,
                    "n_frames_with_forces": n_with_forces,
                    "n_frames_with_stress": n_with_stress,
                    "n_frames_dropped_no_energy": n_dropped,
                    "max_abs_free_minus_e0_per_atom": max_ts_gap},
        "parser": "ase.OUTCAR",
        "stress_units": "eV/A^3 (ASE Voigt [xx,yy,zz,yz,xz,xy], REF_stress label)",
        # Net magnetic moment (from the OUTCAR magnetization line) + net charge (header
        # ZVAL x ions per type); same values written to each frame's info. See electronic.py.
        "electronic": electronic,
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


# Parse rejections a re-parse would reproduce identically (same file + same pymatgen/ASE):
# skip these on resume instead of re-running the parser on known-bad calcs every time —
# measured wasting minutes per resume on a big bad record (e.g. one with ~7k unparseable
# OUTCARs) and appending DUPLICATE rejection lines each run (inflating the counts). NOT
# terminal, so kept retryable: parse_timeout (retry with a longer --parse-timeout),
# primary_too_large / parse_worker_died (retry on a bigger-RAM node), parse_error (an
# unexpected escape, possibly transient). frames_no_energy is a KEPT calc's audit line, not
# a rejection, so it is not here either. Override with retry_rejected=True after a
# pymatgen/ase upgrade that might now parse a previously-failing file.
_PARSE_TERMINAL_REJECT_REASONS = frozenset({
    "vasprun_parse_error", "vaspout_parse_error", "outcar_parse_error", "no_frames",
})


def _rejected_calc_ids(rejections_path: Path) -> set[str]:
    """calc_ids already rejected with a DETERMINISTIC parse failure — skipped on resume so
    the parser is not re-run on files it has already proven it cannot parse."""
    if not rejections_path.is_file():
        return set()
    return {r["id"] for r in read_jsonl(rejections_path)
            if r.get("stage") == "parse"
            and r.get("reason") in _PARSE_TERMINAL_REJECT_REASONS
            and isinstance(r.get("id"), str)}


# --- per-calc parse timeout (isolate a non-terminating pymatgen/ASE parse) ---------
# A single malformed vasprun.xml/OUTCAR can send pymatgen/ASE into a non-terminating (or
# effectively endless) parse. In the overlapped pipeline that hangs a background parse
# thread, which the orchestrator then blocks on -> the WHOLE harvest freezes, silently,
# until wallclock. A Python-level timeout cannot help: the hang is inside a C extension
# that ignores signals, and a stuck thread cannot be killed. So each calc unit is parsed
# in a short-lived child PROCESS that can be hard-killed on timeout.
#
# The child is forked from a `forkserver` (a clean, single-threaded parent), not `fork`:
# parse runs in a pipeline WORKER thread while fetch's download threads run too, and
# fork() in a multithreaded process can deadlock the child on a lock (e.g. logging) held
# by another thread at fork time. The forkserver preloads pymatgen/ase ONCE so each forked
# child inherits them (a fresh import per child would add ~0.5 s x hundreds of thousands
# of calcs). Preload is best-effort — a miss only costs speed, never correctness.
_MP_CTX: Any = None
_MP_LOCK = threading.Lock()
_FORKSERVER_PRELOAD = ["zenodo_harvest.parse", "pymatgen.io.vasp.outputs", "ase.io"]


def _get_mp_ctx() -> Any:
    """A cached multiprocessing context (forkserver where available, else spawn)."""
    global _MP_CTX
    with _MP_LOCK:
        if _MP_CTX is None:
            method = "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"
            ctx = mp.get_context(method)
            if method == "forkserver":
                try:
                    ctx.set_forkserver_preload(_FORKSERVER_PRELOAD)
                except Exception:  # noqa: BLE001 - preload is a speed-up, never required
                    pass
            _MP_CTX = ctx
        return _MP_CTX


class _RejCollector:
    """A pickle-safe stand-in for :class:`RejectionLogger` inside a parse child.

    A ``RejectionLogger`` owns an open file handle, so it cannot cross a process boundary.
    The child collects ``reject()`` calls into a plain list and the parent replays them
    into the real logger (which owns the audit file) — so ``parse_calc_unit`` runs
    unchanged in the child and no rejection line is lost.
    """

    def __init__(self) -> None:
        self.items: list[tuple[str, str, str, dict]] = []

    def reject(self, stage: str, ident: str, reason: str, **detail: Any) -> None:
        self.items.append((stage, ident, reason, detail))


def _parse_calc_unit_worker(unit: dict, base_meta: dict, availability: dict,
                            max_primary_bytes: int) -> tuple[Any, list]:
    """Module-level (picklable) child target: parse one unit, return its result plus the
    rejections it collected, for the parent to write."""
    coll = _RejCollector()
    result = parse_calc_unit(unit, base_meta, availability, coll,  # type: ignore[arg-type]
                             max_primary_bytes)
    return result, coll.items


def _pipe_call(send: Any, fn: Any, args: tuple) -> None:
    """Child entry point: run ``fn(*args)`` and send its outcome back over ``send``."""
    try:
        send.send(("ok", fn(*args)))
    except BaseException as exc:  # noqa: BLE001 - report ANY failure, incl. non-Exception
        try:
            send.send(("exc", f"{type(exc).__name__}: {exc}"))
        except BaseException:  # noqa: BLE001 - pipe already broken; nothing else to do
            pass
    finally:
        send.close()


def _run_with_timeout(fn: Any, args: tuple, timeout: float) -> tuple[str, Any]:
    """Run module-level ``fn(*args)`` in a child process, hard-killed after ``timeout`` s.

    Returns ``(status, value)``: ``("ok", return_value)``; ``("exc", message)`` if ``fn``
    raised; ``("timeout", None)`` if it did not finish in time (child SIGTERM'd then
    SIGKILL'd); ``("died", None)`` if the child vanished without a result (e.g. OOM-killed).
    """
    ctx = _get_mp_ctx()
    recv, send = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_pipe_call, args=(send, fn, args))
    proc.start()
    send.close()  # parent never sends; closing its end makes recv see EOF if the child dies
    try:
        if not recv.poll(timeout):
            proc.terminate()
            proc.join(5)
            if proc.is_alive():
                proc.kill()
                proc.join()
            return "timeout", None
        try:
            status, value = recv.recv()
        except EOFError:
            status, value = "died", None
    finally:
        recv.close()
    proc.join(10)
    if proc.is_alive():
        proc.kill()
        proc.join()
    return status, value


def _parse_one(unit: dict, base_meta: dict, availability: dict, rej: RejectionLogger,
               max_primary_bytes: int, timeout_s: float, calc_id: str
               ) -> tuple[list[Atoms], dict] | None:
    """Parse one calc unit, optionally in a hard-timed child process.

    ``timeout_s <= 0`` parses in-process (the historical path, used by the offline tests
    and any caller that opts out). Otherwise the parse runs in a child killed after
    ``timeout_s`` seconds: a non-terminating parse becomes a ``parse_timeout`` rejection
    and the harvest moves on instead of freezing. ``parse_timeout`` is NOT terminal — the
    calc is absent from metadata.jsonl, so a later run (a longer ``--parse-timeout``, or a
    bigger-RAM node) re-attempts it, exactly like ``primary_too_large``.
    """
    if timeout_s <= 0:
        return parse_calc_unit(unit, base_meta, availability, rej, max_primary_bytes)
    status, value = _run_with_timeout(
        _parse_calc_unit_worker, (unit, base_meta, availability, max_primary_bytes), timeout_s)
    if status == "timeout":
        logger.warning("parse timed out after %ss for %s; skipping (re-tried on a later run)",
                       timeout_s, calc_id)
        rej.reject("parse", calc_id, "parse_timeout", timeout_s=timeout_s)
        return None
    if status == "died":
        logger.warning("parse worker died (OOM?) for %s; skipping", calc_id)
        rej.reject("parse", calc_id, "parse_worker_died")
        return None
    if status == "exc":
        # parse_calc_unit catches its own parser errors, so an escape here is unexpected.
        rej.reject("parse", calc_id, "parse_error", detail=str(value))
        return None
    result, child_rejections = value
    for stage, ident, reason, detail in child_rejections:  # replay the child's audit lines
        rej.reject(stage, ident, reason, **detail)
    return result


def parse(
    in_path: str | Path,
    dataset_dir: str | Path = config.DATASET_DIR,
    rejections_path: str | Path = config.MANIFEST_DIR / "rejections.jsonl",
    frames_per_shard: int = 10_000,
    max_records: int | None = None,
    raw_dir: str | Path = config.RAW_DIR,
    gzip_level: int = 6,
    max_primary_bytes: int = 0,
    parse_timeout_s: float = 0,
    retry_rejected: bool = False,
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
    job cgroup-killed (taking the fetch progress with it). The size compared is the
    *uncompressed* one for gzip primaries (``vasprun.xml.gz``/``OUTCAR.gz`` — read from the
    gzip ISIZE trailer; see :func:`_effective_primary_size`), since RAM tracks that, not
    the compressed bytes on disk. Set it for long unattended cluster runs, sized against
    the job's ``--mem``; leave it 0 to attempt everything.

    ``parse_timeout_s`` (0 = off) parses each calc unit in a child process hard-killed after
    that many seconds, so one non-terminating pymatgen/ASE parse cannot silently freeze the
    whole run (see :func:`_parse_one`). Size it well above a legitimate long trajectory: an
    ionic relaxation is tens of steps (parses in seconds), and even a long AIMD run parses
    in a few minutes, whereas a truly stuck parse runs for hours — so a generous value (the
    CLI default is 20 min) clears real data while still catching hangs promptly. A timeout is
    logged as a ``parse_timeout`` rejection and, being absent from metadata, is re-attempted
    on a later run.

    On resume, calcs already rejected with a *deterministic* parse failure (see
    :data:`_PARSE_TERMINAL_REJECT_REASONS`) are **skipped** rather than re-parsed — re-running
    the parser on files it has already proven unparseable wastes minutes per resume on a big
    bad record and duplicates rejection lines. ``retry_rejected=True`` re-attempts them (use
    after a pymatgen/ase upgrade).
    """
    dataset_dir = Path(dataset_dir)
    raw_dir = Path(raw_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    stats: dict[str, Any] = {"records": 0, "calc_units": 0, "calcs_parsed": 0,
                             "skipped_existing": 0, "skipped_rejected": 0, "frames": 0}

    # Hold the dataset-dir lock across prune + all writes: two parse tasks sharing
    # one --dataset-dir would interleave shard/metadata writes and corrupt both
    # (the parallel model is one dataset dir per array task; merge afterwards).
    with DatasetLock(dataset_dir):
        done_calc_ids, committed_frame_ids = _load_committed(metadata_path)
        rejected_calc_ids = set() if retry_rejected else _rejected_calc_ids(Path(rejections_path))
        pruned = prune_uncommitted_frames(dataset_dir, committed_frame_ids)
        start_index = next_shard_index(dataset_dir)  # after pruning may drop shards
        if done_calc_ids or rejected_calc_ids or pruned["frames_dropped"]:
            logger.info("parse resume: %d calc(s) already done, %d previously rejected "
                        "(skipped), pruned %s, new shards from %05d",
                        len(done_calc_ids), len(rejected_calc_ids), pruned, start_index)

        with RejectionLogger(rejections_path) as rej, \
                ShardedExtxyzWriter(dataset_dir, frames_per_shard, start_index=start_index,
                                    compresslevel=gzip_level) as xyz, \
                MetadataWriter(metadata_path) as meta_w:
            for rec in read_jsonl(in_path):
                stats["records"] += 1
                base_meta = {"provenance": rec["provenance"],
                             "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
                # Per-calc availability (fetch now scopes heavy-output flags to each calc's
                # own directory). Aligned index-for-index with calc_units; fall back to the
                # record-level union for older manifests that predate calc_availability.
                calc_avails = rec.get("calc_availability")
                for idx, unit in enumerate(rec["calc_units"]):
                    stats["calc_units"] += 1
                    # Resolve stored (relative, or legacy absolute) paths against raw_dir.
                    unit = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
                    calc_id = _calc_id(unit, base_meta)
                    if calc_id in done_calc_ids:
                        stats["skipped_existing"] += 1
                        continue
                    if calc_id in rejected_calc_ids:  # known-bad; don't re-run the parser
                        stats["skipped_rejected"] += 1
                        continue
                    availability = (calc_avails[idx]
                                    if isinstance(calc_avails, list) and idx < len(calc_avails)
                                    else rec.get("availability", {}))
                    # Name the calc BEFORE parsing so a slow/hung parse is identifiable live
                    # (`tail -f` the log) and by the last line if the job is killed mid-parse.
                    logger.info("parsing %s", calc_id)
                    result = _parse_one(unit, base_meta, availability,
                                        rej, max_primary_bytes, parse_timeout_s, calc_id)
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
