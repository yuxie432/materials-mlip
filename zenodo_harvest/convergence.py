"""Electronic (SCF) convergence from an OUTCAR body + parser-agnostic ionic-convergence helpers.

The SCF part (below) recovers the per-frame `scf_dE`/`electronic_converged` an OUTCAR/vaspout calc
would otherwise lack. :func:`converged_ionic_from_params` covers **ionic** convergence — a
genuinely calc-level property (one geometry-relaxation trajectory), distinct from the per-frame
electronic one: it reimplements pymatgen's ``Vasprun.converged_ionic`` from NSW/IBRION/EDIFFG + the
ionic-step count, so the OUTCAR path reaches the same calc-level ``quality.ionic_converged`` the
vasprun/vaspout paths get from pymatgen directly. (The ionic-convergence *magnitude* — the
last-two-frames energy ΔE — is deliberately NOT stored: unlike the electronic SCF ΔE it is not a
per-frame label-quality signal for MLIP training, off-equilibrium frames are valid+desirable
data, and it is anyway derivable from the per-frame ``REF_energy``/``REF_forces`` already stored.)

---

Per-ionic-step electronic (SCF) convergence from an OUTCAR body.

vasprun.xml exposes per-electronic-step energies (pymatgen → ``ionic_steps[i]["electronic_steps"]``),
so :mod:`parse` already tags each vasprun frame with *its own* SCF convergence verdict + magnitude
(see :func:`parse._step_scf`). The OUTCAR carries the SAME information in its SCF trace, but ASE's
``vasp-out`` reader does not surface it, and ``vaspout.h5`` omits it entirely (pymatgen's ``Vaspout``
sets ``electronic_steps=[]`` — "no info about electronic steps in vaspout.h5"). This module recovers
it straight from the OUTCAR text, so an OUTCAR-parsed calc — and a ``vaspout`` calc with a co-located
OUTCAR — reaches vasprun-level per-frame convergence tagging.

Signals (identical in OUTCAR and OSZICAR; cf. ``pymatgen.io.vasp.outputs.Oszicar``):

* ``----- Iteration    X(   Y)  -----`` delimits ionic step ``X`` / electronic step ``Y`` (both
  1-based). Counting the electronic steps of an ionic step gives ``n_esteps``.
* ``total energy-change (2. order) :  dE  ( ... )`` prints **once per electronic step**; the first
  number ``dE`` is the change in the **free** energy ``F`` (VASP's ``TOTEN``) from the previous
  electronic step. So the LAST such value in ionic step ``X`` is ``|F[-1] − F[-2]|`` = the SCF
  extent-of-convergence for that step — the OUTCAR analogue of vasprun's ``scf_dE``.

**Energy-basis note.** vasprun's ``scf_dE`` uses the σ→0 energy (``e_0_energy``). The OUTCAR prints
σ→0 only **once per ionic step** (in the post-loop summary), not per electronic step, so the only
per-SCF energy signal it exposes is ``F``. ``F`` and ``E0`` differ only by the change in the
electronic-entropy term ``T·S`` between the last two (near-converged) electronic steps — a negligible
difference for the stored magnitude — and ``F`` is exactly the quantity VASP tests against ``EDIFF``.
Callers therefore tag the OUTCAR path ``scf_dE_key="free_energy"`` (vs the vasprun path's
``"e_0_energy"``) so the basis is explicit in the metadata.

**Convergence verdict.** ``electronic_converged = n_esteps < NELM`` — the same rule
:func:`parse._step_scf` applies to a vasprun ionic step (pymatgen's ``converged_electronic``
semantics), so the OUTCAR and vasprun booleans mean the same thing. ``None`` when NELM is unknown or
no electronic steps were parsed (never a fabricated verdict). VASP's own
``aborting loop because EDIFF is reached`` / ``… was not reached (unconverged)`` markers agree with
this rule and could be used as a cross-check, but the count rule is what keeps parity with vasprun.

Pure stdlib (reuses :func:`outcar_params._open_text` for gz/bz2/xz), so it is unit-tested offline
with no pymatgen/ase dependency.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .outcar_params import _open_text

# "----- Iteration    3(  12)  -----": ionic step 3, electronic step 12 (both 1-based).
_ITER_RE = re.compile(r"Iteration\s+(\d+)\(\s*(\d+)\s*\)")
# "total energy-change (2. order) :-0.7943085E+03  (-0.8109329E+03)" — first number is ΔF.
_ECHG_RE = re.compile(r"total energy-change \(2\. order\)\s*:\s*([-+0-9.Ee]+)")
# "NELM   =    120;   NELMIN=  6; NELMDL=-17" — match NELM but never NELMIN/NELMDL (no '=' after
# "NELM" in those). The default is VASP's own default of 60, matching parse._nelm.
_NELM_RE = re.compile(r"\bNELM\s*=\s*(\d+)")
_DEFAULT_NELM = 60


def outcar_nelm(header_lines: list[str]) -> int:
    """Max SCF steps NELM from OUTCAR header lines (VASP default 60 if absent).

    Mirrors :func:`parse._nelm` (which reads ``parameters["NELM"]``): the header prints
    ``NELM = …;   NELMIN= …; NELMDL= …`` on one line, so a single regex over the (already-read)
    header lines suffices, and ``NELMIN``/``NELMDL`` never false-match (no ``=`` directly after
    ``NELM`` in either).
    """
    for line in header_lines:
        m = _NELM_RE.search(line)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                break
    return _DEFAULT_NELM


def scan_outcar_scf(path: str | Path) -> list[dict]:
    """Stream an OUTCAR (plain or gz/bz2/xz) → per ionic step ``{"n_esteps", "scf_dE"}``.

    ``result[i]`` is ionic step ``i`` in file order (0-based, so it lines up with ASE's
    ``enumerate(traj)`` index and the stored ``ionic_step``): ``n_esteps`` is the number of
    electronic steps, ``scf_dE`` is ``|ΔF|`` of the *last* electronic step (None if that step's
    change line was unparseable, or the ionic step had no ``total energy-change`` line). Ionic
    steps are numbered from the ``Iteration X(Y)`` markers; a gap (a step with no markers at all)
    yields a zero-filled placeholder so the list stays index-aligned with the trajectory.
    """
    # ionic index (1-based, from the Iteration marker) -> running {n_esteps, last ΔF}.
    steps: dict[int, dict] = {}
    cur: int | None = None
    with _open_text(path) as fh:
        for raw in fh:
            m = _ITER_RE.search(raw)
            if m:
                cur = int(m.group(1))
                entry = steps.setdefault(cur, {"n": 0, "last": None})
                entry["n"] = max(entry["n"], int(m.group(2)))
                continue
            if cur is not None:
                c = _ECHG_RE.search(raw)
                if c:
                    try:
                        steps[cur]["last"] = float(c.group(1))
                    except ValueError:
                        pass
    if not steps:
        return []
    out: list[dict] = []
    for i in range(1, max(steps) + 1):          # dense 1..N so indices map to trajectory order
        e = steps.get(i, {"n": 0, "last": None})
        last = e["last"]
        out.append({"n_esteps": e["n"], "scf_dE": (abs(last) if last is not None else None)})
    return out


def outcar_scf_convergence(path: str | Path, nelm: int) -> list[tuple[float | None, bool | None]]:
    """Per ionic step (file order): ``(scf_dE, electronic_converged)`` for an OUTCAR.

    The direct OUTCAR analogue of :func:`parse._step_scf` over a whole trajectory:
    ``electronic_converged = n_esteps < nelm`` (None when ``nelm`` is falsy or the step had no
    electronic steps parsed — never fabricated), ``scf_dE`` is the free-energy magnitude from
    :func:`scan_outcar_scf`. Returns ``[]`` if no SCF trace is found (a truncated/foreign OUTCAR),
    so callers simply leave those frames' convergence unknown.
    """
    out: list[tuple[float | None, bool | None]] = []
    for blk in scan_outcar_scf(path):
        n = blk["n_esteps"]
        converged: bool | None = (n < nelm) if (nelm and n) else None
        out.append((blk["scf_dE"], converged))
    return out


# --- ionic convergence (calc-level, parser-agnostic) ------------------------------------

def cparam(calc_parameters: dict, key: str) -> Any:
    """A VASP tag from a stored ``calc_parameters`` dict (the OUTCAR-header / vasprun schema):
    resolved ``parameters`` first, then user ``incar``, then ``resolved``. None if absent
    everywhere. Shared by the OUTCAR parse path and the recovery so both read NSW/IBRION/EDIFFG
    identically (no pymatgen/ase needed → safe to call from the Phase-2 apply)."""
    for src in ("parameters", "incar", "resolved"):
        blk = calc_parameters.get(src)
        if isinstance(blk, dict) and blk.get(key) is not None:
            return blk.get(key)
    return None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def converged_ionic_from_params(nsw: Any, ibrion: Any, ediffg: Any, n_ionic_steps: int | None,
                                md_n_steps: int | None = None) -> bool | None:
    """pymatgen ``Vasprun.converged_ionic`` reimplemented from parameters + the ionic-step count.

    Faithful to pymatgen 2026.5.4 (outputs.py): a relaxation "converged" iff VASP exited before the
    max ionic steps (``len(ionic_steps) < NSW``); ``NSW ≤ 1`` (single-point) is trivially True; the
    ``IBRION=0`` (MD) and ``IBRION∈{1,2} & EDIFFG=0`` ("run for NSW steps") cases return True iff the
    full NSW steps ran. ``ibrion`` defaults exactly as pymatgen does (``-1`` when ``NSW∈{-1,0}`` else
    ``0``). ``md_n_steps`` (MD only) falls back to ``n_ionic_steps`` when unknown — the OUTCAR/ASE
    path has no separate MD-step count. Returns ``None`` only if the ionic-step count is unknown.
    """
    if n_ionic_steps is None:
        return None
    nsw = _as_int(nsw)
    if nsw is None:
        nsw = 0
    ib = _as_int(ibrion)
    if ib is None:
        ib = -1 if nsw in (-1, 0) else 0
    if ib == 0:  # MD: converged iff it ran the full NSW steps
        md = n_ionic_steps if md_n_steps is None else md_n_steps
        return nsw <= 1 or md == nsw
    try:
        ediffg_zero = float(ediffg) == 0.0
    except (TypeError, ValueError):
        ediffg_zero = False        # pymatgen defaults EDIFFG to 1 (non-zero) when absent
    if ib in (1, 2) and ediffg_zero:
        return nsw <= 1 or nsw == n_ionic_steps
    return nsw <= 1 or n_ionic_steps < nsw
