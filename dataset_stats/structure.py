"""Per-structure descriptors used by the statistics: composition, cell geometry, vacuum
(dimensionality), shortest interatomic distance and space group.

Everything here is computed from the cell, the Cartesian positions and the species of ONE frame,
with numpy (+ ASE's neighbour list and spglib, both already dependencies of the harvest env).
The scan calls it for the first and the last frame of each calc only — composition never changes
within a VASP run, and two frames bound a trajectory — so the cost is ~1-3 ms per calc.
"""

from __future__ import annotations

import math
import os
from typing import Any

import numpy as np

# spglib's C core prints "ssm_get_exact_positions failed" etc. to stderr for every rattled /
# low-symmetry cell; at millions of cells that drowns the job log. It honours this switch.
os.environ.setdefault("SPGLIB_WARNING", "OFF")

# 1 amu / 1 A^3 in g/cm^3
AMU_PER_A3_TO_G_CM3 = 1.66053906660
# A direction counts as vacuum when the largest empty slab of space perpendicular to it is at
# least this wide. Bonds are < ~3.5 A and vdW layer gaps ~3-4 A, so 6 A separates a vacuum
# region (slabs/molecules are usually given 10-20 A) from bulk. The gaps themselves are stored,
# so the report can re-threshold.
VACUUM_GAP_A = 6.0
# Shortest-distance search radius. A structure with no pair inside it records None (= "> cutoff").
DMIN_CUTOFF_A = 3.0
# spglib cost grows steeply with cell size (31 ms at 432 atoms vs ~1 ms at 50); larger cells are
# almost never symmetric anyway (supercells, slabs, AIMD), so they are skipped.
SPG_MAX_ATOMS = 400
SPG_SYMPREC = 0.1

DIM_LABELS = {0: "bulk", 1: "slab", 2: "wire", 3: "molecule"}


def element_of(symbol: str) -> str:
    """Element of a species/POTCAR symbol: ``Fe_pv`` -> ``Fe``, ``H1.25`` -> ``H`` (pseudo-H)."""
    s = symbol.split("_", 1)[0].split(".", 1)[0]
    return "".join(ch for ch in s if ch.isalpha())


def composition(species: list[str]) -> dict[str, int]:
    """Element -> atom count."""
    counts: dict[str, int] = {}
    for sp in species:
        counts[sp] = counts.get(sp, 0) + 1
    return counts


def reduced_formula(counts: dict[str, int]) -> str:
    """Canonical reduced formula: elements in alphabetical order, counts divided by their gcd
    (``{'O': 6, 'Fe': 4}`` -> ``Fe2O3``). Alphabetical, not electronegativity order, so it is a
    pure function of the composition and identical however the reference data spell it."""
    g = 0
    for c in counts.values():
        g = math.gcd(g, int(c))
    g = g or 1
    parts = []
    for el in sorted(counts):
        n = int(counts[el]) // g
        parts.append(el if n == 1 else f"{el}{n}")
    return "".join(parts)


def chemsys(counts: dict[str, int]) -> str:
    return "-".join(sorted(counts))


def cell_geometry(cell: np.ndarray) -> tuple[float, np.ndarray] | None:
    """(volume A^3, |b_i| reciprocal lengths in 1/A without the 2*pi), or None if singular."""
    vol = float(abs(np.linalg.det(cell)))
    if not math.isfinite(vol) or vol < 1e-8:
        return None
    recip = np.linalg.inv(cell).T  # rows b_i with b_i . a_j = delta_ij
    return vol, np.linalg.norm(recip, axis=1)


def vacuum_gaps(cell: np.ndarray, positions: np.ndarray) -> list[float] | None:
    """Widest empty slab (A) perpendicular to each lattice direction.

    Fractional coordinates along ``a_i`` are sorted on the unit circle; the largest gap between
    neighbours (including the wrap-around) times the interplanar spacing ``1/|b_i|`` is the
    thickness of the widest atom-free slab. A slab model has one large gap (its vacuum), a
    molecule in a box three, a bulk crystal none. One atom gives the whole spacing (a lone atom
    in a 10 A box is a "molecule"; an fcc primitive cell is bulk because its spacing is ~2 A).
    """
    geo = cell_geometry(cell)
    if geo is None or len(positions) == 0:
        return None
    _vol, rlen = geo
    frac = positions @ np.linalg.inv(cell)
    gaps = []
    for i in range(3):
        f = np.sort(np.mod(frac[:, i], 1.0))
        if len(f) == 1:
            g = 1.0
        else:
            g = max(float(np.max(np.diff(f))), float(1.0 - f[-1] + f[0]))
        gaps.append(g / float(rlen[i]))
    return gaps


def n_vacuum_axes(gaps: list[float] | None, threshold: float = VACUUM_GAP_A) -> int | None:
    if gaps is None:
        return None
    return sum(1 for g in gaps if g >= threshold)


def min_distance(cell: np.ndarray, positions: np.ndarray,
                 cutoff: float = DMIN_CUTOFF_A) -> float | None:
    """Shortest interatomic distance under periodic boundary conditions (periodic images of the
    same atom included), or None when no pair is closer than ``cutoff``."""
    if len(positions) == 0 or cell_geometry(cell) is None:
        return None
    from ase.neighborlist import primitive_neighbor_list
    try:
        d = primitive_neighbor_list("d", (True, True, True), cell, positions, cutoff,
                                    self_interaction=False)
    except Exception:  # noqa: BLE001 - a degenerate cell must not stop a scan
        return None
    return float(d.min()) if len(d) else None


def space_group(cell: np.ndarray, positions: np.ndarray, numbers: list[int],
                symprec: float = SPG_SYMPREC, max_atoms: int = SPG_MAX_ATOMS) -> int | None:
    """International space-group number from spglib, or None (too big / failed / no spglib)."""
    if not 0 < len(positions) <= max_atoms:
        return None
    try:
        import spglib
        if getattr(getattr(spglib, "error", None), "OLD_ERROR_HANDLING", False):
            spglib.error.OLD_ERROR_HANDLING = False  # raise (caught below) instead of warn+None
        frac = np.mod(positions @ np.linalg.inv(cell), 1.0)
        ds = spglib.get_symmetry_dataset((cell, frac, numbers),  # type: ignore[arg-type]
                                         symprec=symprec)
    except Exception:  # noqa: BLE001
        return None
    if ds is None:
        return None
    num = getattr(ds, "number", None)
    if num is None and isinstance(ds, dict):
        num = ds.get("number")
    return int(num) if num else None


def describe(cell: np.ndarray, positions: np.ndarray, species: list[str], *,
             spg: bool = True, dmin: bool = True) -> dict[str, Any]:
    """All per-structure descriptors of one frame as a JSON-ready dict."""
    from ase.data import atomic_masses, atomic_numbers
    counts = composition(species)
    out: dict[str, Any] = {
        "natoms": len(species),
        "comp": counts,
        "formula": reduced_formula(counts),
        "chemsys": chemsys(counts),
        "nel": len(counts),
    }
    geo = cell_geometry(cell)
    if geo is not None:
        vol, rlen = geo
        mass = sum(float(atomic_masses[atomic_numbers.get(s, 0)]) for s in species)
        out["vol"] = vol
        out["rho"] = AMU_PER_A3_TO_G_CM3 * mass / vol
        out["rlen"] = [float(x) for x in rlen]
    gaps = vacuum_gaps(cell, positions)
    out["gaps"] = None if gaps is None else [round(g, 3) for g in gaps]
    if dmin:
        d = min_distance(cell, positions)
        out["dmin"] = None if d is None else round(d, 4)
    if spg:
        nums = [atomic_numbers.get(s, 0) for s in species]
        out["spg"] = space_group(cell, positions, nums)
    return out
