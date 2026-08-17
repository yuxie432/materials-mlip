"""Net magnetic moment + net charge of a VASP calculation (per-calc, cross-parser).

These are two per-calculation *derived output* scalars the harvest stores on every extxyz
frame (``atoms.info["total_magnetization"]`` / ``["total_charge"]``) and mirrors into the
metadata record (the ``electronic`` block). Both are **frame-invariant** for a calc — net
charge is exactly constant (NELECT and the atoms don't change during a run), and the net
moment is the calc's converged/final value — so the value written to each frame is one
representative per calc.

**Net magnetization** (μ_B, VASP's convention = N_up − N_down = 2·S), by availability:

* an ``OUTCAR`` is present (any parser): the final ``number of electron … magnetization …``
  line — the same line pymatgen's ``Outcar.total_mag`` reads, but as one cheap forward scan
  (no per-atom magnetization/charge tables built). Non-collinear prints three components →
  the vector norm (``magnetization_source="outcar_ncl"``).
* vasprun.xml / vaspout.h5, no OUTCAR, collinear spin-polarised (ISPIN==2): doped's
  occupancy method — ``N_up − N_down = Σ_k w_k Σ_b (occ_up − occ_down)`` from the eigenvalue
  occupancies (needs the object parsed with ``parse_eigen=True``). VASP does not write the
  total magnetization to vasprun.xml, so this reverse-engineers it (exact vs the OUTCAR value
  for collinear runs — validated in doped, mentor's reference).
  https://github.com/SMTG-Bham/doped/blob/9f08c3e1/doped/utils/parsing.py#L1370
* non-spin-polarised (ISPIN==1): ``0.0`` (``"nonmagnetic"``).
* non-collinear without an OUTCAR: ``None`` (``"unavailable_ncl"``) — a faithful estimate
  needs the projected magnetization (``parse_projected_eigen=True``, very heavy) and NCL
  direct uploads are vanishingly rare, so we record it as unavailable rather than pay that.

**Net charge** (e, = ``nelect_neutral − nelect``; positive = electron-deficient / net
positive charge), self-contained from data we already hold — no POTCAR file needed:

* ``nelect`` (actual): ``vasprun.parameters["NELECT"]`` / the OUTCAR header's ``NELECT``.
* ``nelect_neutral`` (Z_neutral = Σ ZVAL): the vasprun.xml ``<atominfo>`` ``atomtypes``
  *valence* column (pymatgen parses atominfo but discards valence, so we read it ourselves);
  the OUTCAR header's per-POTCAR ``POMASS = …; ZVAL = …`` × ``ions per type``; or, as a
  fallback (notably for vaspout.h5, which has no atominfo XML), pymatgen's bundled
  ``potcar-summary-stats.json.bz2`` table keyed by POTCAR TITEL → ZVAL.

Genuinely-unknown values stay ``None`` (never a fabricated 0): a spin calc whose OUTCAR line
is missing, or a charge whose ZVAL could not be resolved, is ``None`` with a ``…_source`` of
``"unavailable"`` — strictly better than silently wrong.

Pure logic + light file scans; unit-tested offline. pymatgen is imported lazily (only the
occupancy method and the ZVAL table need it), so this module stays importable without it.
"""

from __future__ import annotations

import logging
import math
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAGNETIZATION_UNITS = "mu_B"

# The per-SCF line VASP prints for spin-polarised / non-collinear runs, e.g.
#   number of electron     368.0000000 magnetization      12.0000041
# (three trailing values for non-collinear). Identical to pymatgen Outcar's nelect/mag regex.
_MAG_LINE_RE = re.compile(r"number of electron\s+(\S+)\s+magnetization\s+(.*)")
# OUTCAR per-POTCAR valence: "  POMASS =   55.847; ZVAL   =    8.000  mass and valenz". One
# per species, in POSCAR/POTCAR order — unambiguous (the paired POMASS;ZVAL form only appears
# in the per-POTCAR header blocks, never in the resolved-parameter prose).
_POMASS_ZVAL_RE = re.compile(r"POMASS\s*=\s*[-\d.]+\s*;\s*ZVAL\s*=\s*([-\d.]+)")
_IONS_PER_TYPE_RE = re.compile(r"ions per type\s*=\s*([\d\s]+)")
_STARTPARAM = "Startparameter for this run:"


# --- low-level helpers -----------------------------------------------------------------

def _to_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().upper() in {"T", ".TRUE.", ".T.", "TRUE", "1"}
    return False


def _opener(path: str | Path) -> Any:
    """A binary opener for a plain or gz/bz2/xz file (compression-aware)."""
    import bz2
    import gzip
    import lzma

    low = str(path).lower()
    openers: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open,
                               ".xz": lzma.open, ".lzma": lzma.open}
    return next((fn for suf, fn in openers.items() if low.endswith(suf)), open)


def _block(net_mag: float | None, mag_source: str, net_charge: float | None,
           nelect: float | None, nelect_neutral: float | None,
           charge_source: str) -> dict[str, Any]:
    """The calc-level ``electronic`` metadata block (also the source of the frame keys)."""
    return {
        "net_magnetization": net_mag,
        "magnetization_units": MAGNETIZATION_UNITS,
        "magnetization_source": mag_source,
        "net_charge": net_charge,
        "nelect": nelect,
        "nelect_neutral": nelect_neutral,
        "charge_source": charge_source if net_charge is not None else "unavailable",
    }


# --- OUTCAR magnetization line ---------------------------------------------------------

def scan_outcar_mag_line(path: str | Path) -> tuple[float | None, list[float]]:
    """Return the LAST ``number of electron X magnetization …`` line's ``(nelect, [mags])``.

    One forward pass keeping the last match (the final SCF of the last ionic step → the
    converged values). ``[mags]`` is one value for a collinear run, three (mx,my,mz) for
    non-collinear, and ``[]`` if the line never appears (a non-spin run, which omits it).
    Best-effort: an unreadable file yields ``(None, [])``.
    """
    nelect: float | None = None
    mags: list[float] = []
    try:
        with _opener(path)(path, mode="rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "magnetization" not in line:
                    continue
                m = _MAG_LINE_RE.search(line)
                if m:
                    nelect = _to_float(m.group(1))
                    vals = [_to_float(t) for t in m.group(2).split()]
                    mags = [v for v in vals if v is not None]
    except OSError as exc:
        logger.debug("OUTCAR magnetization scan failed for %s: %s", path, exc)
    return nelect, mags


def _mag_from_outcar(path: str | Path, is_spin: bool,
                     noncollinear: bool) -> tuple[float | None, str]:
    """Net moment from an OUTCAR: the last mag-line value (norm for NCL); 0 for a non-spin run
    that legitimately omits the line; None if a spin run's line could not be read."""
    _nelect, mags = scan_outcar_mag_line(path)
    if len(mags) >= 3:
        return round(math.sqrt(sum(m * m for m in mags[:3])), 6), "outcar_ncl"
    if mags:
        return round(mags[0], 6), "outcar"
    if not is_spin and not noncollinear:
        return 0.0, "nonmagnetic"          # VASP prints no magnetization line for ISPIN=1
    return None, "unavailable"             # spin/NCL run but the line was unreadable


# --- vasprun/vaspout occupancy magnetization (doped's method) --------------------------

def net_magnetization_from_object(v: Any, eigen_parsed: bool, is_spin: bool,
                                  noncollinear: bool) -> tuple[float | None, str]:
    """Net moment (μ_B) from a parsed ``Vasprun``/``Vaspout`` via eigenvalue occupancies.

    ``N_up − N_down = Σ_k w_k Σ_b occ`` (doped's method). Needs ``eigen_parsed`` (the object
    parsed with ``parse_eigen=True``). Returns ``(0.0, "nonmagnetic")`` for a non-spin run,
    ``(value, "occupancies")`` for a collinear spin run, ``(None, "unavailable_ncl")`` for
    non-collinear (needs the projected magnetization we do not parse), or ``(None,
    "unavailable")`` if the occupancies are missing.
    """
    if not is_spin:
        return (None, "unavailable_ncl") if noncollinear else (0.0, "nonmagnetic")
    if not eigen_parsed:
        return None, "unavailable"
    try:
        import numpy as np
        from pymatgen.electronic_structure.core import Spin

        eig = getattr(v, "eigenvalues", None) or {}
        if Spin.up not in eig or Spin.down not in eig:
            return None, "unavailable"
        kw = np.asarray(v.actual_kpoints_weights, dtype=float)
        up = np.asarray(eig[Spin.up], dtype=float)     # (nkpt, nband, 2): [...,1] = occupancy
        dn = np.asarray(eig[Spin.down], dtype=float)
        n_up = float(np.sum(up[:, :, 1].sum(axis=1) * kw))
        n_dn = float(np.sum(dn[:, :, 1].sum(axis=1) * kw))
        return round(n_up - n_dn, 6), "occupancies"
    except Exception as exc:  # noqa: BLE001 - occupancy shapes drift; fail to "unavailable"
        logger.debug("occupancy magnetization failed: %s", exc)
        return None, "unavailable"


# --- ZVAL / neutral electron count -----------------------------------------------------

_POTCAR_STATS: dict[str, Any] | None = None
# Try the common modern functionals first, then anything else in the table.
_STATS_FUNCTIONAL_ORDER = ("PBE_64", "PBE_54", "PBE_52", "PBE", "LDA_64", "LDA_54", "LDA_52", "LDA")


def _potcar_stats() -> dict[str, Any]:
    """pymatgen's bundled ``titel → ZVAL`` table (``potcar-summary-stats.json.bz2``), cached.

    Lets ``nelect_neutral`` be derived from a POTCAR *set* (its TITEL strings, which we always
    have) without the copyrighted POTCAR files. Empty dict if pymatgen/the file is unavailable.
    """
    global _POTCAR_STATS
    if _POTCAR_STATS is None:
        try:
            import pymatgen.io.vasp.inputs as _inp
            from monty.serialization import loadfn
            p = Path(_inp.__file__).with_name("potcar-summary-stats.json.bz2")
            loaded = loadfn(str(p))
            _POTCAR_STATS = loaded if isinstance(loaded, dict) else {}
        except Exception as exc:  # noqa: BLE001 - optional table; charge falls back to None
            logger.debug("POTCAR summary-stats table unavailable: %s", exc)
            _POTCAR_STATS = {}
    return _POTCAR_STATS


def _zval_for_titel(titel: str) -> float | None:
    """ZVAL for one POTCAR TITEL via the summary-stats table, else None (unknown titel)."""
    stats = _potcar_stats()
    if not stats or not titel:
        return None
    key = titel.replace(" ", "")
    order = list(_STATS_FUNCTIONAL_ORDER) + [f for f in stats if f not in _STATS_FUNCTIONAL_ORDER]
    for func in order:
        entries = stats.get(func, {}).get(key)
        if entries:
            z = _to_float(entries[0].get("ZVAL"))
            if z is not None:
                return z
    return None


def _species_counts(atomic_symbols: list[str] | None) -> list[int]:
    """Per-species atom counts in first-seen (POTCAR) order — the order titels are listed in."""
    import itertools
    if not atomic_symbols:
        return []
    return [len(list(g)) for _key, g in itertools.groupby(atomic_symbols)]


def neutral_nelect_from_titels(atomic_symbols: list[str] | None,
                               titels: list[str] | None) -> float | None:
    """Z_neutral = Σ (atoms-of-species × ZVAL) using the titel→ZVAL table; None if any titel is
    unresolved or the species/titel counts disagree (better None than a wrong charge)."""
    counts = _species_counts(atomic_symbols)
    if not counts or not titels or len(counts) != len(titels):
        return None
    total = 0.0
    for n, titel in zip(counts, titels):
        z = _zval_for_titel(str(titel))
        if z is None:
            return None
        total += n * z
    return total


def neutral_nelect_from_atominfo(vasprun_path: str | Path | None) -> float | None:
    """Z_neutral from a vasprun.xml ``<atominfo>`` ``atomtypes`` valence column (self-contained).

    ``atomtypes`` rows are ``[atomspertype, element, mass, valence, pseudopotential]`` — pymatgen
    keeps only symbols/titels, so we read the valence (col 3) ourselves. Targeted iterparse that
    stops at ``atomtypes`` (near the file top, long before the trajectory), so it is cheap on any
    size. None on any read/parse problem, or if atomtypes is absent (e.g. a vaspout.h5 path).
    """
    if not vasprun_path or not Path(vasprun_path).is_file():
        return None
    try:
        with _opener(vasprun_path)(vasprun_path, "rb") as fh:
            for _event, elem in ET.iterparse(fh, events=("end",)):
                if elem.tag == "array" and elem.get("name") == "atomtypes":
                    total, ok = 0.0, False
                    setel = elem.find("set")
                    for rc in (setel.findall("rc") if setel is not None else []):
                        cs = rc.findall("c")
                        if len(cs) < 4:
                            return None
                        count = _to_float((cs[0].text or "").strip())
                        val = _to_float((cs[3].text or "").strip())
                        if count is None or val is None:
                            return None
                        total += count * val
                        ok = True
                    return total if ok else None
                if elem.tag == "calculation":       # reached the trajectory: no atomtypes
                    return None
    except Exception as exc:  # noqa: BLE001 - malformed XML / truncation → charge unavailable
        logger.debug("atominfo neutral-nelect failed for %s: %s", vasprun_path, exc)
    return None


def _outcar_neutral_nelect(header_lines: list[str],
                           titels: list[str] | None) -> tuple[float | None, str]:
    """Z_neutral from OUTCAR header ``POMASS;ZVAL`` × ``ions per type`` (preferred), else the
    titel→ZVAL table with the same per-species counts. Returns ``(value, source)``."""
    pre = header_lines
    for i, ln in enumerate(header_lines):
        if _STARTPARAM in ln:
            pre = header_lines[:i]                  # ZVAL blocks sit above Startparameter
            break
    zvals = [z for ln in pre for z in _POMASS_ZVAL_RE.findall(ln)]
    zvals_f = [_to_float(z) for z in zvals]
    ions: list[int] = []
    for ln in header_lines:
        m = _IONS_PER_TYPE_RE.search(ln)
        if m:
            ions = [int(t) for t in m.group(1).split()]
            break
    if zvals_f and ions and len(zvals_f) == len(ions) and all(z is not None for z in zvals_f):
        total = sum(z * n for z, n in zip(zvals_f, ions) if z is not None)
        return total, "outcar_header"
    # Fallback: titel→ZVAL table with the ions-per-type counts (species order matches).
    if titels and ions and len(titels) == len(ions):
        total = 0.0
        for titel, n in zip(titels, ions):
            z = _zval_for_titel(str(titel))
            if z is None:
                return None, "unavailable"
            total += z * n
        return total, "potcar_stats"
    return None, "unavailable"


# --- public entry points (one per parser path) -----------------------------------------

def electronic_from_object(v: Any, *, atominfo_path: str | Path | None,
                           outcar_path: str | None, eigen_parsed: bool) -> dict[str, Any]:
    """The ``electronic`` block for a vasprun.xml / vaspout.h5 calc (``v`` the parsed object).

    Magnetization from the OUTCAR line when an OUTCAR is present, else the occupancy method.
    Net charge from ``parameters["NELECT"]`` and Z_neutral (atominfo valence for vasprun.xml,
    else the titel→ZVAL table — used for vaspout.h5, which has no atominfo XML).
    """
    params = getattr(v, "parameters", {}) or {}
    is_spin = bool(getattr(v, "is_spin", False))
    noncollinear = _as_bool(params.get("LNONCOLLINEAR")) or _as_bool(params.get("LSORBIT"))

    if outcar_path:
        net_mag, mag_src = _mag_from_outcar(outcar_path, is_spin, noncollinear)
    else:
        net_mag, mag_src = net_magnetization_from_object(v, eigen_parsed, is_spin, noncollinear)

    nelect = _to_float(params.get("NELECT"))
    neutral = neutral_nelect_from_atominfo(atominfo_path)
    charge_src = "vasprun_atominfo"
    if neutral is None:
        neutral = neutral_nelect_from_titels(getattr(v, "atomic_symbols", None),
                                             getattr(v, "potcar_symbols", None))
        charge_src = "potcar_stats"
    net_charge = round(neutral - nelect, 6) if (neutral is not None and nelect is not None) else None
    return _block(net_mag, mag_src, net_charge, nelect, neutral, charge_src)


def electronic_from_outcar(header_calc_parameters: dict[str, Any] | None,
                           header_lines: list[str], outcar_path: str) -> dict[str, Any]:
    """The ``electronic`` block for an OUTCAR-only calc.

    ``header_calc_parameters`` is :func:`outcar_params.build_calc_parameters` (gives ISPIN /
    NELECT / titels / non-collinear flags); ``header_lines`` are its source lines (reused for
    the ZVAL/ions scan, so the header is read once). Magnetization from the OUTCAR mag line;
    net charge from the header ZVAL × ions per type.
    """
    cp = header_calc_parameters or {}
    incar = cp.get("incar") or {}
    params = cp.get("parameters") or {}
    is_spin = cp.get("ispin") == 2
    noncollinear = (_as_bool(incar.get("LNONCOLLINEAR")) or _as_bool(params.get("LNONCOLLINEAR"))
                    or _as_bool(incar.get("LSORBIT")) or _as_bool(params.get("LSORBIT")))
    nelect = _to_float(params.get("NELECT"))
    if nelect is None:
        nelect = _to_float(incar.get("NELECT"))

    net_mag, mag_src = _mag_from_outcar(outcar_path, is_spin, noncollinear)
    neutral, charge_src = _outcar_neutral_nelect(header_lines, cp.get("potcar_symbols"))
    net_charge = round(neutral - nelect, 6) if (neutral is not None and nelect is not None) else None
    return _block(net_mag, mag_src, net_charge, nelect, neutral, charge_src)


def empty_block() -> dict[str, Any]:
    """An all-unavailable ``electronic`` block (used when a calc's primary is unreadable)."""
    return _block(None, "unavailable", None, None, None, "unavailable")


def is_magnetic(calc_parameters: dict[str, Any]) -> bool:
    """Whether a calc carries magnetization data — spin-polarised (ISPIN=2) OR non-collinear.

    Backs the ``availability["magnetization"]`` flag now that per-atom magmoms are no longer
    stored (so ``site_magmoms_present`` is gone): a faithful "does this calc have magnetization
    data" reading, strictly better than the old ``spin_polarized``-only test because it also
    catches non-collinear runs. Reads ISPIN via ``spin_polarized`` and LNONCOLLINEAR/LSORBIT
    from the stored INCAR / resolved ``parameters`` (both are in ``outcar_params.EFFECTIVE_TAGS``,
    so they survive on both parser paths). A single source of truth shared by :mod:`parse` and
    :mod:`outcar_recover`.
    """
    if calc_parameters.get("spin_polarized"):
        return True
    params = calc_parameters.get("parameters") or {}
    incar = calc_parameters.get("incar") or {}
    return any(_as_bool((params if key in params else incar).get(key))
               for key in ("LNONCOLLINEAR", "LSORBIT"))
