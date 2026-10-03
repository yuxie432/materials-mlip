"""Normalised views of a calc's stored ``calc_parameters`` (pure functions, no I/O).

The stored ``run_type``/``functional`` are faithful pymatgen echoes (``revPBE+Padé`` is GGA=RP,
i.e. RPBE; ``GGA`` is "no GGA tag", i.e. whatever the POTCARs are; ``HF`` is pymatgen's label for
AEXX=1, which a parameters block without AEXX also triggers). For statistics and consistency
buckets they are re-derived here from the effective parameters (``parameters`` first, user
``incar`` second) and the POTCAR titels:

* :func:`xc_family` — exchange-correlation family (PBE, PBEsol, RPBE, SCAN, R2SCAN, HSE06, ...);
* :func:`hubbard_u` / :func:`vdw_method` — the +U and dispersion parts;
* :func:`calc_type` — static / relax / relax-cell / md / phonon / neb / saddle / mlff / nscf;
* :func:`potcar_libraries` — which VASP POTCAR releases contain every titel of the calc;
* :func:`mp_compatible` — whether the settings match the Materials Project GGA/GGA+U recipe
  (MPRelaxSet POTCAR symbols in the PBE release, MP U values for O/F compounds, no vdW/meta/hybrid),
  i.e. whether its energies share MPtrj's reference after the MP2020 corrections.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from .structure import element_of

_OFF = {"", "--", "NONE", "F", "FALSE", ".FALSE.", "0", "NO"}


def _get(cp: dict, key: str, incar_first: bool = False) -> Any:
    blocks = [cp.get("parameters") or {}, cp.get("incar") or {}]
    if incar_first:
        blocks.reverse()
    for blk in blocks:
        if isinstance(blk, dict) and blk.get(key) is not None:
            return blk[key]
    return None


def as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().upper().lstrip(".").startswith(("T", "Y"))


def as_float(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (list, tuple)):
        return as_float(v[0]) if v else None
    try:
        return float(v)
    except (TypeError, ValueError):
        try:
            return float(str(v).split()[0])
        except (ValueError, IndexError):
            return None


def as_int(v: Any) -> int | None:
    f = as_float(v)
    return None if f is None else int(round(f))


def as_floats(v: Any) -> list[float]:
    if v is None:
        return []
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return [float(v)]
    if isinstance(v, str):
        v = v.split()
    out = []
    for x in v if isinstance(v, (list, tuple)) else []:
        f = as_float(x)
        if f is not None:
            out.append(f)
    return out


def potcar_symbols(cp: dict) -> list[str]:
    """POTCAR symbols in species order (``PAW_PBE Fe_pv 06Sep2000`` -> ``Fe_pv``)."""
    out = []
    for t in cp.get("potcar_symbols") or []:
        parts = str(t).split()
        out.append(parts[1] if len(parts) > 1 else parts[0] if parts else "")
    return out


def potcar_elements(cp: dict) -> list[str]:
    return [element_of(s) for s in potcar_symbols(cp)]


# --- exchange-correlation ---------------------------------------------------------------------
_GGA = {"PE": "PBE", "PS": "PBEsol", "RP": "RPBE", "RE": "revPBE", "91": "PW91", "AM": "AM05",
        "CA": "LDA", "PZ": "LDA", "OR": "optPBE-vdW", "BO": "optB88-vdW", "MK": "optB86b-vdW",
        "ML": "vdW-DF2", "BF": "BEEF-vdW", "CX": "vdW-DF-cx", "B3": "B3LYP", "B5": "B3LYP",
        "SO": "SOGGA", "LI": "LIBXC", "LIBXC": "LIBXC"}
_META = {"SCAN": "SCAN", "R2SCAN": "R2SCAN", "RSCAN": "rSCAN", "TPSS": "TPSS",
         "RTPSS": "revTPSS", "M06L": "M06-L", "MBJ": "mBJ", "MS0": "MS0", "MS1": "MS1",
         "MS2": "MS2", "R2SCANL": "r2SCAN-L", "R2SL": "r2SCAN-L", "SCANL": "SCAN-L"}


def _tag(v: Any) -> str:
    """A string INCAR tag as VASP reads it: the first token, without an inline comment or the
    dots of a Fortran-style logical typo (``"BF   ! BEEF"`` -> ``BF``, ``".pe."`` -> ``PE``)."""
    tok = re.split(r"[\s!#(]", str(v).strip(), maxsplit=1)[0]
    return tok.strip(".").upper()


def xc_family(cp: dict) -> str:
    """Exchange-correlation family from the effective parameters (+ POTCAR fallback)."""
    if as_bool(_get(cp, "LHFCALC")):
        hfs = as_float(_get(cp, "HFSCREEN")) or 0.0
        aexx = as_float(_get(cp, "AEXX"))
        gga = _tag(_get(cp, "GGA", incar_first=True) or "")
        if hfs > 0.25:
            return "HSE03"
        if hfs > 0.0:
            return "HSE06"
        if gga in ("B3", "B5"):
            return "B3LYP"
        if aexx is not None and abs(aexx - 1.0) < 1e-6:
            return "HF"
        return "PBE0/hybrid"
    metagga = _get(cp, "METAGGA", incar_first=True)
    if not isinstance(metagga, bool) and metagga is not None and _tag(metagga) not in _OFF:
        tag = _tag(metagga)
        return _META.get(tag, tag)
    gga = _get(cp, "GGA", incar_first=True)
    if gga is not None and not isinstance(gga, bool) and _tag(gga) not in _OFF:
        tag = _tag(gga)
        return _GGA.get(tag) or _GGA.get(tag[:2]) or f"GGA={tag}"
    titels = [str(t) for t in cp.get("potcar_symbols") or []]
    if titels:
        head = titels[0].split()[0].upper() if titels[0].split() else ""
        if head.startswith("PAW_PBE"):
            return "PBE"
        if head.startswith("PAW_GGA") or head.startswith("PAW_PW91"):
            return "PW91"
        if head in ("PAW", "PAW_LDA") or head.startswith("PAW_LDA"):
            return "LDA"
        if head.startswith("US"):
            return "LDA-US"
    return "unknown"


def hubbard_u(cp: dict) -> dict[str, float]:
    """``{element: U}`` for the species with a non-zero U (U_eff = U - J), empty if no +U."""
    if not as_bool(_get(cp, "LDAU")):
        return {}
    uu = as_floats(_get(cp, "LDAUU"))
    jj = as_floats(_get(cp, "LDAUJ"))
    els = potcar_elements(cp)
    out: dict[str, float] = {}
    for i, el in enumerate(els):
        if i < len(uu):
            u = uu[i] - (jj[i] if i < len(jj) else 0.0)
            if abs(u) > 1e-6:
                out[el] = round(u, 3)
    return out


_IVDW = {1: "D2", 10: "D2", 11: "D3", 12: "D3-BJ", 13: "D4", 2: "TS", 20: "TS", 21: "TS-H",
         202: "MBD", 263: "MBD@rsSCS", 4: "dDsC"}


def vdw_method(cp: dict) -> str:
    """Dispersion treatment: none / D2 / D3 / D3-BJ / TS / ... / nonlocal (LUSE_VDW: vdW-DF, rVV10)."""
    if as_bool(_get(cp, "LUSE_VDW")):
        return "nonlocal"
    incar = cp.get("incar") or {}
    ivdw = as_int(incar.get("IVDW"))  # user-set only: the effective block always echoes IVDW=0
    if ivdw:
        return _IVDW.get(ivdw, f"IVDW={ivdw}")
    if as_bool(incar.get("LVDW")):
        return "D2"
    return "none"


def functional_label(cp: dict) -> str:
    """``family[+U][+vdW]`` — the bucket label used for consistency groups."""
    lab = xc_family(cp)
    if hubbard_u(cp):
        lab += "+U"
    vdw = vdw_method(cp)
    if vdw != "none":
        lab += f"+{vdw}"
    return lab


# --- calculation type -------------------------------------------------------------------------
def calc_type(cp: dict) -> str:
    incar = cp.get("incar") or {}
    kp = cp.get("kpoints") or {}
    if as_bool(incar.get("ML_LMLFF")) or str(incar.get("ML_MODE", "")).strip().lower() in (
            "train", "run", "select", "refit", "refitbayesian"):
        return "mlff"
    ichg = as_int(incar.get("ICHARG"))
    if (ichg is not None and ichg >= 10) or str(kp.get("style", "")).lower().startswith("line"):
        return "nscf"
    images = as_int(incar.get("IMAGES"))
    if images is not None and images > 0:
        return "neb"
    ichain = as_int(incar.get("ICHAIN"))
    if ichain in (1, 2, 3):
        return "saddle"
    ibrion = as_int(_get(cp, "IBRION"))
    nsw = as_int(_get(cp, "NSW")) or 0
    isif = as_int(_get(cp, "ISIF"))
    if nsw <= 0 or (ibrion == -1 and nsw <= 1):
        return "static"
    if ibrion is None:
        ibrion = 0 if nsw > 1 else -1  # VASP default
    if ibrion == 0:
        return "md"
    if ibrion in (1, 2, 3):
        return "relax-cell" if isif is not None and isif >= 3 else "relax"
    if ibrion in (5, 6, 7, 8):
        return "phonon"
    if ibrion == 44:
        return "saddle"
    return "other"


def kpoint_summary(cp: dict) -> tuple[str, tuple[int, int, int] | None, float | None, int | None]:
    """(style, explicit k-grid or None, KSPACING or None, number of irreducible k-points or None).

    A grid is returned for a KPOINTS file giving one triple of positive integers (Gamma /
    Monkhorst / "Automatic" read back with its subdivisions); a fully-automatic length (one number)
    or an explicit list/line-mode gives None."""
    kp = cp.get("kpoints") or {}
    style = str(kp.get("style") or "none")
    kpts = kp.get("kpts")
    grid = None
    if isinstance(kpts, list) and len(kpts) == 1 and isinstance(kpts[0], (list, tuple)) \
            and len(kpts[0]) == 3:
        try:
            g = [int(round(float(x))) for x in kpts[0]]
            if all(0 < x < 10000 for x in g):
                grid = (g[0], g[1], g[2])
        except (TypeError, ValueError):
            grid = None
    # KSPACING only as the user set it: the effective block echoes the 0.5 default even when a
    # KPOINTS file was used.
    ksp = as_float(kp.get("kspacing")) or as_float((cp.get("incar") or {}).get("KSPACING"))
    nk = as_int(kp.get("num_kpts"))
    return style, grid, ksp, nk


# --- POTCAR libraries + Materials Project compatibility --------------------------------------
_LIBS = ("PBE", "PBE_52", "PBE_54", "PBE_64", "LDA", "LDA_52", "LDA_54", "LDA_64", "PW91",
         "LDA_US", "PW91_US")
LIB_BITS = {"PBE": 1, "PBE_52": 2, "PBE_54": 4, "PBE_64": 8, "LDA*": 16, "PW91*": 32, "US": 64,
            "unknown_titel": 128}


@lru_cache(maxsize=1)
def _titel_libraries() -> dict[str, frozenset[str]]:
    """POTCAR TITEL (spaces removed) -> the VASP releases that contain it (pymatgen's table)."""
    try:
        import bz2

        import pymatgen.io.vasp.inputs as inp
        p = Path(inp.__file__).with_name("potcar-summary-stats.json.bz2")
        stats = json.load(bz2.open(p))
    except Exception:  # noqa: BLE001 - table unavailable: every titel reads "unknown"
        return {}
    out: dict[str, set[str]] = {}
    for lib in _LIBS:
        for key in stats.get(lib, {}):
            out.setdefault(key, set()).add(lib)
    return {k: frozenset(v) for k, v in out.items()}


def potcar_libraries(cp: dict) -> int:
    """Bitmask (:data:`LIB_BITS`) of the POTCAR releases containing EVERY titel of the calc."""
    table = _titel_libraries()
    titels = [str(t).replace(" ", "") for t in cp.get("potcar_symbols") or []]
    if not titels or not table:
        return LIB_BITS["unknown_titel"]
    common: set[str] | None = None
    unknown = False
    for t in titels:
        libs = table.get(t)
        if libs is None:
            unknown = True
            continue
        common = set(libs) if common is None else common & libs
    bits = LIB_BITS["unknown_titel"] if unknown else 0
    for lib in common or ():
        if lib in ("PBE", "PBE_52", "PBE_54", "PBE_64"):
            bits |= LIB_BITS[lib]
        elif lib.startswith("LDA_US") or lib.endswith("_US"):
            bits |= LIB_BITS["US"]
        elif lib.startswith("LDA"):
            bits |= LIB_BITS["LDA*"]
        elif lib.startswith("PW91"):
            bits |= LIB_BITS["PW91*"]
    return bits


@lru_cache(maxsize=1)
def _mp_recipe() -> tuple[dict[str, str], dict[str, dict[str, float]]] | None:
    try:
        from pymatgen.io.vasp.sets import MPRelaxSet
        cfg = MPRelaxSet.CONFIG
        pot = {str(k): str(v) for k, v in cfg["POTCAR"].items()}
        uu = {str(a): {str(e): float(u) for e, u in t.items()}
              for a, t in cfg["INCAR"]["LDAUU"].items()}
        return pot, uu
    except Exception:  # noqa: BLE001
        return None


# OMat24 used the MP recipe with the PBE_54 release, W_sv and Yb_3 (arXiv:2410.12771).
_OMAT_SYMBOL_OVERRIDES = {"W": "W_sv", "Yb": "Yb_3"}


def mp_compatible(cp: dict, *, release: str = "PBE",
                  symbol_overrides: dict[str, str] | None = None) -> int:
    """1 if the calc follows the Materials Project GGA(+U) recipe (POTCAR symbols of MPRelaxSet in
    ``release``, MP U values on O/F compounds and no U otherwise, PBE exchange-correlation without
    vdW / meta-GGA / hybrid), 0 if not, -1 if undeterminable (no POTCAR info / no pymatgen)."""
    recipe = _mp_recipe()
    syms = potcar_symbols(cp)
    if recipe is None or not syms:
        return -1
    pot, uu = recipe
    if symbol_overrides:
        pot = {**pot, **symbol_overrides}
    if xc_family(cp) != "PBE" or vdw_method(cp) != "none":
        return 0
    titels = [str(t).replace(" ", "") for t in cp.get("potcar_symbols") or []]
    table = _titel_libraries()
    for el, sym, t in zip(potcar_elements(cp), syms, titels):
        if pot.get(el) != sym:
            return 0
        if table and release not in table.get(t, frozenset()):
            return 0
    els = set(potcar_elements(cp))
    anion = "F" if "F" in els else "O" if "O" in els else None
    want = {el: u for el, u in (uu.get(anion, {}) if anion else {}).items() if el in els}
    have = hubbard_u(cp)
    if set(want) != set(have):
        return 0
    if any(abs(want[el] - have[el]) > 0.011 for el in want):
        return 0
    return 1


def omat24_compatible(cp: dict) -> int:
    return mp_compatible(cp, release="PBE_54", symbol_overrides=_OMAT_SYMBOL_OVERRIDES)
