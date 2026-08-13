"""Parse a VASP ``OUTCAR`` **header** into ``calc_parameters`` (parity with the vasprun path).

ASE's ``vasp-out`` reader gives us an OUTCAR's ionic *trajectory* (positions / energy /
forces / stress per step) but never touches the OUTCAR *header*, where VASP echoes the
run's parameters. So an OUTCAR-only calc historically landed in the dataset with
``run_type`` / ``functional`` / INCAR all ``null`` — 25.6 % of harvested frames with an
unknown functional, unusable for a functional-consistent MLIP training subset (see
docs/EVALUATION.md and the ``metadata-gaps-and-param-resolver`` memory). This module closes
that gap by reading only the header (everything before the first ionic step) and returning a
dict shaped **exactly** like :func:`zenodo_harvest.parse._calc_parameters` (the vasprun path),
so downstream curation is uniform across parsers.

VASP prints TWO INCAR-equivalent regions in the header, and we keep both — mirroring how the
vasprun path stores ``vasprun.incar`` (user-set) alongside ``vasprun.parameters`` (resolved):

* the ``INCAR:`` echo — a verbatim copy of the user's INCAR file (only the tags the user set,
  the direct analog of ``vasprun.incar``) -> stored under ``incar``;
* the resolved-parameter blocks (``Startparameter for this run:`` / ``Electronic Relaxation``
  / ``Ionic relaxation`` / ``DOS related values:`` / ``Exchange correlation treatment:`` /
  ...) — VASP's *effective* values with defaults already filled (the analog of
  ``vasprun.parameters``) -> stored under ``parameters``.

``run_type`` / ``functional`` are then classified from the effective values by
:func:`classify_run_type`, which mirrors pymatgen 2026.5.4's ``Vasprun.run_type`` EXACTLY —
same ``GGA``/``METAGGA`` tables, same hybrid ``AEXX``/``HFSCREEN`` tests, same ``+U`` and
``+vdW`` suffixes — so an OUTCAR-parsed PBE run gets the identical label a vasprun-parsed one
would (measured: pymatgen already labels 45 k vasprun calcs as bare ``"GGA"`` when ``GGA=--``;
the two parsers MUST agree or the ``frames_by_run_type`` curation groups fragment by parser).

The one subtlety that makes this safe: pymatgen's tests read
``parameters.get("AEXX", 1.0)`` / ``parameters.get("LDAU", True)`` with defaults that are only
harmless when the parameters dict is COMPLETE (a real vasprun always carries them). VASP does
not always print those lines (e.g. no HF block on a plain GGA run), so
:func:`_classify_inputs` guarantees ``AEXX``/``HFSCREEN``/``LHFCALC``/``LDAU``/``GGA``/
``LUSE_VDW`` are present (VASP's non-feature defaults when the header omitted them) — without
which a plain PBE OUTCAR would be mislabeled ``HF`` or ``PBE+U``.

Pure text logic: no network, no pymatgen import — unit-testable offline against header
fixtures. The only heavy dependency (a gzip/bz2/xz OUTCAR) is handled by the stdlib.
"""

from __future__ import annotations

import bz2
import gzip
import logging
import lzma
import math
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# The header ends at the first ionic step. Real headers are ~1k lines; the cap bounds a
# pathological / never-started OUTCAR so we never read a multi-GB body looking for a marker.
_HEADER_LINE_CAP = 20_000
_ITERATION_RE = re.compile(r"Iteration\s+1\(\s*1\s*\)")  # "----- Iteration    1(   1) -----"
_STARTPARAM_RE = re.compile(r"Startparameter for this run:")

# ``TAG = value`` where the value is a single token (stops at whitespace, ``;`` or the
# trailing description VASP appends). Used to scan the resolved-parameter region; a line may
# pack several (``NELM = 299;  NELMIN= 3;  NELMDL= -5``) and finditer catches each.
_EFF_ASSIGN_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^\s;]+)")

# The effective (resolved) tags we lift out of the parameter blocks. A curated allow-list
# (superset of the user's requested set) so the stored ``parameters`` stays meaningful rather
# than swallowing every ``TAG=`` token in the descriptive prose. Absence of a tag here just
# means "not recorded", never "off". Public: the vasprun path (:mod:`parse`) stores the SAME
# subset of ``vasprun.parameters`` so both parsers' ``parameters`` blocks share a schema and
# stay a bounded size.
EFFECTIVE_TAGS = frozenset({
    # electronic / accuracy
    "PREC", "ENCUT", "ENINI", "ENAUG", "EDIFF", "NELM", "NELMIN", "NELMDL", "ALGO", "IALGO",
    "LREAL", "ADDGRID", "LASPH", "LMAXMIX", "LMAXPAW", "GGA_COMPAT", "ISYM", "ISPIN",
    "LNONCOLLINEAR", "LSORBIT", "NBANDS", "NELECT", "NUPDOWN", "ISMEAR", "SIGMA",
    # ionic / cell
    "IBRION", "NSW", "ISIF", "POTIM", "EDIFFG", "NFREE", "PSTRESS", "SMASS",
    # exchange-correlation / hybrids
    "GGA", "METAGGA", "LHFCALC", "LHFONE", "AEXX", "AGGAX", "AGGAC", "ALDAC", "HFSCREEN",
    "HFSCREENC", "ENCUTFOCK", "PRECFOCK", "NKRED", "NKREDX", "NKREDY", "NKREDZ", "LDAU",
    "LDAUTYPE", "LDAUL", "LDAUU", "LDAUJ", "LSPINORB", "LIBXC",
    # dispersion / dipole
    "IVDW", "LUSE_VDW", "LVDW", "LDIPOL", "IDIPOL", "LMONO",
    # output flags (availability cross-checks)
    "LWAVE", "LCHARG", "LVTOT", "LVHAR", "LELF", "LORBIT", "LAECHG",
    # k-points
    "KSPACING", "KGAMMA",
})

# ---------------------------------------------------------------------------------------
# pymatgen 2026.5.4 Vasprun.run_type tables, copied verbatim so the OUTCAR path classifies
# identically to the vasprun path. Keep in sync with pymatgen on upgrade (a mismatch only
# mislabels functional/run_type, which `verify`'s frames_by_run_type would surface).
# ---------------------------------------------------------------------------------------
_GGA_TYPES = {
    "RE": "revPBE", "PE": "PBE", "PS": "PBEsol", "RP": "revPBE+Padé", "AM": "AM05",
    "OR": "optPBE", "BO": "optB88", "MK": "optB86b", "--": "GGA",
}
_METAGGA_TYPES = {
    "TPSS": "TPSS", "RTPSS": "revTPSS", "M06L": "M06-L", "MBJ": "modified Becke-Johnson",
    "SCAN": "SCAN", "R2SCAN": "R2SCAN", "RSCAN": "RSCAN", "MS0": "MadeSimple0",
    "MS1": "MadeSimple1", "MS2": "MadeSimple2",
}
_IVDW_TYPES = {
    0: "no-correction", 1: "DFT-D2", 10: "DFT-D2", 11: "DFT-D3", 12: "DFT-D3-BJ",
    2: "TS", 20: "TS", 21: "TS-H", 202: "MBD", 4: "dDsC",
}


# --- low-level value coercion ----------------------------------------------------------

_BOOL_TOKENS = {".TRUE.": True, "T": True, ".FALSE.": False, "F": False,
                ".T.": True, ".F.": False}


def _coerce_scalar(tok: str) -> Any:
    """One OUTCAR/INCAR token -> bool / int / float / str (Fortran ``1.E-8`` handled)."""
    t = tok.strip()
    if not t:
        return None
    up = t.upper()
    if up in _BOOL_TOKENS:
        return _BOOL_TOKENS[up]
    try:
        return int(t)
    except ValueError:
        pass
    try:
        # Fortran floats: ``0.1E-07``, ``1.E-8``, ``-.1E-04`` all parse in Python once E->e.
        return float(t.replace("E", "e").replace("D", "e"))
    except ValueError:
        return t


def _coerce_value(val: str) -> Any:
    """A whole INCAR value (may be a list like ``LDAUU = 4.0 0.0``, or a string with spaces)."""
    val = val.strip()
    if not val:
        return None
    toks = val.split()
    coerced = [_coerce_scalar(t) for t in toks]
    if len(coerced) == 1:
        return coerced[0]
    if all(isinstance(c, (int, float, bool)) for c in coerced):
        return coerced           # numeric array (LDAUL/LDAUU/LDAUJ/MAGMOM-as-numbers)
    return val                   # e.g. "unknown system" — keep the raw string


def _as_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().upper() in {"T", ".TRUE.", ".T.", "TRUE", "1"}
    return False


# --- header reading --------------------------------------------------------------------

def _open_text(path: str | Path) -> Any:
    """Open a plain or gzip/bz2/xz OUTCAR as decoded text (errors replaced, never raised)."""
    low = str(path).lower()
    openers: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open,
                               ".xz": lzma.open, ".lzma": lzma.open}
    opener = next((fn for suf, fn in openers.items() if low.endswith(suf)), None)
    if opener is None:
        return open(path, encoding="utf-8", errors="replace")
    return opener(path, mode="rt", encoding="utf-8", errors="replace")


def read_header_lines(path: str | Path, line_cap: int = _HEADER_LINE_CAP) -> list[str]:
    """Return the OUTCAR header lines (up to, not including, the first ionic step).

    Stops at the first ``Iteration    1(   1)`` marker or ``line_cap`` lines, whichever comes
    first, so we never stream a multi-GB trajectory just to read parameters. The last few
    header lines echo the k-point list, which can be long for a dense mesh; the cap is set
    well above any real header.
    """
    lines: list[str] = []
    with _open_text(path) as fh:
        for i, raw in enumerate(fh):
            if _ITERATION_RE.search(raw):
                break
            lines.append(raw.rstrip("\n"))
            if i >= line_cap:
                break
    return lines


# --- region parsers --------------------------------------------------------------------

def _incar_echo_lines(lines: list[str]) -> list[str]:
    """The lines of the verbatim ``INCAR:`` echo block (between ``INCAR:`` and ``POTCAR:``)."""
    block: list[str] = []
    in_block = False
    for line in lines:
        stripped = line.strip()
        if not in_block:
            if stripped.startswith("INCAR:"):
                in_block = True
            continue
        if stripped.startswith("POTCAR:"):
            break                                    # end of the INCAR echo
        block.append(line)
    return block


def _incar_from_lines_stdlib(block: list[str]) -> dict[str, Any]:
    """Stdlib fallback INCAR-echo parser (used only when pymatgen is unavailable).

    Tolerates ``#``/``!`` comments and ``;``-separated multi-tag lines. Less complete than
    pymatgen's ``Incar`` (no ``N*x`` repeat expansion), but keeps this module usable — and
    its offline tests runnable — without a pymatgen import.
    """
    incar: dict[str, Any] = {}
    for line in block:
        body = re.split(r"[#!]", line, maxsplit=1)[0]   # drop trailing comment
        for part in body.split(";"):                 # several tags may share a line
            m = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*\S)\s*$", part)
            if m:
                incar[m.group(1).upper()] = _coerce_value(m.group(2))
    return incar


def _jsonable_incar(v: Any) -> Any:
    """Coerce a pymatgen ``Incar`` value to JSON-safe Python (primitives/lists; else str)."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, (list, tuple)):
        return [_jsonable_incar(x) for x in v]
    return str(v)


def _parse_incar_echo(lines: list[str]) -> dict[str, Any]:
    """The verbatim ``INCAR:`` echo -> ``{TAG: value}`` (user-set tags; analog of vasprun.incar).

    The block is literally the run's INCAR file, so it is parsed by pymatgen's **canonical**
    ``Incar`` parser (``pymatgen.io.vasp.inputs.Incar.from_str``) — the same code that reads a
    real INCAR — which is the robust way to handle VASP's flexible INCAR syntax the user
    flagged: proper per-tag typing (logical/int/float/string), quoted/spaced string values, and
    the ``N*x`` repeat expansion (``MAGMOM = 3*0.6 2*0.0`` -> ``[0.6, 0.6, 0.6, 0.0, 0.0]``).
    pymatgen is imported lazily so this module stays importable (and unit-testable) without it;
    on ImportError or any parse error we fall back to :func:`_incar_from_lines_stdlib`. Absent
    ``INCAR:`` echo (some edited/old OUTCARs) -> ``{}``.
    """
    block = _incar_echo_lines(lines)
    if not block:
        return {}
    try:
        from pymatgen.io.vasp.inputs import Incar
        text = "\n".join(ln.strip() for ln in block if ln.strip())
        parsed = Incar.from_str(text)
        return {str(k).upper(): _jsonable_incar(v) for k, v in parsed.items()}
    except Exception:                                # noqa: BLE001 - any pymatgen issue -> stdlib
        return _incar_from_lines_stdlib(block)


def _parse_effective(lines: list[str]) -> dict[str, Any]:
    """The resolved-parameter blocks -> ``{TAG: value}`` (VASP effective values; analog of
    vasprun.parameters), restricted to :data:`_EFFECTIVE_TAGS`.

    Scanned from ``Startparameter for this run:`` onward so it never picks up the raw INCAR
    echo or the per-POTCAR blocks above it. First occurrence of each tag wins (the parameter
    echo prints each once; later mentions in prose are ignored).
    """
    start = next((i for i, ln in enumerate(lines) if _STARTPARAM_RE.search(ln)), None)
    if start is None:
        return {}
    eff: dict[str, Any] = {}
    for line in lines[start:]:
        for m in _EFF_ASSIGN_RE.finditer(line):
            key = m.group(1).upper()
            if key in EFFECTIVE_TAGS and key not in eff:
                eff[key] = _coerce_scalar(m.group(2))
    return eff


def _titels_from_lines(lines: list[str]) -> list[str]:
    """Ordered, de-duplicated POTCAR titel strings (species order == POSCAR order).

    Prefers ``TITEL  = PAW_PBE Fe_pv 06Sep2000`` lines (VASP 5.4+/6.x — the same source as
    :func:`zenodo_harvest.parse._outcar_potcar_titels`). Older VASP (<=5.3) prints no ``TITEL``
    line, only the ``POTCAR:    PAW_PBE Ni 02Aug2007`` summary (each species listed, often
    duplicated), so we fall back to those — deduplicated first-seen to preserve species order.
    """
    titels: list[str] = []
    for line in lines:
        if "TITEL" in line and "=" in line:
            t = line.split("=", 1)[1].strip()
            if t and t not in titels:
                titels.append(t)
    if titels:
        return titels
    for line in lines:                               # old-VASP fallback: `POTCAR:  <titel>`
        stripped = line.strip()
        if stripped.startswith("POTCAR:"):
            t = stripped[len("POTCAR:"):].strip()
            if t and t not in titels:
                titels.append(t)
    return titels


def _potcar_lexch(lines: list[str]) -> list[str]:
    """POTCAR-block ``LEXCH`` strings (e.g. ``PE`` for PBE) — the POTCAR's own XC family.

    Only the per-POTCAR blocks (above ``Startparameter``) carry a *string* LEXCH; the
    resolved block's ``LEXCH = 8`` is an unrelated integer code, so we stop at
    ``Startparameter``. Useful when ``GGA = --`` leaves ``run_type`` as the bare ``"GGA"``:
    ``LEXCH = PE`` still pins the POTCAR family as PBE. A hint only — never overrides the
    pymatgen-faithful ``run_type``/``functional``.
    """
    out: list[str] = []
    for line in lines:
        if _STARTPARAM_RE.search(line):
            break
        m = re.search(r"\bLEXCH\s*=\s*([A-Za-z0-9]+)", line)
        if m:
            val = m.group(1).strip()
            if val and not val.isdigit() and val not in out:
                out.append(val)
    return out


def _parse_ldau(lines: list[str], incar: dict, eff: dict) -> tuple[bool, dict | None]:
    """``(ldau_on, hubbard_u)`` from the OUTCAR.

    LDA+U is signalled either by an explicit ``LDAU`` tag or by VASP's ``LDA+U is selected``
    block; the U/J arrays come from the INCAR echo (``LDAUU = 4.0 0.0``) or that block.
    ``hubbard_u`` mirrors the vasprun path: ``{LDAUTYPE, LDAUL, LDAUU, LDAUJ}`` (only the keys
    found), or ``None`` when +U is off.
    """
    text = "\n".join(lines)
    ldau_on = _as_bool(incar.get("LDAU")) or _as_bool(eff.get("LDAU")) \
        or ("LDA+U is selected" in text)
    if not ldau_on:
        return False, None
    hub: dict[str, Any] = {}
    for key in ("LDAUTYPE", "LDAUL", "LDAUU", "LDAUJ"):
        if key in incar and incar[key] is not None:
            hub[key] = incar[key]                    # pymatgen already typed it (proper list)
        else:
            # OUTCAR echo, e.g. "... LDAUTYPE =  2" or the multi-species
            # "U (eV)  for each species LDAUU =  4.00 0.00". Capture to end of line (dropping a
            # trailing comment) so a whole per-species ARRAY is kept, not just its first value.
            m = re.search(rf"\b{key}\s*=\s*(.+)$", text, re.MULTILINE)
            if m:
                hub[key] = _coerce_value(re.split(r"[#!]", m.group(1), maxsplit=1)[0].strip())
    return True, (hub or None)


# --- run_type / functional classification (pymatgen-faithful) --------------------------

def _classify_inputs(incar: dict, eff: dict, ldau_on: bool) -> dict[str, Any]:
    """Assemble a *complete* effective-parameter view for :func:`classify_run_type`.

    pymatgen reads AEXX/HFSCREEN/LHFCALC/LDAU/GGA/LUSE_VDW from ``parameters`` with defaults
    that only stay harmless when those keys are present (a real vasprun always carries them).
    VASP omits e.g. the HF block on a plain GGA run, so we fill VASP's non-feature defaults
    here — otherwise ``.get("AEXX", 1.0)`` would read 1.0 and mislabel a PBE run as ``HF``,
    and ``.get("LDAU", True)`` would append a spurious ``+U``. Effective (resolved) values win
    over the user echo for those tags.

    **``IVDW`` is read from the user INCAR echo ONLY**, exactly as pymatgen reads
    ``self.incar.get("IVDW")`` — NOT from the effective block. VASP echoes the resolved default
    ``IVDW = 0`` in the effective parameters even when the user requested no dispersion, and
    ``0`` is a member of pymatgen's ``IVDW_TYPES`` ("no-correction"), so reading it from the
    effective block would append a spurious ``+vdW-no-correction`` to essentially every run
    (caught by the real-OUTCAR cross-check against pymatgen's vasprun ``run_type``). ``METAGGA``
    prefers the echo and falls back to the effective value — its non-metaGGA effective form is
    ``--``/``F`` (handled as "off"), so the fallback only ever *recovers* a real metaGGA on an
    OUTCAR whose INCAR echo was empty, never invents one.
    """
    def pick(key: str, default: Any) -> Any:
        if key in eff and eff[key] is not None:
            return eff[key]
        if key in incar and incar[key] is not None:
            return incar[key]
        return default

    metagga = incar.get("METAGGA")
    if metagga is None:
        metagga = eff.get("METAGGA")
    return {
        "AEXX": _as_float(pick("AEXX", 0.0), 0.0),
        "HFSCREEN": _as_float(pick("HFSCREEN", 0.0), 0.0),
        "LHFCALC": _as_bool(pick("LHFCALC", False)),
        "LDAU": ldau_on,
        "GGA": pick("GGA", None),
        "METAGGA": metagga,
        "IVDW": incar.get("IVDW"),      # user-set only (see docstring); eff has default 0
        "LUSE_VDW": _as_bool(pick("LUSE_VDW", False)),
    }


def classify_run_type(p: dict, potcar_symbols: list[str]) -> str:
    """``run_type`` from a *complete* effective-parameter view ``p`` (see :func:`_classify_inputs`).

    A faithful re-implementation of pymatgen 2026.5.4 ``Vasprun.run_type`` (tables copied
    above). ``functional`` downstream is ``run_type.split("+")[0]``, exactly as the vasprun
    path derives it.
    """
    aexx = _as_float(p.get("AEXX"), 1.00)
    hfscreen_30 = _as_float(p.get("HFSCREEN"), 0.30)
    hfscreen_20 = _as_float(p.get("HFSCREEN"), 0.20)
    aexx_20 = _as_float(p.get("AEXX"), 0.20)
    metagga = p.get("METAGGA")
    gga = p.get("GGA")

    if math.isclose(aexx, 1.00):
        run_type = "HF"
    elif math.isclose(hfscreen_30, 0.30):
        run_type = "HSE03"
    elif math.isclose(hfscreen_20, 0.20):
        run_type = "HSE06"
    elif math.isclose(aexx_20, 0.20):
        run_type = "B3LYP"
    elif _as_bool(p.get("LHFCALC")):
        run_type = "PBEO or other Hybrid Functional"
    elif metagga and str(metagga).upper() not in {"--", "NONE", "F", ".FALSE."}:
        tag = str(metagga).upper()
        run_type = _METAGGA_TYPES.get(tag, tag)
    elif gga:
        tag = str(gga).upper()
        run_type = _GGA_TYPES.get(tag, tag)
    elif potcar_symbols and potcar_symbols[0].split()[0] == "PAW":
        run_type = "LDA"
    else:
        run_type = "unknown"

    if _as_bool(p.get("LDAU")):
        run_type += "+U"
    if _as_bool(p.get("LUSE_VDW")):
        run_type += "+rVV10"
    else:
        ivdw = p.get("IVDW")
        try:
            ivdw_int = int(ivdw) if ivdw is not None else None
        except (TypeError, ValueError):
            ivdw_int = None
        if ivdw_int in _IVDW_TYPES:
            run_type += f"+vdW-{_IVDW_TYPES[ivdw_int]}"
        elif ivdw_int:
            run_type += "+vdW-unknown"
    return run_type


# --- k-points --------------------------------------------------------------------------

def _parse_kpoints(lines: list[str], incar: dict, eff: dict) -> dict[str, Any]:
    """Best-effort k-point mesh from the OUTCAR header (schema parity with the vasprun path).

    ``num_kpts`` (irreducible count) is reliable (``NKPTS = N``); the generating grid comes
    from ``generate k-points for:  a b c`` when VASP echoes an automatic mesh; ``kspacing`` is
    read from the INCAR when KSPACING-based. The exact Monkhorst/Gamma centring is not always
    unambiguous in the OUTCAR, so ``style`` is best-effort and may be ``None`` — the values
    that matter for consistency (grid density / kspacing / count) are captured.
    """
    text = "\n".join(lines)
    num_kpts = None
    m = re.search(r"NKPTS\s*=\s*(\d+)", text)
    if m:
        num_kpts = int(m.group(1))
    kpts: list | None = None
    m = re.search(r"generate k-points for:\s*(\d+)\s+(\d+)\s+(\d+)", text)
    if m:
        kpts = [[int(m.group(1)), int(m.group(2)), int(m.group(3))]]
    style = None
    if re.search(r"Gamma-point only", text) or re.search(r"\bGamma\b.*k-?points?", text, re.I):
        style = "Gamma"
    if re.search(r"Monkhorst[_ -]?Pack", text, re.I):
        style = "Monkhorst"
    elif re.search(r"Automatic generation of k-mesh", text, re.I) and style is None:
        style = "Automatic"
    kspacing = incar.get("KSPACING")
    if kspacing is None:
        kspacing = eff.get("KSPACING")
    if style is None and kspacing is not None:
        style = "KSPACING"
    return {"style": style, "kpts": kpts, "num_kpts": num_kpts,
            "kpts_shift": None, "kspacing": kspacing}


# --- assembly --------------------------------------------------------------------------

def _potcar_set_hash(titels: list[str]) -> str | None:
    """Deterministic fingerprint of the POTCAR set from its titels (as in :mod:`parse`)."""
    import hashlib
    titels = [t for t in titels if t]
    if not titels:
        return None
    return hashlib.sha1("\n".join(titels).encode("utf-8", "replace")).hexdigest()[:16]


def build_calc_parameters(lines: list[str]) -> dict[str, Any]:
    """Assemble a vasprun-schema ``calc_parameters`` dict from OUTCAR header ``lines``.

    Pure function (no I/O) so it unit-tests against inline header fixtures. Produces every key
    the vasprun path's :func:`zenodo_harvest.parse._calc_parameters` produces —
    ``code``/``code_version``/``run_type``/``functional``/``hubbard_u``/``spin_polarized``/
    ``encut``/``ediff``/``ismear``/``sigma``/``ispin``/``kpoints``/``potcar_symbols``/
    ``potcar_spec``/``potcar_set_hash``/``incar`` — plus ``parameters`` (the resolved values,
    matching the vasprun path once it also stores them), an ``xc_lexch`` POTCAR-family hint,
    and ``parsed_from: "outcar_header"`` for provenance.
    """
    incar = _parse_incar_echo(lines)
    eff = _parse_effective(lines)
    titels = _titels_from_lines(lines)
    ldau_on, hubbard_u = _parse_ldau(lines, incar, eff)

    classify_view = _classify_inputs(incar, eff, ldau_on)
    run_type = classify_run_type(classify_view, titels)
    functional = run_type.split("+")[0]

    code_version = None
    if lines:
        mv = re.match(r"\s*vasp\.(\S+)", lines[0])
        if mv:
            code_version = mv.group(1)

    def param(key: str) -> Any:
        """Prefer the user INCAR echo, then the resolved value (mirrors the vasprun path)."""
        v = incar.get(key)
        return v if v is not None else eff.get(key)

    ispin = param("ISPIN")
    return {
        "code": "vasp",
        "code_version": code_version,
        "run_type": run_type,
        "functional": functional,
        "hubbard_u": hubbard_u,
        "spin_polarized": (ispin == 2) if ispin is not None else None,
        "encut": param("ENCUT"),
        "ediff": param("EDIFF"),
        "ismear": param("ISMEAR"),
        "sigma": param("SIGMA"),
        "ispin": ispin,
        "kpoints": _parse_kpoints(lines, incar, eff),
        "potcar_symbols": titels,
        "potcar_spec": [{"titel": t, "hash": None} for t in titels],
        "potcar_set_hash": _potcar_set_hash(titels),
        "incar": incar,               # user-set tags (analog of vasprun.incar)
        "parameters": eff,            # resolved/effective tags (analog of vasprun.parameters)
        "xc_lexch": _potcar_lexch(lines) or None,   # POTCAR XC family hint (esp. when GGA=--)
        "parsed_from": "outcar_header",
    }


def parse_outcar_header(path: str | Path) -> dict[str, Any] | None:
    """Read ``path`` (plain or gz/bz2/xz OUTCAR) and return its ``calc_parameters``.

    Returns ``None`` only if the header cannot be read at all (missing/unreadable file). A
    header that parses but yields little (an edited/truncated OUTCAR) still returns a dict —
    with whatever was recoverable — because a partial parameter set is strictly better than
    the ``null`` the old OUTCAR path stored.
    """
    try:
        lines = read_header_lines(path)
    except OSError as exc:
        logger.warning("cannot read OUTCAR header %s: %s", path, exc)
        return None
    if not lines:
        return None
    return build_calc_parameters(lines)
