"""Resolve *effective* VASP parameters from already-stored calc metadata.

The parser records only the **user-set INCAR** (pymatgen's ``vasprun.incar``), so a tag
the user left at VASP's default is simply absent. This module fills those gaps WITHOUT
re-reading any file: it applies VASP's documented defaults for the tags whose default is a
stable constant, *derives* the context-dependent ones (hybrid mixing/screening) by inverting
``run_type``, and **leaves genuinely undeterminable values as ``None``** rather than guessing.
Pure logic — no network, no pymatgen — so it runs over ``metadata.jsonl`` offline and
unit-tests network-free.

Why this is safe (and where it stops):

* **Authoritative ``parameters`` win over any guess.** When a calc carries a
  ``calc_parameters["parameters"]`` block — VASP's RESOLVED/effective values, stored by BOTH
  the vasprun path and the OUTCAR-header parser (:mod:`outcar_params`) — the resolver reads a
  tag straight from it (``source="parameters"``) instead of filling a documented default or
  inverting ``run_type``. This is the memory's "better fix": no version-dependent default
  guessing when VASP already told us the effective value. Records that predate the block (the
  original Zenodo vasprun harvest) simply have no ``parameters`` key, so the resolver falls
  back to the default/derived tiers exactly as before — fully backward compatible.

* **CRITICAL gate — default-filling requires the FULL INCAR.** On the vasprun/vaspout path
  ``calc_parameters["incar"]`` is the user's complete INCAR, so a tag's *absence* means the
  user did not set it and VASP applied its default. On an OUTCAR path with **no INCAR and no
  parameters block** (an old, un-refreshed OUTCAR record) absence means "we never parsed it",
  NOT "left at default" — so assuming ``LHFCALC=False`` there could silently relabel a
  hybrid/+U/SOC run. The resolver therefore fills nothing on such a calc: every tag comes back
  ``source="unknown"``. Those calcs need the re-fetch + OUTCAR-header re-parse (which gives
  them the authoritative ``parameters`` block above), not a guess.

* **Binary/enum flags with a stable constant default** (``LHFCALC``, ``LDAU``, ``LSORBIT``,
  ``NKRED``, ``ADDGRID``, ``LREAL``, ``LASPH``) are filled from :data:`_CONSTANT_DEFAULTS`.
  These defaults have been constant across VASP 4/5/6; two of them (``LHFCALC``/``LDAU``) are
  additionally cross-checked against ``run_type`` (which carries the hybrid / ``+U`` label).

* **Context-dependent hybrid mixing** (``AEXX``, ``HFSCREEN``) is *derived by inverting*
  ``run_type``: pymatgen computes ``run_type`` FROM the resolved parameters, so a named label
  encodes them — ``HSE06`` ⇒ (AEXX 0.25, HFSCREEN 0.2), ``HSE03`` ⇒ (0.25, 0.3), ``B3LYP`` ⇒
  (0.20, 0.0), ``HF`` ⇒ (1.0, 0.0). pymatgen's catch-all ``"PBEO or other Hybrid Functional"``
  means it could *not* pin the mixing, so neither can we → left ``None`` (``source="unknown"``).
  When the calc is not a hybrid at all these tags are ``source="not_applicable"`` (no exact
  exchange), not a meaningful ``0``.

* **``ENCUT``** default is ``max(ENMAX)`` over the POTCARs — POTCAR-dependent, not a constant —
  and the per-POTCAR ENMAX is not in the stored metadata, so an unset ``ENCUT`` is left
  ``None`` (only ~0.6% of vasprun calcs lack it; resolve those from an ENMAX table or the
  OUTCAR if ever needed).

* **``ISIF``** cannot be recovered as an integer from what we store (2/3/4/6/7 all compute
  stress), so an unset ``ISIF`` is left ``None``. What IS deducible — did the run compute a
  stress tensor — is surfaced separately as ``derived["computes_stress"]`` from the frame
  stress counts (mirroring the measured fact that ISIF-unset calcs are ~98% stress-present,
  consistent with the nominal default 2, without staking anything on that constant).

Provenance is attached to every resolved value via a ``source`` tag so a downstream consumer
can weight authoritative (``"incar"``) values above filled (``"default"``) or inferred
(``"derived:*"``) ones, and skip ``"unknown"``/``"not_applicable"`` entirely.
"""

from __future__ import annotations

from typing import Any

# --- provenance tiers for every resolved value -------------------------------------------
SRC_INCAR = "incar"                 # user-set, authoritative
SRC_PARAMETERS = "parameters"       # VASP's RESOLVED/effective value (authoritative, no guess)
SRC_DEFAULT = "default"             # VASP documented constant default, filled (INCAR known)
SRC_DERIVED_RUNTYPE = "derived:run_type"   # inferred by inverting run_type
SRC_DERIVED_STRESS = "derived:stress"      # inferred from frame stress presence
SRC_NOT_APPLICABLE = "not_applicable"      # only meaningful under a flag that is off
SRC_UNKNOWN = "unknown"             # could not determine — left None

# Stable-constant defaults, safe to assume for an UNSET tag *when the full INCAR is known*.
# Values are VASP's documented defaults, constant across VASP 4/5/6.
_CONSTANT_DEFAULTS: dict[str, Any] = {
    "LHFCALC": False,   # no Hartree-Fock exchange (master hybrid switch)
    "LDAU": False,      # no DFT+U
    "LSORBIT": False,   # no spin-orbit coupling (collinear)
    "NKRED": 1,         # no HF kernel k-grid reduction
    "ADDGRID": False,   # no additional support grid
    "LREAL": False,     # reciprocal-space projection
    "LASPH": False,     # no aspherical PAW contributions
}

# Named hybrids whose mixing IS recoverable from run_type: label -> (AEXX, HFSCREEN).
# (run_type is pymatgen's function of the resolved params, so a named label pins these.)
_HYBRID_MIXING: dict[str, tuple[float, float]] = {
    "HSE06": (0.25, 0.2),
    "HSE03": (0.25, 0.3),
    "B3LYP": (0.20, 0.0),
    "HF": (1.0, 0.0),
}

# The tags this resolver reports on (the user's target set + LASPH).
TARGET_TAGS = ("ENCUT", "LREAL", "ADDGRID", "LASPH", "LHFCALC", "AEXX",
               "HFSCREEN", "ISIF", "LDAU", "NKRED", "LSORBIT")


def _base_run_type(run_type: Any) -> str | None:
    """Base functional label of a run_type, i.e. the part before the first ``+`` suffix.

    ``"HSE06+vdW-DFT-D3-BJ"`` -> ``"HSE06"``, ``"HF+U"`` -> ``"HF"``, ``"PBE"`` -> ``"PBE"``.
    pymatgen's catch-all ``"PBEO or other Hybrid Functional"`` has no ``+`` so it survives
    whole (and is deliberately absent from :data:`_HYBRID_MIXING`).
    """
    if not isinstance(run_type, str) or not run_type:
        return None
    return run_type.split("+", 1)[0]


def _is_hybrid(run_type: Any) -> bool:
    """Whether run_type denotes a hybrid functional (exact-exchange on)."""
    base = _base_run_type(run_type)
    if base is None:
        return False
    return base in _HYBRID_MIXING or "Hybrid" in run_type  # incl. "PBEO or other Hybrid ..."


def _has_hubbard_u(run_type: Any) -> bool:
    """Whether run_type carries a ``+U`` (Hubbard) label."""
    return isinstance(run_type, str) and "+U" in run_type


def _v(value: Any, source: str) -> dict[str, Any]:
    return {"value": value, "source": source}


def resolve_parameters(calc_parameters: dict | None,
                       quality: dict | None = None) -> dict[str, Any]:
    """Resolve effective values (+ provenance) for :data:`TARGET_TAGS` from stored metadata.

    ``calc_parameters`` is a calc's ``metadata.jsonl`` ``calc_parameters`` block; ``quality``
    (optional) is its ``quality`` block, used only to deduce ``computes_stress``.

    Returns::

        {
          "incar_complete": bool,          # was the full user INCAR available?
          "parameters": {TAG: {"value": .., "source": ..}, ...},
          "derived": {"computes_stress": {"value": bool|None, "source": ..}},
        }

    A ``"value"`` of ``None`` with ``source`` ``"unknown"``/``"not_applicable"`` means "left
    unresolved" — never a fabricated default. ``"incar"`` values are passed through verbatim
    (e.g. ``LREAL="Auto"``); only filled defaults / derived values are canonicalised.
    """
    cp = calc_parameters or {}
    incar = cp.get("incar") or {}
    params_eff = cp.get("parameters") or {}   # VASP's RESOLVED/effective values (authoritative)
    run_type = cp.get("run_type")
    # The full INCAR is known only on the vasprun/vaspout path, which stores an ``incar``
    # dict (the OUTCAR path omits the key entirely). Presence of the key — NOT its
    # truthiness — is the discriminator: a genuinely empty INCAR ``{}`` still means "user
    # set nothing, so every tag is at its default", which we CAN fill; a missing key means
    # "we never parsed an INCAR", which we cannot (see module docstring).
    incar_complete = isinstance(cp.get("incar"), dict)

    def _incar_get(tag: str) -> tuple[bool, Any]:
        """(present, value) for a tag in the user INCAR."""
        if tag in incar and incar[tag] is not None:
            return True, incar[tag]
        return False, None

    def _eff_get(tag: str) -> tuple[bool, Any]:
        """(present, value) for a tag in VASP's resolved ``parameters`` (authoritative)."""
        if tag in params_eff and params_eff[tag] is not None:
            return True, params_eff[tag]
        return False, None

    out: dict[str, dict[str, Any]] = {}

    # --- constant-default flags (+ run_type cross-check for the two that have one) --------
    # Precedence per tag: user INCAR > resolved parameters > (default / run_type-derived).
    for tag, default in _CONSTANT_DEFAULTS.items():
        present, val = _incar_get(tag)
        eff_present, eff_val = _eff_get(tag)
        if present:
            out[tag] = _v(val, SRC_INCAR)
        elif eff_present:
            out[tag] = _v(eff_val, SRC_PARAMETERS)     # VASP told us the effective value
        elif not incar_complete:
            out[tag] = _v(None, SRC_UNKNOWN)
        elif tag == "LHFCALC" and _is_hybrid(run_type):
            out[tag] = _v(True, SRC_DERIVED_RUNTYPE)   # run_type proves it IS a hybrid
        elif tag == "LDAU" and _has_hubbard_u(run_type):
            out[tag] = _v(True, SRC_DERIVED_RUNTYPE)   # run_type carries "+U"
        else:
            out[tag] = _v(default, SRC_DEFAULT)

    # --- hybrid mixing (AEXX, HFSCREEN): parameters if known, else invert run_type --------
    hybrid = bool(out["LHFCALC"]["value"])  # resolved LHFCALC (incar / parameters / derived)
    mixing = _HYBRID_MIXING.get(_base_run_type(run_type) or "")
    for tag, idx in (("AEXX", 0), ("HFSCREEN", 1)):
        present, val = _incar_get(tag)
        eff_present, eff_val = _eff_get(tag)
        if present:
            out[tag] = _v(val, SRC_INCAR)
        elif eff_present:
            out[tag] = _v(eff_val, SRC_PARAMETERS)     # authoritative (e.g. AEXX=0.0 non-hybrid)
        elif not incar_complete:
            out[tag] = _v(None, SRC_UNKNOWN)
        elif not hybrid:
            out[tag] = _v(None, SRC_NOT_APPLICABLE)    # no exact exchange -> AEXX/HFSCREEN moot
        elif mixing is not None:
            out[tag] = _v(mixing[idx], SRC_DERIVED_RUNTYPE)
        else:
            out[tag] = _v(None, SRC_UNKNOWN)           # hybrid but pymatgen couldn't pin it

    # --- ENCUT: default is POTCAR-dependent (max ENMAX), not a constant -> parameters or unset
    present, val = _incar_get("ENCUT")
    eff_present, eff_val = _eff_get("ENCUT")
    out["ENCUT"] = (_v(val, SRC_INCAR) if present else
                    _v(eff_val, SRC_PARAMETERS) if eff_present else _v(None, SRC_UNKNOWN))

    # --- ISIF: integer not recoverable by guessing -> take it if parameters/INCAR carry it -
    present, val = _incar_get("ISIF")
    eff_present, eff_val = _eff_get("ISIF")
    out["ISIF"] = (_v(val, SRC_INCAR) if present else
                   _v(eff_val, SRC_PARAMETERS) if eff_present else _v(None, SRC_UNKNOWN))

    # derived physical fact: did the run compute a stress tensor? (from frame counts)
    computes_stress: dict[str, Any]
    if quality and quality.get("n_frames_with_stress") is not None:
        computes_stress = _v(int(quality.get("n_frames_with_stress") or 0) > 0,
                             SRC_DERIVED_STRESS)
    else:
        computes_stress = _v(None, SRC_UNKNOWN)

    return {
        "incar_complete": incar_complete,
        "parameters": out,
        "derived": {"computes_stress": computes_stress},
    }


def effective_params(calc_parameters: dict | None,
                     quality: dict | None = None) -> dict[str, Any]:
    """Convenience: ``{TAG: value}`` for :data:`TARGET_TAGS` (``None`` where unresolved).

    Drops the provenance — use :func:`resolve_parameters` when you need to know whether a
    value was user-set, filled, or inferred.
    """
    res = resolve_parameters(calc_parameters, quality)
    return {tag: res["parameters"][tag]["value"] for tag in res["parameters"]}
