"""Offline unit tests for the VASP defaults-resolver (``zenodo_harvest.param_resolver``).

No network, no pymatgen — pure logic over stored calc metadata. Covers: the INCAR-complete
gate (OUTCAR path fills nothing), constant-default filling, run_type cross-checks for
LHFCALC/LDAU, hybrid-mixing inversion (HSE06/HSE03/B3LYP/HF and the unpinnable "PBEO or
other" catch-all), not-applicable AEXX/HFSCREEN when not a hybrid, ENCUT/ISIF left unset,
and the computes_stress derivation.

Run: ``python -m pytest tests/test_param_resolver.py -q``.
"""

from __future__ import annotations

from zenodo_harvest.param_resolver import (
    SRC_DEFAULT,
    SRC_DERIVED_RUNTYPE,
    SRC_DERIVED_STRESS,
    SRC_INCAR,
    SRC_NOT_APPLICABLE,
    SRC_UNKNOWN,
    effective_params,
    resolve_parameters,
)


def _vasprun_cp(incar: dict, run_type: str = "PBE") -> dict:
    """A minimal vasprun-path calc_parameters (has an INCAR => INCAR-complete)."""
    return {"run_type": run_type, "functional": run_type.split("+")[0], "incar": incar}


# --- the INCAR-complete gate -------------------------------------------------------------

def test_outcar_path_fills_nothing():
    """No INCAR (OUTCAR path) => every tag 'unknown', no fabricated defaults."""
    cp = {"code": "vasp", "run_type": None, "functional": None, "potcar_titels": ["PAW_PBE H"]}
    res = resolve_parameters(cp)
    assert res["incar_complete"] is False
    for tag, entry in res["parameters"].items():
        assert entry["value"] is None, tag
        assert entry["source"] == SRC_UNKNOWN, tag


def test_none_calc_parameters_is_safe():
    res = resolve_parameters(None)
    assert res["incar_complete"] is False
    assert res["parameters"]["LHFCALC"]["source"] == SRC_UNKNOWN


# --- constant-default flags --------------------------------------------------------------

def test_unset_binary_flags_get_documented_defaults():
    res = resolve_parameters(_vasprun_cp({"ENCUT": 520.0}))["parameters"]
    assert res["LREAL"] == {"value": False, "source": SRC_DEFAULT}
    assert res["ADDGRID"] == {"value": False, "source": SRC_DEFAULT}
    assert res["LASPH"] == {"value": False, "source": SRC_DEFAULT}
    assert res["LSORBIT"] == {"value": False, "source": SRC_DEFAULT}
    assert res["NKRED"] == {"value": 1, "source": SRC_DEFAULT}
    assert res["LHFCALC"] == {"value": False, "source": SRC_DEFAULT}
    assert res["LDAU"] == {"value": False, "source": SRC_DEFAULT}


def test_set_values_pass_through_as_incar():
    """A user-set value (even a non-bool enum like LREAL=Auto) is authoritative, verbatim."""
    res = resolve_parameters(_vasprun_cp({"LREAL": "Auto", "ADDGRID": True}))["parameters"]
    assert res["LREAL"] == {"value": "Auto", "source": SRC_INCAR}
    assert res["ADDGRID"] == {"value": True, "source": SRC_INCAR}


# --- run_type cross-checks ---------------------------------------------------------------

def test_lhfcalc_derived_true_when_runtype_is_hybrid():
    res = resolve_parameters(_vasprun_cp({}, run_type="HSE06"))["parameters"]
    assert res["LHFCALC"] == {"value": True, "source": SRC_DERIVED_RUNTYPE}


def test_ldau_derived_true_when_runtype_has_plus_u():
    res = resolve_parameters(_vasprun_cp({}, run_type="GGA+U"))["parameters"]
    assert res["LDAU"] == {"value": True, "source": SRC_DERIVED_RUNTYPE}
    # ...and combined HF+U carries both signals
    res2 = resolve_parameters(_vasprun_cp({}, run_type="HF+U"))["parameters"]
    assert res2["LHFCALC"]["value"] is True
    assert res2["LDAU"]["value"] is True


# --- hybrid mixing inversion -------------------------------------------------------------

def test_hse06_mixing_inverted_from_runtype():
    res = resolve_parameters(_vasprun_cp({}, run_type="HSE06+vdW-DFT-D3-BJ"))["parameters"]
    assert res["AEXX"] == {"value": 0.25, "source": SRC_DERIVED_RUNTYPE}
    assert res["HFSCREEN"] == {"value": 0.2, "source": SRC_DERIVED_RUNTYPE}


def test_b3lyp_and_hf_and_hse03_mixing():
    assert resolve_parameters(_vasprun_cp({}, "B3LYP"))["parameters"]["AEXX"]["value"] == 0.20
    assert resolve_parameters(_vasprun_cp({}, "HF+U"))["parameters"]["AEXX"]["value"] == 1.0
    assert resolve_parameters(_vasprun_cp({}, "HSE03"))["parameters"]["HFSCREEN"]["value"] == 0.3


def test_unpinnable_hybrid_left_unknown():
    """pymatgen's catch-all label => hybrid True, but mixing cannot be pinned => unknown."""
    res = resolve_parameters(_vasprun_cp({}, "PBEO or other Hybrid Functional"))["parameters"]
    assert res["LHFCALC"]["value"] is True          # it IS a hybrid
    assert res["AEXX"] == {"value": None, "source": SRC_UNKNOWN}
    assert res["HFSCREEN"] == {"value": None, "source": SRC_UNKNOWN}


def test_aexx_not_applicable_for_non_hybrid():
    res = resolve_parameters(_vasprun_cp({}, "PBEsol"))["parameters"]
    assert res["AEXX"] == {"value": None, "source": SRC_NOT_APPLICABLE}
    assert res["HFSCREEN"] == {"value": None, "source": SRC_NOT_APPLICABLE}


def test_explicit_aexx_beats_derivation():
    res = resolve_parameters(_vasprun_cp({"AEXX": 0.4}, "HSE06"))["parameters"]
    assert res["AEXX"] == {"value": 0.4, "source": SRC_INCAR}


# --- ENCUT / ISIF: left unresolved -------------------------------------------------------

def test_encut_and_isif_unset_are_left_unknown_not_guessed():
    res = resolve_parameters(_vasprun_cp({}))["parameters"]
    assert res["ENCUT"] == {"value": None, "source": SRC_UNKNOWN}
    assert res["ISIF"] == {"value": None, "source": SRC_UNKNOWN}


def test_encut_and_isif_set_are_authoritative():
    res = resolve_parameters(_vasprun_cp({"ENCUT": 600.0, "ISIF": 3}))["parameters"]
    assert res["ENCUT"] == {"value": 600.0, "source": SRC_INCAR}
    assert res["ISIF"] == {"value": 3, "source": SRC_INCAR}


# --- computes_stress derivation ----------------------------------------------------------

def test_computes_stress_from_quality():
    cp = _vasprun_cp({})
    assert resolve_parameters(cp, {"n_frames_with_stress": 12})["derived"]["computes_stress"] \
        == {"value": True, "source": SRC_DERIVED_STRESS}
    assert resolve_parameters(cp, {"n_frames_with_stress": 0})["derived"]["computes_stress"] \
        == {"value": False, "source": SRC_DERIVED_STRESS}
    assert resolve_parameters(cp, None)["derived"]["computes_stress"]["source"] == SRC_UNKNOWN


# --- convenience wrapper -----------------------------------------------------------------

def test_effective_params_flattens_values():
    vals = effective_params(_vasprun_cp({"ENCUT": 520.0}, "HSE06"))
    assert vals["LHFCALC"] is True and vals["AEXX"] == 0.25 and vals["LREAL"] is False
    assert vals["ENCUT"] == 520.0 and vals["ISIF"] is None
