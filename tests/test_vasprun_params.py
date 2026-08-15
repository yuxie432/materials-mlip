"""Tests for the lightweight vasprun-header parameters reader (``zenodo_harvest.vasprun_params``).

``resolved_parameters`` is pure (no pymatgen) — the ENMAX→ENCUT rename + EFFECTIVE_TAGS
restriction shared with the live parser. ``parse_vasprun_parameters`` is exercised against a
minimal on-disk vasprun.xml (needs lxml + pymatgen, skipped if absent) to confirm it reads the
``<parameters>`` block, stops before the ``<calculation>`` trajectory, and normalises ENMAX.
"""

from __future__ import annotations

import gzip

import pytest

from zenodo_harvest.vasprun_params import parse_vasprun_parameters, resolved_parameters

# A minimal but well-formed vasprun.xml: a <parameters> block (with ENMAX, ISIF, a nested
# exchange-correlation separator) followed by a <calculation> the reader must NOT read.
MINIMAL_VASPRUN = """<?xml version="1.0" encoding="ISO-8859-1"?>
<modeling>
 <generator>
  <i name="program" type="string">vasp</i>
  <i name="version" type="string">6.4.2</i>
 </generator>
 <incar>
  <i type="int" name="ISIF">3</i>
 </incar>
 <parameters>
  <separator name="electronic">
   <i type="float" name="ENMAX">400.00000000</i>
   <i type="int" name="ISIF">3</i>
   <i type="logical" name="ADDGRID"> T </i>
   <i type="logical" name="LREAL"> F </i>
   <separator name="electronic exchange-correlation">
    <i type="string" name="GGA">PE</i>
    <i type="logical" name="LHFCALC"> F </i>
    <i type="float" name="AEXX">0.00000000</i>
   </separator>
  </separator>
 </parameters>
 <calculation>
  <energy><i name="e_fr_energy">-1.23456789</i></energy>
 </calculation>
</modeling>
"""


# --- resolved_parameters (pure) --------------------------------------------------------

def test_resolved_parameters_renames_enmax_to_encut():
    out = resolved_parameters({"ENMAX": 400.0, "ISIF": 3})
    assert out["ENCUT"] == 400.0 and "ENMAX" not in out and out["ISIF"] == 3


def test_resolved_parameters_restricts_to_effective_tags_and_drops_none():
    out = resolved_parameters({"ENMAX": 520.0, "GGA": "PE", "NOT_A_TAG": 9,
                               "ADDGRID": True, "LREAL": None})
    assert out == {"ENCUT": 520.0, "GGA": "PE", "ADDGRID": True}   # NOT_A_TAG dropped; None dropped


def test_resolved_parameters_existing_encut_wins_over_enmax():
    # if a source ever carries both, do not clobber the canonical ENCUT with ENMAX
    out = resolved_parameters({"ENCUT": 500.0, "ENMAX": 400.0})
    assert out["ENCUT"] == 500.0


def test_resolved_parameters_empty_and_none():
    assert resolved_parameters({}) == {}
    assert resolved_parameters(None) == {}


# --- parse_vasprun_parameters (needs lxml + pymatgen) ----------------------------------

def _pmg_or_skip():
    pytest.importorskip("lxml")
    pytest.importorskip("pymatgen")


def test_parse_vasprun_parameters_reads_header(tmp_path):
    _pmg_or_skip()
    f = tmp_path / "vasprun.xml"
    f.write_text(MINIMAL_VASPRUN)
    cp = parse_vasprun_parameters(f)
    assert cp is not None
    assert cp["ENCUT"] == 400.0          # ENMAX -> ENCUT
    assert cp["ISIF"] == 3               # authoritative integer (the key win)
    assert cp["ADDGRID"] is True and cp["LREAL"] is False
    # pymatgen's Incar parser title-cases the GGA string ("PE" -> "Pe"); we store it verbatim,
    # and classify_run_type upper-cases it, so PBE classification is unaffected.
    assert cp["GGA"] == "Pe" and cp["LHFCALC"] is False and cp["AEXX"] == 0.0
    assert "ENMAX" not in cp


def test_parse_vasprun_parameters_gzip(tmp_path):
    _pmg_or_skip()
    f = tmp_path / "vasprun.xml.gz"
    with gzip.open(f, "wt", encoding="utf-8") as fh:
        fh.write(MINIMAL_VASPRUN)
    cp = parse_vasprun_parameters(f)
    assert cp is not None and cp["ENCUT"] == 400.0 and cp["ISIF"] == 3


def test_parse_vasprun_parameters_bz2_and_xz(tmp_path):
    # regression: real uploads stage vasprun.xml.bz2 / .xz — the opener MUST decompress them,
    # else lxml gets compressed bytes and fails "Start tag expected" (the 112-calc recovery gap)
    _pmg_or_skip()
    import bz2 as _bz2
    import lzma as _lzma
    for suffix, opener in ((".bz2", _bz2.open), (".xz", _lzma.open)):
        f = tmp_path / f"vasprun.xml{suffix}"
        with opener(f, "wt", encoding="utf-8") as fh:
            fh.write(MINIMAL_VASPRUN)
        cp = parse_vasprun_parameters(f)
        assert cp is not None, suffix
        assert cp["ENCUT"] == 400.0 and cp["ISIF"] == 3, suffix


def test_parse_vasprun_parameters_missing_or_bad_returns_none(tmp_path):
    _pmg_or_skip()
    assert parse_vasprun_parameters(tmp_path / "nope.xml") is None
    bad = tmp_path / "bad.xml"
    bad.write_text("<modeling><calculation/></modeling>")   # no <parameters> block
    assert parse_vasprun_parameters(bad) is None
