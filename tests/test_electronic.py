"""Offline unit tests for net magnetization + net charge (``zenodo_harvest.electronic``).

The OUTCAR magnetization-line scan, the vasprun ``<atominfo>`` valence read, the OUTCAR
ZVAL/ions charge, and :func:`is_magnetic` are pure stdlib (+ the stdlib-fallback OUTCAR header
parser), so they run in the dependency-free tier. The occupancy method and the POTCAR-stats
ZVAL table need pymatgen, so those tests ``importorskip`` it.

Synthetic OUTCAR/vasprun fragments below reproduce the real VASP formats: the per-SCF
``number of electron X magnetization Y`` line, the per-POTCAR ``POMASS = …; ZVAL = …`` block,
the ``ions per type`` line, and the ``<atominfo>`` ``atomtypes`` array (whose 4th column is the
valence pymatgen discards).

Run: ``python -m pytest tests/test_electronic.py -q``.
"""

from __future__ import annotations

import gzip
import math

import pytest

from zenodo_harvest import electronic as E

# --- OUTCAR magnetization line ---------------------------------------------------------

_OUTCAR = """ vasp.6.3.0
  PAW_PBE Fe 06Sep2000 :
   POMASS =   55.847; ZVAL   =    8.000    mass and valenz
  PAW_PBE O 08Apr2002 :
   POMASS =   16.000; ZVAL   =    6.000    mass and valenz
   ions per type =               2   3
 Startparameter for this run:
   NELECT =      34.0000    total number of electrons
--------------------------------------- Iteration    1(   1)  ------------------------
 number of electron      34.0000000 magnetization       4.0000010
--------------------------------------- Iteration    2(   1)  ------------------------
 number of electron      34.0000000 magnetization       3.7500000
"""


def _write(tmp_path, name, text, *, gz=False):
    p = tmp_path / name
    if gz:
        with gzip.open(p, "wt") as fh:
            fh.write(text)
    else:
        p.write_text(text)
    return str(p)


def test_scan_outcar_mag_line_takes_last(tmp_path):
    nelect, mags = E.scan_outcar_mag_line(_write(tmp_path, "OUTCAR", _OUTCAR))
    assert nelect == 34.0
    assert mags == [3.75]  # the FINAL iteration's value, not the first


def test_scan_outcar_mag_line_gzip(tmp_path):
    nelect, mags = E.scan_outcar_mag_line(_write(tmp_path, "OUTCAR.gz", _OUTCAR, gz=True))
    assert nelect == 34.0 and mags == [3.75]


def test_scan_outcar_mag_line_noncollinear_norm(tmp_path):
    ncl = " number of electron   40.0 magnetization   3.0 0.0 4.0\n"
    val, src = E._mag_from_outcar(_write(tmp_path, "OUTCAR", ncl), True, True)
    assert src == "outcar_ncl"
    assert math.isclose(val, 5.0)  # |(3,0,4)| = 5


def test_mag_from_outcar_absent_line_is_nonmagnetic(tmp_path):
    # An ISPIN=1 run prints no magnetization line -> net moment is a firm 0.
    plain = " number of electron      34.0000000\n"
    assert E._mag_from_outcar(_write(tmp_path, "OUTCAR", plain), False, False) == (0.0, "nonmagnetic")


def test_mag_from_outcar_absent_but_spin_is_unavailable(tmp_path):
    # A spin/NCL run whose line could not be read -> unavailable, never a fabricated 0.
    plain = " some truncated outcar with no mag line\n"
    assert E._mag_from_outcar(_write(tmp_path, "OUTCAR", plain), True, False) == (None, "unavailable")


# --- vasprun atominfo valence ----------------------------------------------------------

_ATOMINFO = """<?xml version="1.0"?>
<modeling>
<atominfo>
 <array name="atomtypes">
  <field type="int">atomspertype</field>
  <field type="string">element</field>
  <field>atommass</field>
  <field>valence</field>
  <field type="string">pseudopotential</field>
  <set>
   <rc><c>2</c><c>Fe</c><c>55.847</c><c>8.000</c><c>PAW_PBE Fe 06Sep2000</c></rc>
   <rc><c>3</c><c>O</c><c>16.000</c><c>6.000</c><c>PAW_PBE O 08Apr2002</c></rc>
  </set>
 </array>
</atominfo>
<calculation></calculation>
</modeling>"""


def test_neutral_nelect_from_atominfo(tmp_path):
    assert E.neutral_nelect_from_atominfo(_write(tmp_path, "vasprun.xml", _ATOMINFO)) == 34.0


def test_neutral_nelect_from_atominfo_gzip(tmp_path):
    assert E.neutral_nelect_from_atominfo(_write(tmp_path, "vasprun.xml.gz", _ATOMINFO, gz=True)) == 34.0


def test_neutral_nelect_from_atominfo_absent(tmp_path):
    no_types = "<modeling><atominfo></atominfo><calculation></calculation></modeling>"
    assert E.neutral_nelect_from_atominfo(_write(tmp_path, "vasprun.xml", no_types)) is None


def test_neutral_nelect_from_atominfo_missing_file(tmp_path):
    assert E.neutral_nelect_from_atominfo(str(tmp_path / "nope.xml")) is None
    assert E.neutral_nelect_from_atominfo(None) is None


# --- OUTCAR full electronic block (net moment + net charge) -----------------------------

def test_electronic_from_outcar_neutral(tmp_path):
    from zenodo_harvest.outcar_params import build_calc_parameters, read_header_lines
    p = _write(tmp_path, "OUTCAR", _OUTCAR)
    lines = read_header_lines(p)
    blk = E.electronic_from_outcar(build_calc_parameters(lines), lines, p)
    assert blk["net_magnetization"] == 3.75 and blk["magnetization_source"] == "outcar"
    assert blk["nelect_neutral"] == 34.0          # 2*8 + 3*6
    assert blk["nelect"] == 34.0
    assert blk["net_charge"] == 0.0 and blk["charge_source"] == "outcar_header"
    assert blk["magnetization_units"] == "mu_B"


def test_electronic_from_outcar_charged(tmp_path):
    # NELECT set two electrons below neutral -> net charge +2 (electron-deficient / cation).
    charged = _OUTCAR.replace("NELECT =      34.0000", "NELECT =      32.0000")
    from zenodo_harvest.outcar_params import build_calc_parameters, read_header_lines
    p = _write(tmp_path, "OUTCAR", charged)
    lines = read_header_lines(p)
    blk = E.electronic_from_outcar(build_calc_parameters(lines), lines, p)
    assert blk["nelect"] == 32.0 and blk["nelect_neutral"] == 34.0
    assert blk["net_charge"] == 2.0


class _FakeVaspout:
    """A vaspout.h5-like object: no atominfo XML, and (as seen in real uploads) neither NELECT
    in .parameters nor resolvable POTCAR titels — so charge is unavailable from the object alone."""
    def __init__(self):
        self.parameters = {}          # HDF5 exposes no NELECT here
        self.is_spin = False
        self.atomic_symbols = None
        self.potcar_symbols = None


def test_vaspout_charge_falls_back_to_colocated_outcar(tmp_path):
    # The fix: electronic_from_object now reads a co-located OUTCAR for net charge (not just
    # magnetization). Without the OUTCAR the vaspout charge is unavailable; with it, it matches
    # exactly what electronic_from_outcar computes (NELECT + ZVAL from the OUTCAR header).
    p = _write(tmp_path, "OUTCAR", _OUTCAR)
    # no OUTCAR -> charge unavailable (pre-fix behaviour, still correct)
    blk0 = E.electronic_from_object(_FakeVaspout(), atominfo_path=None,
                                    outcar_path=None, eigen_parsed=False)
    assert blk0["net_charge"] is None and blk0["charge_source"] == "unavailable"
    # co-located OUTCAR -> charge recovered from the header
    blk = E.electronic_from_object(_FakeVaspout(), atominfo_path=None,
                                   outcar_path=p, eigen_parsed=False)
    assert blk["nelect"] == 34.0 and blk["nelect_neutral"] == 34.0    # 2*8 + 3*6
    assert blk["net_charge"] == 0.0 and blk["charge_source"] == "outcar_header"


def test_vaspout_charge_charged_from_outcar(tmp_path):
    charged = _OUTCAR.replace("NELECT =      34.0000", "NELECT =      32.0000")
    p = _write(tmp_path, "OUTCAR", charged)
    blk = E.electronic_from_object(_FakeVaspout(), atominfo_path=None,
                                   outcar_path=p, eigen_parsed=False)
    assert blk["net_charge"] == 2.0 and blk["charge_source"] == "outcar_header"


def test_outcar_neutral_nelect_mismatch_returns_none(tmp_path):
    # ZVAL count (1) != ions-per-type count (2) -> refuse (None) rather than guess.
    bad = (" PAW_PBE Fe 06Sep2000 :\n   POMASS =   55.847; ZVAL   =    8.000\n"
           "   ions per type =               2   3\n Startparameter for this run:\n")
    from zenodo_harvest.outcar_params import read_header_lines
    lines = read_header_lines(_write(tmp_path, "OUTCAR", bad))
    val, src = E._outcar_neutral_nelect(lines, ["PAW_PBE Fe 06Sep2000"])
    assert val is None and src == "unavailable"


# --- is_magnetic -----------------------------------------------------------------------

def test_is_magnetic():
    assert E.is_magnetic({"spin_polarized": True}) is True
    assert E.is_magnetic({"spin_polarized": False, "parameters": {"LNONCOLLINEAR": True}}) is True
    assert E.is_magnetic({"spin_polarized": False, "incar": {"LSORBIT": ".TRUE."}}) is True
    assert E.is_magnetic({"spin_polarized": False}) is False
    assert E.is_magnetic({"spin_polarized": False, "parameters": {"LNONCOLLINEAR": False}}) is False


def test_empty_block():
    blk = E.empty_block()
    assert blk["net_magnetization"] is None and blk["net_charge"] is None
    assert blk["magnetization_source"] == "unavailable" and blk["charge_source"] == "unavailable"


# --- pymatgen-dependent: ZVAL table + occupancy method ---------------------------------

def test_zval_for_titel():
    pytest.importorskip("pymatgen")
    assert E._zval_for_titel("PAW_PBE F 08Apr2002") == 7.0
    assert E._zval_for_titel("PAW_PBE Zz 01Jan1900") is None      # not in the table
    assert E._zval_for_titel("") is None


def test_neutral_nelect_from_titels():
    pytest.importorskip("pymatgen")
    sym = ["Fe", "Fe", "O", "O", "O"]
    tit = ["PAW_PBE Fe 06Sep2000", "PAW_PBE O 08Apr2002"]
    assert E.neutral_nelect_from_titels(sym, tit) == 34.0        # 2*8 + 3*6
    # an unresolved titel -> None (better than a wrong charge)
    assert E.neutral_nelect_from_titels(sym, ["PAW_PBE Fe 06Sep2000", "PAW_PBE Zz 01Jan1900"]) is None
    # mismatched species/titel counts -> None
    assert E.neutral_nelect_from_titels(sym, tit[:1]) is None


def _fake_vasprun(is_spin, up_occ, dn_occ, weights, noncollinear=False):
    from types import SimpleNamespace

    import numpy as np
    from pymatgen.electronic_structure.core import Spin

    def _arr(occs):  # (nk, nb, 2): energies unused, occupancies in [...,1]
        return np.array([[[0.0, o] for o in band] for band in occs], dtype=float)

    eig = {Spin.up: _arr(up_occ)}
    if dn_occ is not None:
        eig[Spin.down] = _arr(dn_occ)
    return SimpleNamespace(is_spin=is_spin, parameters={"LNONCOLLINEAR": noncollinear},
                           eigenvalues=eig, actual_kpoints_weights=weights)


def test_occupancy_method_collinear():
    pytest.importorskip("pymatgen")
    # 2 kpoints (weights .5/.5). up occ sum = 3 each, down occ sum = 1 each -> mag = 2.
    v = _fake_vasprun(True, [[1, 1, 1], [1, 1, 1]], [[1, 0, 0], [1, 0, 0]], [0.5, 0.5])
    val, src = E.net_magnetization_from_object(v, True, True, False)
    assert math.isclose(val, 2.0) and src == "occupancies"


def test_occupancy_method_nonmagnetic():
    pytest.importorskip("pymatgen")
    v = _fake_vasprun(False, [[1, 1]], None, [1.0])
    assert E.net_magnetization_from_object(v, False, False, False) == (0.0, "nonmagnetic")


def test_occupancy_method_ncl_unavailable():
    pytest.importorskip("pymatgen")
    v = _fake_vasprun(False, [[1, 1]], None, [1.0], noncollinear=True)
    # non-collinear needs the projected magnetization we don't parse -> unavailable_ncl
    assert E.net_magnetization_from_object(v, False, False, True) == (None, "unavailable_ncl")


def test_occupancy_method_needs_eigen_parsed():
    pytest.importorskip("pymatgen")
    v = _fake_vasprun(True, [[1, 1, 1]], [[1, 0, 0]], [1.0])
    # spin-polarised but eigen not parsed -> unavailable (caller decides parse_eigen)
    assert E.net_magnetization_from_object(v, False, True, False) == (None, "unavailable")
