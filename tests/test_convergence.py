"""Tests for OUTCAR SCF-convergence extraction (``zenodo_harvest.convergence``).

Network-free and pymatgen/ase-free: a small synthetic OUTCAR (header + two ionic steps, one
converged in fewer than NELM electronic steps, one that hits NELM) exercises the
``Iteration X(Y)`` / ``total energy-change (2. order)`` parsing, the NELM read, the
``n_esteps < NELM`` verdict, index/order alignment, and gzip input. A cross-check against ASE's
real ``OUTCAR_example_1`` fixture runs when it is present.

Run: ``python -m pytest tests/test_convergence.py -q``.
"""

from __future__ import annotations

import gzip

from zenodo_harvest.convergence import (converged_ionic_from_params, cparam,
                                        outcar_nelm, outcar_scf_convergence,
                                        scan_outcar_scf)

# Header (NELM=4) then two ionic steps. Ionic step 1 converges in 3 electronic steps (< NELM);
# ionic step 2 runs the full 4 (== NELM -> unconverged). The LAST "total energy-change" of each
# step is its scf_dE. VASP prints these values signed; the scanner stores the magnitude.
SYNTH_OUTCAR = """\
 vasp.6.4.2
   NELM   =      4;   NELMIN=  2; NELMDL= -5     # of ELM steps
   EDIFF  = 0.1E-03   stopping-criterion for ELM
----------------------------------------- Iteration    1(   1)  ---------------------------------------
 total energy-change (2. order) : 0.5000000E+02  (-0.1000000E+03)
----------------------------------------- Iteration    1(   2)  ---------------------------------------
 total energy-change (2. order) :-0.2000000E+01  (-0.1000000E+01)
----------------------------------------- Iteration    1(   3)  ---------------------------------------
 total energy-change (2. order) :-0.3000000E-04  (-0.2000000E-04)
------------------------ aborting loop because EDIFF is reached ----------------------------------------
  free  energy   TOTEN  =       -10.00000000 eV
----------------------------------------- Iteration    2(   1)  ---------------------------------------
 total energy-change (2. order) : 0.1000000E+01  (-0.5000000E+00)
----------------------------------------- Iteration    2(   2)  ---------------------------------------
 total energy-change (2. order) :-0.5000000E+00  (-0.2500000E+00)
----------------------------------------- Iteration    2(   3)  ---------------------------------------
 total energy-change (2. order) :-0.1000000E+00  (-0.5000000E-01)
----------------------------------------- Iteration    2(   4)  ---------------------------------------
 total energy-change (2. order) :-0.9000000E-02  (-0.4000000E-02)
------------------------ aborting loop EDIFF was not reached (unconverged)  ----------------------------
  free  energy   TOTEN  =       -10.10000000 eV
"""


def _write(path, text, *, gz=False):
    if gz:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write(text)
    else:
        path.write_text(text, encoding="utf-8")
    return str(path)


def test_scan_outcar_scf_counts_and_last_delta(tmp_path):
    p = _write(tmp_path / "OUTCAR", SYNTH_OUTCAR)
    blocks = scan_outcar_scf(p)
    assert len(blocks) == 2
    # step 1: 3 e-steps, scf_dE = |last change| = 3e-05
    assert blocks[0]["n_esteps"] == 3
    assert abs(blocks[0]["scf_dE"] - 3.0e-05) < 1e-12
    # step 2: 4 e-steps, scf_dE = |−9.0e-03| = 9.0e-03
    assert blocks[1]["n_esteps"] == 4
    assert abs(blocks[1]["scf_dE"] - 9.0e-03) < 1e-12


def test_outcar_nelm_reads_header_not_nelmin():
    lines = SYNTH_OUTCAR.splitlines()
    assert outcar_nelm(lines) == 4                 # never matches NELMIN/NELMDL
    assert outcar_nelm(["no nelm here"]) == 60     # VASP default


def test_convergence_verdict_lt_nelm(tmp_path):
    p = _write(tmp_path / "OUTCAR", SYNTH_OUTCAR)
    conv = outcar_scf_convergence(p, nelm=4)
    assert len(conv) == 2
    # step 0 file-order: (scf_dE, converged=True) because 3 < 4
    assert conv[0][1] is True and abs(conv[0][0] - 3.0e-05) < 1e-12
    # step 1: 4 e-steps == NELM -> NOT converged
    assert conv[1][1] is False and abs(conv[1][0] - 9.0e-03) < 1e-12


def test_convergence_unknown_when_nelm_zero(tmp_path):
    p = _write(tmp_path / "OUTCAR", SYNTH_OUTCAR)
    conv = outcar_scf_convergence(p, nelm=0)        # NELM unknown -> verdict None (never fabricated)
    assert [c[1] for c in conv] == [None, None]
    assert all(c[0] is not None for c in conv)       # but the magnitude is still recovered


def test_scan_handles_gzip(tmp_path):
    p = _write(tmp_path / "OUTCAR.gz", SYNTH_OUTCAR, gz=True)
    blocks = scan_outcar_scf(p)
    assert [b["n_esteps"] for b in blocks] == [3, 4]


def test_scan_empty_on_foreign_text(tmp_path):
    p = _write(tmp_path / "OUTCAR", "not an outcar\njust some text\n")
    assert scan_outcar_scf(p) == []
    assert outcar_scf_convergence(p, nelm=60) == []


def test_converged_ionic_faithful_to_pymatgen():
    # relaxation (IBRION 2): converged iff it exited before NSW steps
    assert converged_ionic_from_params(100, 2, 0.01, 40) is True    # 40 < 100
    assert converged_ionic_from_params(40, 2, 0.01, 40) is False    # ran all NSW -> not converged
    # single-point / NSW<=1 is trivially converged, regardless of IBRION
    assert converged_ionic_from_params(0, -1, 0.001, 1) is True
    assert converged_ionic_from_params(1, 2, 0.01, 1) is True
    # MD (IBRION 0): converged iff it ran the full NSW steps
    assert converged_ionic_from_params(1000, 0, None, 1000) is True
    assert converged_ionic_from_params(1000, 0, None, 500) is False
    # EDIFFG==0 "run for NSW steps" (IBRION 1/2): converged iff it ran all NSW
    assert converged_ionic_from_params(50, 2, 0, 50) is True
    assert converged_ionic_from_params(50, 2, 0, 30) is False
    # IBRION default: -1 when NSW in {-1,0}, else 0
    assert converged_ionic_from_params(0, None, 0.01, 1) is True    # NSW=0 -> IBRION -1 path
    assert converged_ionic_from_params(1000, None, None, 1000) is True  # NSW>0 -> IBRION 0 (MD)
    # unknown ionic-step count -> None (never fabricated)
    assert converged_ionic_from_params(100, 2, 0.01, None) is None


def test_cparam_lookup_order():
    cp = {"parameters": {"NSW": 100, "IBRION": 2}, "incar": {"IBRION": 1, "EDIFFG": -0.02}}
    assert cparam(cp, "NSW") == 100                 # parameters preferred
    assert cparam(cp, "IBRION") == 2                # parameters wins over incar
    assert cparam(cp, "EDIFFG") == -0.02            # falls back to incar
    assert cparam(cp, "MISSING") is None
    assert cparam({}, "NSW") is None


def test_real_ase_fixture_if_present():
    import os
    fx = ("/home/yuxie432/anaconda3/lib/python3.13/site-packages/"
          "ase/test/testdata/vasp/OUTCAR_example_1")
    if not os.path.isfile(fx):
        return
    blocks = scan_outcar_scf(fx)
    assert len(blocks) == 1 and blocks[0]["n_esteps"] == 63
    assert abs(blocks[0]["scf_dE"] - 5.969621e-06) < 1e-12
    conv = outcar_scf_convergence(fx, nelm=120)
    assert conv[0][1] is True                        # 63 < 120
