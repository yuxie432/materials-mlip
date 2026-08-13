"""Offline unit tests for the OUTCAR-header parameter parser (``zenodo_harvest.outcar_params``).

Pure stdlib logic — no network, no pymatgen, no ASE — so this runs in the dependency-free
tier alongside ``test_harvest.py``. The synthetic headers below reproduce the real VASP
OUTCAR format verified against live Zenodo records (a 6.4.2 GGA run + an old 5.3.3 run): the
verbatim ``INCAR:`` echo, the per-POTCAR ``TITEL``/``POTCAR:`` lines, the resolved-parameter
blocks (``Startparameter``/``Exchange correlation treatment:``), the automatic k-mesh echo,
and the first-ionic-step marker that ends the header.

The ``run_type``/``functional`` expectations were cross-checked against pymatgen 2026.5.4's
actual ``Vasprun.run_type`` (see the dev-time matrix); they are hard-coded here so the check
stays dependency-free. The single most important property under test is that a plain GGA run —
whose OUTCAR prints no HF block and no ``LDAU`` line — is NOT mislabeled ``HF`` or ``+U`` by
pymatgen's default-laden classifier.

Run: ``python -m pytest tests/test_outcar_params.py -q``.
"""

from __future__ import annotations

import gzip

from zenodo_harvest.outcar_params import (
    _classify_inputs,
    _coerce_value,
    _parse_effective,
    _parse_incar_echo,
    _parse_ldau,
    _titels_from_lines,
    build_calc_parameters,
    classify_run_type,
    parse_outcar_header,
    read_header_lines,
)

# --- synthetic headers (trimmed but format-faithful) -----------------------------------

# A plain PBE (GGA=PE) relaxation, vasp 6.x, with a full INCAR echo, two POTCARs, an
# automatic k-mesh, and the resolved blocks. Ends at the first ionic-step marker.
PBE_HEADER = """\
 vasp.6.4.2 20Jul23 (build Apr 28 2025 14:18:53) complex
 executed on             LinuxGNU date 2025.08.16  12:18:21

 INCAR:
   SYSTEM = FeO test
   ISMEAR = 0
   SIGMA = 0.05
   PREC = Accurate
   LREAL = Auto
   ADDGRID = .TRUE.
   ENCUT = 520
   GGA = PE
   ISIF = 3
   IBRION = 2
   NSW = 50
   EDIFF = 1.E-6
 POTCAR:    PAW_PBE Fe_pv 06Sep2000
 POTCAR:    PAW_PBE O 08Apr2002
   VRHFIN =Fe: 3d7 4s1
   LEXCH  = PE
   TITEL  = PAW_PBE Fe_pv 06Sep2000
 POTCAR:    PAW_PBE O 08Apr2002
   LEXCH  = PE
   TITEL  = PAW_PBE O 08Apr2002

 Automatic generation of k-mesh.
 generate k-points for:    8    8    8
   k-points           NKPTS =     20   k-points in BZ     NKDIM =     20

 Startparameter for this run:
   PREC   = accura    normal or accurate
   ISPIN  =      1    spin polarized calculation?
   LSORBIT =      F    spin-orbit coupling
   LASPH  =      F    aspherical Exc in radial PAW
 Electronic Relaxation 1
   ENCUT  =  520.0 eV
   EDIFF  = 0.1E-05   stopping-criterion for ELM
   LREAL  =      T    real-space projection
   ADDGRID=      T    additional support grid
 Ionic relaxation
   NSW    =     50    number of steps for IOM
   IBRION =      2    ionic relax
   ISIF   =      3    stress and relaxation
   ISMEAR =     0;   SIGMA  =   0.05  broadening in eV
 Exchange correlation treatment:
   GGA     =    PE    GGA type
   LHFCALC =     F    Hartree Fock is set to
   AEXX    =    0.0000 exact exchange contribution
--------------------------------------- Iteration    1(   1)  ---------------
   ... trajectory body we must NOT read ...
   ENCUT = 99999
"""

# An HSE06 hybrid +U run: LHFCALC=T, AEXX=0.25, HFSCREEN=0.2, an LDA+U block, PRECFOCK/NKRED.
HSE_U_HEADER = """\
 vasp.6.3.2 20Jan22
 INCAR:
   LHFCALC = .TRUE.
   HFSCREEN = 0.2
   AEXX = 0.25
   LDAU = .TRUE.
   LDAUTYPE = 2
   LDAUL = 2 -1
   LDAUU = 4.0 0.0
   LDAUJ = 0.0 0.0
   ENCUT = 400
   PRECFOCK = Fast
   NKRED = 2
 POTCAR:    PAW_PBE Ni_pv 02Aug2007
   TITEL  = PAW_PBE Ni_pv 02Aug2007
 POTCAR:    PAW_PBE O 08Apr2002
   TITEL  = PAW_PBE O 08Apr2002
   k-points           NKPTS =      1
 Startparameter for this run:
   ISPIN  =      2    spin polarized calculation?
   LSORBIT =      F
 LDA+U is selected, type is set to LDAUTYPE =  2
 Exchange correlation treatment:
   GGA     =    PE    GGA type
   LHFCALC =     T    Hartree Fock is set to
   HFSCREEN=    0.2000 screening length
   AEXX    =    0.2500 exact exchange contribution
   PRECFOCK=    Fast
   NKRED   =      2
--------------------------------------- Iteration    1(   1)  ---------------
"""

# Old VASP 5.3.3: no TITEL lines, no INCAR echo body, POTCAR listed only via `POTCAR:` lines.
OLD_HEADER = """\
 vasp.5.3.3 18Dez12gamma-only
 INCAR:
 POTCAR:    PAW_PBE Ni 02Aug2007
 POTCAR:    PAW_PBE Ni 02Aug2007
 Startparameter for this run:
   ISPIN  =      2    spin polarized calculation?
 Exchange correlation treatment:
   GGA     =    91    GGA type
   LHFCALC =     F    Hartree Fock is set to
   AEXX    =    0.0000
--------------------------------------- Iteration    1(   1)  ---------------
"""


# --- value coercion --------------------------------------------------------------------

def test_coerce_value_types():
    assert _coerce_value(".TRUE.") is True
    assert _coerce_value("F") is False
    assert _coerce_value("520") == 520
    assert _coerce_value("1.E-6") == 1e-6
    assert _coerce_value("-.1E-04") == -1e-5
    assert _coerce_value("Auto") == "Auto"
    assert _coerce_value("4.0 0.0") == [4.0, 0.0]          # numeric array
    assert _coerce_value("2 -1") == [2, -1]
    assert _coerce_value("unknown system") == "unknown system"  # non-numeric multi-token


# --- INCAR echo + effective blocks -----------------------------------------------------

def test_incar_echo_parsed_as_user_tags():
    incar = _parse_incar_echo(PBE_HEADER.splitlines())
    assert incar["ENCUT"] == 520
    assert incar["ADDGRID"] is True
    assert incar["LREAL"] == "Auto"           # user value, verbatim
    assert incar["ISIF"] == 3
    assert incar["EDIFF"] == 1e-6
    assert "SYSTEM" in incar
    # must STOP at the first POTCAR: line (never absorb the trajectory body's ENCUT=99999)
    assert incar["ENCUT"] != 99999


def test_effective_block_parsed_and_stops_are_scoped():
    eff = _parse_effective(PBE_HEADER.splitlines())
    assert eff["LREAL"] is True                # RESOLVED value (Auto -> T), unlike the echo
    assert eff["GGA"] == "PE"
    assert eff["LHFCALC"] is False
    assert eff["AEXX"] == 0.0
    assert eff["ISMEAR"] == 0 and eff["SIGMA"] == 0.05   # ';'-packed line split correctly


def test_header_reader_stops_at_first_ionic_step(tmp_path):
    p = tmp_path / "OUTCAR"
    p.write_text(PBE_HEADER)
    lines = read_header_lines(p)
    assert not any("Iteration    1(   1)" in ln for ln in lines)
    assert not any("99999" in ln for ln in lines)   # trajectory body never reached


# --- run_type / functional classification (validated vs pymatgen 2026.5.4) -------------

def _run_type(eff: dict, incar: dict, ldau: bool, potcar=("PAW_PBE Fe 06Sep2000",)) -> str:
    return classify_run_type(_classify_inputs(incar, eff, ldau), list(potcar))


def test_plain_gga_not_mislabeled_hf_or_plus_u():
    """THE safety property: a plain PBE OUTCAR (no HF block, no LDAU line) must classify PBE,
    never HF (from AEXX's 1.0 default) or +U (from LDAU's True default)."""
    assert _run_type({"GGA": "PE", "LHFCALC": False, "AEXX": 0.0, "HFSCREEN": 0.0}, {}, False) == "PBE"
    # even with NOTHING in the effective dict, the completion defaults keep it safe:
    assert _run_type({}, {"GGA": "PE"}, False) == "PBE"


def test_classifier_matrix():
    cases = [
        ({"GGA": "PS"}, {}, False, "PBEsol"),
        ({"GGA": "RE"}, {}, False, "revPBE"),
        ({"GGA": "--"}, {}, False, "GGA"),
        ({"LHFCALC": True, "AEXX": 0.25, "HFSCREEN": 0.2}, {}, False, "HSE06"),
        ({"LHFCALC": True, "AEXX": 0.25, "HFSCREEN": 0.3}, {}, False, "HSE03"),
        ({"LHFCALC": True, "AEXX": 1.0}, {}, False, "HF"),
        ({"LHFCALC": True, "AEXX": 0.2}, {}, False, "B3LYP"),
        ({"LHFCALC": True, "AEXX": 0.25, "HFSCREEN": 0.0}, {}, False,
         "PBEO or other Hybrid Functional"),
        ({"GGA": "PE"}, {"METAGGA": "SCAN"}, False, "SCAN"),
        ({"GGA": "PE"}, {"METAGGA": "R2SCAN"}, False, "R2SCAN"),
        ({"GGA": "PE"}, {}, True, "PBE+U"),
        ({"GGA": "PE"}, {"IVDW": 12}, False, "PBE+vdW-DFT-D3-BJ"),
        ({"GGA": "PE", "LUSE_VDW": True}, {}, False, "PBE+rVV10"),
        ({"GGA": "ZZ"}, {}, False, "ZZ"),           # unknown tag passes through, as pymatgen
    ]
    for eff, incar, ldau, expected in cases:
        # completion defaults for the hybrid keys the effective dict omits
        eff = {"AEXX": 0.0, "HFSCREEN": 0.0, "LHFCALC": False, **eff}
        assert _run_type(eff, incar, ldau) == expected, (eff, incar, ldau)


# --- POTCAR titels: TITEL preferred, POTCAR: fallback for old VASP ---------------------

def test_titels_from_titel_lines():
    assert _titels_from_lines(PBE_HEADER.splitlines()) == [
        "PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"]


def test_titels_old_vasp_potcar_line_fallback_dedups():
    # OLD_HEADER lists `POTCAR: PAW_PBE Ni 02Aug2007` twice and has NO TITEL line.
    assert _titels_from_lines(OLD_HEADER.splitlines()) == ["PAW_PBE Ni 02Aug2007"]


# --- LDA+U ------------------------------------------------------------------------------

def test_ldau_block_recovered():
    lines = HSE_U_HEADER.splitlines()
    incar = _parse_incar_echo(lines)
    eff = _parse_effective(lines)
    on, hub = _parse_ldau(lines, incar, eff)
    assert on is True
    assert hub["LDAUTYPE"] == 2
    assert hub["LDAUU"] == [4.0, 0.0]
    assert hub["LDAUL"] == [2, -1]


def test_no_ldau_is_off():
    lines = PBE_HEADER.splitlines()
    on, hub = _parse_ldau(lines, _parse_incar_echo(lines), _parse_effective(lines))
    assert on is False and hub is None


# --- full calc_parameters assembly (vasprun-schema parity) -----------------------------

def test_build_calc_parameters_pbe_full_schema():
    cp = build_calc_parameters(PBE_HEADER.splitlines())
    # every key the vasprun path emits must be present
    for key in ("code", "code_version", "run_type", "functional", "hubbard_u",
                "spin_polarized", "encut", "ediff", "ismear", "sigma", "ispin",
                "kpoints", "potcar_symbols", "potcar_spec", "potcar_set_hash",
                "incar", "parameters"):
        assert key in cp, key
    assert cp["code"] == "vasp" and cp["code_version"] == "6.4.2"
    assert cp["run_type"] == "PBE" and cp["functional"] == "PBE"
    assert cp["parsed_from"] == "outcar_header"
    assert cp["encut"] == 520 and cp["ismear"] == 0 and cp["sigma"] == 0.05
    assert cp["spin_polarized"] is False        # ISPIN == 1
    assert cp["kpoints"]["num_kpts"] == 20
    assert cp["kpoints"]["kpts"] == [[8, 8, 8]]
    assert cp["potcar_symbols"] == ["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"]
    assert cp["potcar_set_hash"] is not None
    assert cp["xc_lexch"] == ["PE"]
    assert cp["hubbard_u"] is None


def test_build_calc_parameters_hse_u():
    cp = build_calc_parameters(HSE_U_HEADER.splitlines())
    assert cp["run_type"] == "HSE06+U"
    assert cp["functional"] == "HSE06"
    assert cp["spin_polarized"] is True         # ISPIN == 2
    assert cp["hubbard_u"]["LDAUU"] == [4.0, 0.0]
    assert cp["parameters"]["NKRED"] == 2
    assert cp["parameters"]["PRECFOCK"] == "Fast"


def test_build_calc_parameters_old_vasp_degrades_gracefully():
    cp = build_calc_parameters(OLD_HEADER.splitlines())
    assert cp["code_version"] == "5.3.3"
    # GGA=91 is NOT in pymatgen's GGA_TYPES table, so it passes through as the raw tag "91"
    # — we deliberately match pymatgen exactly (the vasprun path would label it "91" too).
    assert cp["run_type"] == "91"
    assert cp["potcar_symbols"] == ["PAW_PBE Ni 02Aug2007"]  # POTCAR: fallback
    assert cp["spin_polarized"] is True


# --- file reader: compression + robustness ---------------------------------------------

def test_parse_outcar_header_gzip(tmp_path):
    p = tmp_path / "OUTCAR.gz"
    with gzip.open(p, "wt") as fh:
        fh.write(PBE_HEADER)
    cp = parse_outcar_header(p)
    assert cp is not None and cp["run_type"] == "PBE" and cp["encut"] == 520


def test_parse_outcar_header_missing_returns_none(tmp_path):
    assert parse_outcar_header(tmp_path / "nope") is None


def test_parse_outcar_header_empty_returns_none(tmp_path):
    p = tmp_path / "OUTCAR"
    p.write_text("")
    assert parse_outcar_header(p) is None


def test_garbage_header_is_partial_not_crash(tmp_path):
    p = tmp_path / "OUTCAR"
    p.write_text("not an outcar\nrandom text\n")
    cp = parse_outcar_header(p)
    assert cp is not None                        # partial > null; never raises
    assert cp["potcar_symbols"] == []


# =======================================================================================
# Real-world INCAR/OUTCAR variety (observed across 30 real OUTCARs, VASP 5.4.4-6.4.3):
# empty INCAR echoes, the multi-species LDA+U echo block, effective-only hybrids, the
# IVDW faithfulness fix, mixed-case METAGGA, and the flexible INCAR syntaxes pymatgen's
# Incar parser must swallow. These lock in the behaviours the live cross-check validated.
# =======================================================================================

# LDA+U where the INCAR echo carries NO LDAU tags — the U/J arrays live only in VASP's
# "LDA+U is selected" echo block (the multi-species "for each species" prose form).
LDAU_ECHO_HEADER = """\
 vasp.6.3.0 20Jan22
 INCAR:
   ENCUT = 520
   ISMEAR = 0
 POTCAR:    PAW_PBE Fe_pv 06Sep2000
   TITEL  = PAW_PBE Fe_pv 06Sep2000
 POTCAR:    PAW_PBE O 08Apr2002
   TITEL  = PAW_PBE O 08Apr2002
 LDA+U is selected, type is set to LDAUTYPE =  2
   angular momentum for each species LDAUL =  2  -1
   U (eV)           for each species LDAUU =  5.30  0.00
   J (eV)           for each species LDAUJ =  0.00  0.00
 Startparameter for this run:
   ISPIN  =      2
 Exchange correlation treatment:
   GGA     =    PS    GGA type
   LHFCALC =     F
   AEXX    =    0.0000
--------------------------------------- Iteration    1(   1)  ---------------
"""

# A hybrid whose INCAR echo is EMPTY (VASP 5.4.4 style) — LHFCALC/AEXX/HFSCREEN appear only
# in the resolved "Exchange correlation treatment:" block. Must still classify HSE06.
EMPTY_ECHO_HYBRID = """\
 vasp.5.4.4.18Apr17
 INCAR:
 POTCAR:    PAW_PBE Si 05Jan2001
 Startparameter for this run:
   ISPIN  =      1
 Exchange correlation treatment:
   GGA     =    PE    GGA type
   LHFCALC =     T    Hartree Fock is set to
   HFSCREEN=    0.2000 screening length
   AEXX    =    0.2500 exact exchange contribution
--------------------------------------- Iteration    1(   1)  ---------------
"""


def test_ldau_recovered_from_echo_block_multispecies():
    on, hub = _parse_ldau(LDAU_ECHO_HEADER.splitlines(),
                          incar={}, eff={})   # forces the OUTCAR-echo fallback path
    assert on is True
    assert hub["LDAUTYPE"] == 2
    assert hub["LDAUL"] == [2, -1]            # the FULL per-species array, not just the first
    assert hub["LDAUU"] == [5.3, 0.0]
    assert hub["LDAUJ"] == [0.0, 0.0]
    cp = build_calc_parameters(LDAU_ECHO_HEADER.splitlines())
    assert cp["run_type"] == "PBEsol+U" and cp["functional"] == "PBEsol"


def test_hybrid_recovered_from_effective_block_only():
    cp = build_calc_parameters(EMPTY_ECHO_HYBRID.splitlines())
    assert cp["incar"] == {}                  # empty INCAR echo
    assert cp["run_type"] == "HSE06"          # ...but the hybrid is recovered from parameters
    assert cp["parameters"]["LHFCALC"] is True and cp["parameters"]["AEXX"] == 0.25


def test_effective_ivdw_default_zero_does_not_add_spurious_vdw():
    """The effective block echoes the resolved default IVDW=0; it must NOT become
    '+vdW-no-correction' (pymatgen reads IVDW from the user INCAR only)."""
    header = (
        " vasp.6.4.2\n INCAR:\n   ENCUT = 400\n"
        " POTCAR:    PAW_PBE C 08Apr2002\n   TITEL  = PAW_PBE C 08Apr2002\n"
        " Startparameter for this run:\n   ISPIN = 1\n"
        " Exchange correlation treatment:\n   GGA     =    PE\n   LHFCALC =     F\n"
        "   AEXX    =    0.0000\n   IVDW    =      0\n"
        "--------------------------------------- Iteration    1(   1)  -----\n"
    )
    cp = build_calc_parameters(header.splitlines())
    assert cp["run_type"] == "PBE"            # NOT "PBE+vdW-no-correction"


def test_user_set_ivdw_in_echo_does_add_vdw():
    header = (
        " vasp.6.4.2\n INCAR:\n   ENCUT = 400\n   IVDW = 12\n"
        " POTCAR:    PAW_PBE C 08Apr2002\n   TITEL  = PAW_PBE C 08Apr2002\n"
        " Startparameter for this run:\n   ISPIN = 1\n"
        " Exchange correlation treatment:\n   GGA     =    PE\n   LHFCALC =     F\n   AEXX = 0.0\n"
        "--------------------------------------- Iteration    1(   1)  -----\n"
    )
    cp = build_calc_parameters(header.splitlines())
    assert cp["run_type"] == "PBE+vdW-DFT-D3-BJ"   # user-set IVDW=12 -> real dispersion


def test_mixed_case_metagga_tag():
    # real OUTCAR observed METAGGA=R2scan (mixed case) in the INCAR echo
    header = (
        " vasp.6.4.3\n INCAR:\n   METAGGA = R2scan\n   ENCUT = 500\n"
        " POTCAR:    PAW_PBE O 08Apr2002\n   TITEL  = PAW_PBE O 08Apr2002\n"
        " Startparameter for this run:\n   ISPIN = 1\n"
        " Exchange correlation treatment:\n   GGA     =    PE\n   LHFCALC = F\n   AEXX = 0.0\n"
        "--------------------------------------- Iteration    1(   1)  -----\n"
    )
    assert build_calc_parameters(header.splitlines())["run_type"] == "R2SCAN"


def test_flexible_incar_syntax_via_pymatgen_incar():
    """VASP INCAR is flexible: '#'/'!' comments, ';'-separated tags, sci-notation, string
    values with spaces, and 'N*x' repeat arrays. pymatgen's Incar parser handles them all."""
    header = (
        " vasp.6.4.2\n"
        " INCAR:\n"
        "   SYSTEM = my big cell   # a comment\n"
        "   ISMEAR = 0; SIGMA = 0.05    ! two tags, one line\n"
        "   EDIFF = 1.E-06\n"
        "   MAGMOM = 3*0.6 2*0.0\n"
        "   LREAL = Auto\n"
        " POTCAR:    PAW_PBE Fe 06Sep2000\n   TITEL  = PAW_PBE Fe 06Sep2000\n"
        " Startparameter for this run:\n   ISPIN = 2\n"
        " Exchange correlation treatment:\n   GGA = PE\n   LHFCALC = F\n   AEXX = 0.0\n"
        "--------------------------------------- Iteration    1(   1)  -----\n"
    )
    inc = build_calc_parameters(header.splitlines())["incar"]
    assert inc["SYSTEM"] == "my big cell"          # spaced string, comment stripped
    assert inc["ISMEAR"] == 0 and inc["SIGMA"] == 0.05   # ';'-split
    assert inc["EDIFF"] == 1e-6                    # Fortran sci-notation
    assert inc["MAGMOM"] == [0.6, 0.6, 0.6, 0.0, 0.0]    # N*x expansion (pymatgen)
    assert inc["LREAL"] == "Auto"


def test_incar_echo_stdlib_fallback_handles_common_syntax():
    """The stdlib fallback (used when pymatgen is unavailable) still parses the common
    forms: comments, ';'-separated tags, bools, ints, floats, and numeric arrays."""
    from zenodo_harvest.outcar_params import _incar_echo_lines, _incar_from_lines_stdlib
    block = _incar_echo_lines(PBE_HEADER.splitlines())
    inc = _incar_from_lines_stdlib(block)
    assert inc["ENCUT"] == 520 and inc["ADDGRID"] is True and inc["LREAL"] == "Auto"
    assert inc["ISIF"] == 3
    # comment + semicolon handling in the fallback directly
    inc2 = _incar_from_lines_stdlib(["   ISMEAR = 0; SIGMA = 0.05  # c", "   LDAUU = 4.0 0.0"])
    assert inc2["ISMEAR"] == 0 and inc2["SIGMA"] == 0.05 and inc2["LDAUU"] == [4.0, 0.0]


def test_bz2_and_xz_compressed_headers(tmp_path):
    import bz2
    import lzma
    for opener, suffix in ((bz2.open, ".bz2"), (lzma.open, ".xz")):
        p = tmp_path / f"OUTCAR{suffix}"
        with opener(p, "wt") as fh:
            fh.write(PBE_HEADER)
        cp = parse_outcar_header(p)
        assert cp is not None and cp["run_type"] == "PBE", suffix
