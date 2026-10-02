"""Two parser gaps the census T2 run exposed (2026-10-01), both closed in ``parse.py``:

* ``11234637`` — 337 real OUTCARs rejected with ``UnicodeDecodeError``: VASP prints an uninitialised
  character buffer as the method in "vdW correction parametrized for the method ..." (TS / MBD with a
  hybrid), and ASE opens the OUTCAR strictly as UTF-8. ``_parse_outcar_ase`` now re-reads a copy
  with the invalid bytes replaced and records ``outcar_invalid_utf8_lines``.
* ``20403107`` — 70 vaspruns rejected with ``could not convert string to float: '****************'``:
  VASPsol's ``LAMBDA_D_K`` overflows its field in ``<parameters>``. ``_guard_overflowed_params``
  reads an all-asterisk scalar parameter as ``None`` (pymatgen already does so for ``RANDOM_SEED``).

Fixtures: ASE's bundled ``OUTCAR_example_1`` and ``vasprun_dfpt.xml`` with the real failure injected
(the garbage line is the byte string of a real 11234637 OUTCAR).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pymatgen")
ase = pytest.importorskip("ase")

from zenodo_harvest.manifest import read_jsonl  # noqa: E402
from zenodo_harvest.parse import (_guard_overflowed_params, _parse_outcar_ase, parse,  # noqa: E402
                                  parse_vasprun)

DATA = Path(ase.__file__).parent / "test" / "testdata" / "vasp"
OUTCAR = DATA / "OUTCAR_example_1"
VASPRUN = DATA / "vasprun_dfpt.xml"
if not (OUTCAR.is_file() and VASPRUN.is_file()):
    pytest.skip("ASE VASP test fixtures not available", allow_module_level=True)

# verbatim from 11234637 .../Ag4/1/pbe0-tshi/AD/OUTCAR (its only non-UTF-8 line)
VDW_GARBAGE = b"  vdW correction parametrized for the method \x00\x00\xc0\x8c%\t\x00\x00\x00\x00"
STARS = '<i name="LAMBDA_D_K">****************</i>'


def _outcar_with(path: Path, mutate) -> Path:
    lines = OUTCAR.read_bytes().split(b"\n")
    mutate(lines)
    path.write_bytes(b"\n".join(lines))
    return path


def _insert_vdw_garbage(lines: list[bytes]) -> None:
    i = next(n for n, line in enumerate(lines) if b"TOTAL-FORCE" in line)
    lines.insert(i - 1, VDW_GARBAGE)


def _vasprun_with_stars(path: Path) -> Path:
    text = VASPRUN.read_text(encoding="latin-1")
    anchor = '<separator name="general" >'
    assert text.count(anchor) == 1
    path.write_text(text.replace(anchor, anchor + "\n   " + STARS), encoding="latin-1")
    return path


def _fresh_interpreter(code: str):
    import subprocess
    import sys
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)


# ---- OUTCAR with non-UTF-8 bytes ------------------------------------------------------------

def test_outcar_fixture_reproduces_the_ase_failure(tmp_path):
    bad = _outcar_with(tmp_path / "OUTCAR", _insert_vdw_garbage)
    out = _fresh_interpreter("from ase.io import read\n"
                             f"read({str(bad)!r}, format='vasp-out', index=':')\n")
    assert out.returncode != 0 and "UnicodeDecodeError" in out.stderr


def test_outcar_with_invalid_bytes_parses_like_the_clean_file(tmp_path):
    bad = _outcar_with(tmp_path / "OUTCAR", _insert_vdw_garbage)
    frames_b, meta_b = _parse_outcar_ase(str(bad), "zenodo:1:bad/OUTCAR")
    frames_c, meta_c = _parse_outcar_ase(str(OUTCAR), "zenodo:1:clean/OUTCAR")
    assert len(frames_b) == len(frames_c) > 0
    for a, b in zip(frames_b, frames_c):
        assert a.info["REF_energy"] == b.info["REF_energy"]
        assert (a.arrays["REF_forces"] == b.arrays["REF_forces"]).all()
        assert (a.positions == b.positions).all()
    assert meta_b["quality"] == meta_c["quality"]
    assert meta_b["calc_parameters"] == meta_c["calc_parameters"]
    assert meta_b["outcar_invalid_utf8_lines"] == 1
    assert "outcar_invalid_utf8_lines" not in meta_c          # a clean file is read as before
    assert bad.read_bytes().count(b"\xc0") == 1               # the source file is never rewritten


def test_invalid_byte_inside_a_number_still_fails(tmp_path):
    """A replaced byte was >= 0x80, so it cannot have been a digit: one inside a force value makes
    the parse fail instead of yielding a silently different number."""
    def corrupt_a_force(lines: list[bytes]) -> None:
        i = next(n for n, line in enumerate(lines) if b"TOTAL-FORCE" in line) + 2
        k = lines[i].rindex(b".") + 3
        lines[i] = lines[i][:k] + b"\xc0" + lines[i][k:]
    bad = _outcar_with(tmp_path / "OUTCAR", corrupt_a_force)
    with pytest.raises(ValueError):
        _parse_outcar_ase(str(bad), "zenodo:1:bad/OUTCAR")


# ---- vasprun with an overflowed parameter ---------------------------------------------------

def test_vasprun_fixture_reproduces_the_pymatgen_failure(tmp_path):
    bad = _vasprun_with_stars(tmp_path / "vasprun.xml")
    out = _fresh_interpreter(
        "from pymatgen.io.vasp.outputs import Vasprun\n"
        f"Vasprun({str(bad)!r}, parse_dos=False, parse_eigen=False, parse_potcar_file=False)\n")
    if out.returncode == 0:
        pytest.skip("this pymatgen version already tolerates an overflowed parameter")
    assert "could not convert string to float: '****************'" in out.stderr


def test_overflowed_parameter_vasprun_parses_like_the_clean_file(tmp_path):
    bad = _vasprun_with_stars(tmp_path / "vasprun.xml")
    frames_b, meta_b = parse_vasprun(str(bad), "zenodo:1:bad", None)
    frames_c, meta_c = parse_vasprun(str(VASPRUN), "zenodo:1:clean", None)
    assert len(frames_b) == len(frames_c) > 0
    for a, b in zip(frames_b, frames_c):
        assert a.info["REF_energy"] == b.info["REF_energy"]
        assert (a.arrays["REF_forces"] == b.arrays["REF_forces"]).all()
    assert meta_b["quality"] == meta_c["quality"]
    assert meta_b["calc_parameters"]["run_type"] == meta_c["calc_parameters"]["run_type"]
    json.dumps(meta_b, allow_nan=False)                       # no NaN smuggled into metadata


def test_overflow_guard_is_idempotent_and_narrow():
    from pymatgen.io.vasp import outputs
    _guard_overflowed_params()
    first = outputs._parse_parameters
    _guard_overflowed_params()
    assert outputs._parse_parameters is first                 # not wrapped twice
    assert first("", "****************") is None
    assert first("int", "  *****  ") is None
    assert first("", "3.5") == 3.5 and first("int", "7") == 7
    for bad in ("abc", "2****", "1.0e*"):                     # anything else still raises
        with pytest.raises(ValueError):
            first("", bad)


# ---- the recovery route: retry_rejected on the already-rejected calcs -----------------------

@pytest.mark.parametrize("case", ["outcar_utf8", "vasprun_stars"])
@pytest.mark.parametrize("timeout", [0, 120])        # in-process, and the forkserver child
def test_rejected_calc_recovered_by_retry_rejected(tmp_path, case, timeout):
    raw, ds = tmp_path / "raw", tmp_path / "ds"
    calc = raw / "11234637" / "extracted" / "Ag4" / "AD"
    calc.mkdir(parents=True)
    if case == "outcar_utf8":
        role, name, reason = "outcar", "OUTCAR", "outcar_parse_error"
        _outcar_with(calc / name, _insert_vdw_garbage)
        detail = "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc0 in position 1: invalid start byte"
    else:
        role, name, reason = "vasprun", "vasprun.xml", "vasprun_parse_error"
        _vasprun_with_stars(calc / name)
        detail = "ValueError: could not convert string to float: '****************'"
    calc_id = f"zenodo:11234637:Ag4/AD/{name}"
    manifest = tmp_path / "fetched.jsonl"
    manifest.write_text(json.dumps({
        "recid": "11234637", "local_dir": "11234637",
        "provenance": {"source": "zenodo", "record_id": "11234637", "license": "cc-by-4.0"},
        "n_calc_units": 1,
        "calc_units": [{"dir": "11234637/extracted/Ag4/AD",
                        role: f"11234637/extracted/Ag4/AD/{name}"}]}) + "\n")
    rej = tmp_path / "rejections.jsonl"
    rej.write_text(json.dumps({"stage": "parse", "id": calc_id, "reason": reason,
                               "detail": detail}) + "\n")
    kw = dict(dataset_dir=str(ds), rejections_path=str(rej), raw_dir=str(raw),
              parse_timeout_s=timeout)
    s1 = parse(str(manifest), **kw)                         # default resume: still skipped
    assert s1["skipped_rejected"] == 1 and s1["calcs_parsed"] == 0
    s2 = parse(str(manifest), retry_rejected=True, **kw)    # the recovery
    assert s2["calcs_parsed"] == 1 and s2["frames"] > 0 and s2["rejections"] == 0
    meta = list(read_jsonl(ds / "metadata.jsonl"))
    assert [m["calc_id"] for m in meta] == [calc_id]
    if case == "outcar_utf8":
        assert meta[0]["outcar_invalid_utf8_lines"] == 1
    s3 = parse(str(manifest), retry_rejected=True, **kw)    # idempotent: no duplicate
    assert s3["calcs_parsed"] == 0 and s3["skipped_existing"] == 1
