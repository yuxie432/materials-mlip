"""pymatgen numeric-ALGO bug: a valid vasprun.xml whose INCAR gives ``ALGO`` as a number.

``Vasprun.__init__`` -> ``converged_electronic`` -> ``self.incar.get("ALGO", "").lower()`` raised
``AttributeError: 'int' object has no attribute 'lower'`` and the calc was rejected as
``vasprun_parse_error`` (NOMAD ~1,380 calcs; Zenodo census T1 record 7506565, 158 calcs, whose
vaspruns carry ``<i type="string" name="ALGO"> 48</i>``). ``parse._guard_numeric_algo`` now
coerces it to its string form inside pymatgen, so such files parse — identically to the same file
with a string ALGO — on every path: in-process, in the forkserver timeout child, and on a
``retry_rejected`` resume (the recovery route for the already-rejected calcs).

Fixture: the real ``vasprun_dfpt.xml`` bundled with ASE, with the numeric ALGO line injected.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pymatgen")
ase = pytest.importorskip("ase")

from zenodo_harvest.manifest import read_jsonl  # noqa: E402
from zenodo_harvest.parse import _guard_numeric_algo, parse, parse_vasprun  # noqa: E402

FIXTURE = Path(ase.__file__).parent / "test" / "testdata" / "vasp" / "vasprun_dfpt.xml"
if not FIXTURE.is_file():
    pytest.skip("ASE VASP test fixtures not available", allow_module_level=True)

NUMERIC = '<incar>\n  <i type="string" name="ALGO"> 48</i>'
STRING = '<incar>\n  <i type="string" name="ALGO">Normal</i>'


def _write(path: Path, incar_open: str) -> Path:
    text = FIXTURE.read_text(encoding="latin-1")
    assert text.count("<incar>") == 1
    path.write_text(text.replace("<incar>", incar_open), encoding="latin-1")
    return path


def test_fixture_reproduces_the_pymatgen_bug_without_the_guard(tmp_path):
    """Plain pymatgen (a fresh interpreter, guard never installed) rejects the numeric-ALGO file —
    so the tests below exercise the real failure, not a fixture that parses anyway."""
    import subprocess
    import sys
    num = _write(tmp_path / "num.xml", NUMERIC)
    code = ("from pymatgen.io.vasp.outputs import Vasprun\n"
            f"Vasprun({str(num)!r}, parse_dos=False, parse_eigen=False, parse_potcar_file=False)\n")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if out.returncode == 0:
        pytest.skip("this pymatgen version no longer has the numeric-ALGO bug")
    assert "'int' object has no attribute 'lower'" in out.stderr


def test_numeric_algo_vasprun_parses_like_string_algo(tmp_path):
    num = _write(tmp_path / "num.xml", NUMERIC)
    ref = _write(tmp_path / "ref.xml", STRING)
    frames_n, meta_n = parse_vasprun(str(num), "zenodo:1:num", None)
    frames_s, meta_s = parse_vasprun(str(ref), "zenodo:1:ref", None)
    assert len(frames_n) == len(frames_s) > 0
    for a, b in zip(frames_n, frames_s):
        assert a.info["REF_energy"] == b.info["REF_energy"]
        assert (a.arrays["REF_forces"] == b.arrays["REF_forces"]).all()
        assert (a.positions == b.positions).all()
    assert meta_n["quality"] == meta_s["quality"]
    assert str(meta_n["calc_parameters"]["incar"]["ALGO"]).strip() == "48"


def test_guard_is_idempotent_and_leaves_string_algo_alone(tmp_path):
    from pymatgen.io.vasp.outputs import Vasprun
    _guard_numeric_algo()
    first = Vasprun.__dict__["converged_electronic"]
    _guard_numeric_algo()
    assert Vasprun.__dict__["converged_electronic"] is first      # not wrapped twice
    v = Vasprun(str(_write(tmp_path / "s.xml", STRING)), parse_dos=False, parse_eigen=False,
                parse_potcar_file=False)
    assert v.incar["ALGO"] == "Normal"
    assert isinstance(v.converged_electronic, bool)


@pytest.mark.parametrize("timeout", [0, 120])        # in-process, and the forkserver child
def test_rejected_numeric_algo_calc_recovered_by_retry_rejected(tmp_path, timeout):
    raw, ds = tmp_path / "raw", tmp_path / "ds"
    calc = raw / "7506565" / "extracted" / "ZrO2" / "NOSYMdir"
    calc.mkdir(parents=True)
    _write(calc / "vasprun.xml", NUMERIC)
    manifest = tmp_path / "fetched.jsonl"
    manifest.write_text(json.dumps({
        "recid": "7506565", "local_dir": "7506565",
        "provenance": {"source": "zenodo", "record_id": "7506565", "license": "cc-by-4.0"},
        "n_calc_units": 1,
        "calc_units": [{"dir": "7506565/extracted/ZrO2/NOSYMdir",
                        "vasprun": "7506565/extracted/ZrO2/NOSYMdir/vasprun.xml"}]}) + "\n")
    rej = tmp_path / "rejections.jsonl"
    # the state the census T1 run left: this calc rejected by the unpatched pymatgen
    rej.write_text(json.dumps({"stage": "parse", "id": "zenodo:7506565:ZrO2/NOSYMdir/vasprun.xml",
                               "reason": "vasprun_parse_error",
                               "detail": "AttributeError: 'int' object has no attribute 'lower'"})
                   + "\n")
    kw = dict(dataset_dir=str(ds), rejections_path=str(rej), raw_dir=str(raw),
              parse_timeout_s=timeout)
    s1 = parse(str(manifest), **kw)                         # default resume: still skipped
    assert s1["skipped_rejected"] == 1 and s1["calcs_parsed"] == 0
    s2 = parse(str(manifest), retry_rejected=True, **kw)    # the recovery
    assert s2["calcs_parsed"] == 1 and s2["frames"] > 0 and s2["rejections"] == 0
    meta = list(read_jsonl(ds / "metadata.jsonl"))
    assert [m["calc_id"] for m in meta] == ["zenodo:7506565:ZrO2/NOSYMdir/vasprun.xml"]
    s3 = parse(str(manifest), retry_rejected=True, **kw)    # idempotent: no duplicate
    assert s3["calcs_parsed"] == 0 and s3["skipped_existing"] == 1
    shutil.rmtree(ds)
