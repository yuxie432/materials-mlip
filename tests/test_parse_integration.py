"""Real pymatgen/ASE parse -> store -> read-back -> verify, on vendored VASP fixtures.

The rest of the suite mocks the pymatgen/ASE parse (test_harvest.py is deliberately
dependency-free), so nothing exercised the ACTUAL parse integration — which is exactly how
the ``electronic_converged`` round-trip bug slipped through. These tests parse two small
real outputs end to end and assert the training-label fidelity that matters:

* vasprun_dfpt.xml  -> the pymatgen.Vasprun path;
* OUTCAR_example_1  -> the ASE ``vasp-out`` fallback path (OUTCAR-only).

Both are small real outputs bundled with the installed ASE (ase/test/testdata/vasp) — used
from there rather than committed, so no VASP data lives in the repo. Skips cleanly if
pymatgen/ase (or those bundled fixtures) are unavailable. The core extxyz round-trip
invariant this guards is ALSO covered dependency-free in tests/test_store.py.

Run: ``python -m pytest tests/test_parse_integration.py -q`` from the repo root.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

pytest.importorskip("pymatgen")
ase = pytest.importorskip("ase")

from ase.io import read as ase_read  # noqa: E402

from zenodo_harvest.dataset_ops import verify_dataset  # noqa: E402
from zenodo_harvest.manifest import RejectionLogger, read_jsonl  # noqa: E402
from zenodo_harvest.parse import parse, parse_calc_unit  # noqa: E402
from zenodo_harvest.store import ShardedExtxyzWriter  # noqa: E402

# Small real VASP outputs bundled with the installed ASE (ase/test/testdata/vasp) — used
# from there rather than committed, so no VASP data lives in git. Skip if this ASE build
# does not ship them; the core round-trip invariant is covered dependency-free in
# tests/test_store.py::test_electronic_converged_none_is_omitted_not_written_as_true.
FIXTURES = Path(ase.__file__).parent / "test" / "testdata" / "vasp"
if not all((FIXTURES / f).is_file() for f in ("vasprun_dfpt.xml", "OUTCAR_example_1")):
    pytest.skip("ASE VASP test fixtures not available", allow_module_level=True)


def _stage(tmp_path: Path, recid: str, sub: str, fixture: str, role: str) -> tuple[dict, dict]:
    """Copy a fixture into a raw ``<recid>/extracted/<sub>/`` layout; return (unit, base_meta).

    ``role`` is "vasprun" or "outcar" — the file is named canonically so the role/primary
    detection matches what fetch would have produced.
    """
    names = {"vasprun": "vasprun.xml", "outcar": "OUTCAR"}
    extracted = tmp_path / "raw" / recid / "extracted"
    calc = extracted / sub
    calc.mkdir(parents=True, exist_ok=True)
    dst = calc / names[role]
    shutil.copy(FIXTURES / fixture, dst)
    unit = {"dir": str(calc), role: str(dst)}
    base_meta = {"provenance": {"source": "zenodo", "record_id": recid,
                                "license": "cc-by-4.0", "resource_type": "dataset"},
                 "_extracted_root": str(extracted)}
    return unit, base_meta


# --------------------------------------------------------------------------- #
# vasprun.xml path: frames + labels + the SCF convergence MAGNITUDE            #
# --------------------------------------------------------------------------- #

def test_vasprun_frames_labels_and_scf_dE(tmp_path):
    unit, base_meta = _stage(tmp_path, "vr1", "calc", "vasprun_dfpt.xml", "vasprun")
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    result = parse_calc_unit(unit, base_meta, {}, rej)
    rej.close()
    assert result is not None
    frames, meta = result
    assert len(frames) >= 1
    assert meta["parser"] == "pymatgen.Vasprun"

    f = frames[-1]  # final ionic step
    assert "REF_energy" in f.info                 # sigma->0 energy (MACE key)
    assert "REF_forces" in f.arrays               # per-atom forces (MACE key)
    assert "REF_stress" in f.info and len(f.info["REF_stress"]) == 6   # Voigt-6, ASE units
    assert f.info["electronic_converged"] in (True, False)  # a REAL bool, not None/absent

    # THE convergence-magnitude requirement (mentor): scf_dE == |E[-1] - E[-2]| of the
    # final ionic step's last two ELECTRONIC steps (e_0_energy). Recompute independently
    # from pymatgen's own parse and cross-check both the calc-level and per-frame values.
    from pymatgen.io.vasp.outputs import Vasprun
    v = Vasprun(str(unit["vasprun"]), parse_dos=False, parse_eigen=False,
                parse_projected_eigen=False, parse_potcar_file=False,
                exception_on_bad_xml=False)
    esteps = v.ionic_steps[-1]["electronic_steps"]
    assert len(esteps) >= 2, "fixture must have >=2 electronic steps to test the magnitude"
    expected_dE = abs(esteps[-1]["e_0_energy"] - esteps[-2]["e_0_energy"])
    assert meta["quality"]["scf_dE"] == pytest.approx(expected_dE)          # calc-level
    assert meta["quality"]["electronic_converged"] == bool(v.converged_electronic)
    # the FINAL frame is the final ionic step, so its own per-frame scf_dE matches too
    assert f.info["scf_dE"] == pytest.approx(expected_dE)


# --------------------------------------------------------------------------- #
# OUTCAR path: the electronic_converged round-trip regression (#1)            #
# --------------------------------------------------------------------------- #

def test_outcar_unknown_convergence_reads_back_as_none_not_true(tmp_path):
    # The ASE OUTCAR path cannot know SCF convergence, so it must be recorded "unknown".
    # Regression: it was written as None -> serialised as a bare extxyz key -> read back as
    # True (an unknown-convergence frame silently relabelled converged). It must now be
    # OMITTED, so a round-trip through the real shard writer yields None/absent.
    unit, base_meta = _stage(tmp_path, "oc1", "run", "OUTCAR_example_1", "outcar")
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    result = parse_calc_unit(unit, base_meta, {}, rej)
    rej.close()
    assert result is not None
    frames, meta = result
    assert meta["parser"] == "ase.OUTCAR"
    assert meta["quality"]["electronic_converged"] is None      # unknown in metadata too
    # in-memory: the key must be absent (not present-with-None)
    assert "electronic_converged" not in frames[0].info

    # round-trip through the real gzipped-shard writer + ASE reader
    ds = tmp_path / "ds"
    with ShardedExtxyzWriter(ds) as xyz:
        for fr in frames:
            xyz.write(fr)
        xyz.flush()
    shard = next(ds.glob("shard-*.extxyz.gz"))
    back = ase_read(shard, index=":", format="extxyz")
    for a in back:
        # get() must return None ("unknown") — never True, which would mean "converged".
        assert a.info.get("electronic_converged") is None, a.info.get("electronic_converged")
    assert "REF_energy" in back[0].info and "REF_forces" in back[0].arrays


def test_known_convergence_true_false_survive_roundtrip(tmp_path):
    # The flip side: a KNOWN verdict (vasprun path) must survive the round-trip as the same
    # bool — the fix omits only None, it must not drop real True/False.
    unit, base_meta = _stage(tmp_path, "vr2", "calc", "vasprun_dfpt.xml", "vasprun")
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    frames, _meta = parse_calc_unit(unit, base_meta, {}, rej)
    rej.close()
    ds = tmp_path / "ds"
    with ShardedExtxyzWriter(ds) as xyz:
        for fr in frames:
            xyz.write(fr)
        xyz.flush()
    back = ase_read(next(ds.glob("shard-*.extxyz.gz")), index=":", format="extxyz")
    for orig, got in zip(frames, back):
        assert got.info.get("electronic_converged") == orig.info["electronic_converged"]
        assert isinstance(got.info.get("electronic_converged"), bool)


# --------------------------------------------------------------------------- #
# Full stage-3+4 run over both fixtures -> a clean, verifiable dataset         #
# --------------------------------------------------------------------------- #

def test_parse_then_verify_end_to_end(tmp_path):
    raw = tmp_path / "raw"
    # two records: one vasprun-only, one OUTCAR-only
    u1, _ = _stage(tmp_path, "vr", "calc", "vasprun_dfpt.xml", "vasprun")
    u2, _ = _stage(tmp_path, "oc", "run", "OUTCAR_example_1", "outcar")

    def rel(p: str) -> str:
        return str(Path(p).relative_to(raw))

    manifest = tmp_path / "fetched.jsonl"
    import json
    with manifest.open("w") as fh:
        fh.write(json.dumps({
            "recid": "vr", "provenance": {"source": "zenodo", "record_id": "vr",
                                          "license": "cc-by-4.0", "resource_type": "dataset"},
            "local_dir": "vr", "n_calc_units": 1,
            "calc_units": [{"dir": rel(u1["dir"]), "vasprun": rel(u1["vasprun"])}],
            "availability": {"charge_density": False}}) + "\n")
        fh.write(json.dumps({
            "recid": "oc", "provenance": {"source": "zenodo", "record_id": "oc",
                                          "license": "mit", "resource_type": "dataset"},
            "local_dir": "oc", "n_calc_units": 1,
            "calc_units": [{"dir": rel(u2["dir"]), "outcar": rel(u2["outcar"])}],
            "availability": {}}) + "\n")

    ds = tmp_path / "dataset"
    stats = parse(manifest, dataset_dir=ds, rejections_path=tmp_path / "rej.jsonl", raw_dir=raw)
    assert stats["calcs_parsed"] == 2 and stats["frames"] >= 2

    out = verify_dataset(ds)
    assert out["ok"] is True                                   # clean frame_id bijection
    assert out["integrity"]["n_frames_metadata"] == out["integrity"]["n_frames_on_disk"]
    # both parsers represented, stress present on both paths, licenses recorded
    assert out["stats"]["frames_by_parser"].keys() >= {"pymatgen.Vasprun", "ase.OUTCAR"}
    assert out["stats"]["total_n_frames_with_stress"] >= 2
    assert set(out["stats"]["frames_by_license"]) == {"cc-by-4.0", "mit"}
    # convergence split recorded honestly: the vasprun calc known, the OUTCAR calc unknown
    conv = out["stats"]["calcs_by_electronic_converged"]
    assert conv["null"] == 1 and (conv["true"] + conv["false"]) == 1

    # metadata carries the mandatory provenance + full calc parameters
    recs = {r["calc_id"]: r for r in read_jsonl(ds / "metadata.jsonl")}
    vr = next(r for cid, r in recs.items() if r["parser"] == "pymatgen.Vasprun")
    assert vr["provenance"]["record_id"] == "vr" and vr["provenance"]["file_path"]
    assert vr["calc_parameters"]["run_type"] and "incar" in vr["calc_parameters"]
    assert "potcar_set_hash" in vr["calc_parameters"]


def test_resume_skips_previously_rejected_calc(tmp_path):
    """A calc that fails to parse must be re-skipped on resume, not re-run + re-rejected."""
    import json
    raw = tmp_path / "raw"
    calc = raw / "bad" / "extracted" / "c"
    calc.mkdir(parents=True)
    (calc / "OUTCAR").write_text("this is not a valid OUTCAR\n" * 3)  # ASE cannot parse -> rejected
    manifest = tmp_path / "fetched.jsonl"
    manifest.write_text(json.dumps({
        "recid": "bad",
        "provenance": {"source": "zenodo", "record_id": "bad",
                       "license": "cc-by-4.0", "resource_type": "dataset"},
        "local_dir": "bad", "n_calc_units": 1,
        "calc_units": [{"dir": "bad/extracted/c", "outcar": "bad/extracted/c/OUTCAR"}],
        "availability": {}}) + "\n")
    ds, rej = tmp_path / "dataset", tmp_path / "rej.jsonl"

    s1 = parse(manifest, dataset_dir=ds, rejections_path=rej, raw_dir=raw)
    assert s1["calcs_parsed"] == 0 and s1["skipped_rejected"] == 0
    r1 = list(read_jsonl(rej))
    assert len(r1) == 1 and r1[0]["reason"] in {"outcar_parse_error", "no_frames"}

    # resume: the known-bad calc is SKIPPED (not re-parsed), no new rejection line appended
    s2 = parse(manifest, dataset_dir=ds, rejections_path=rej, raw_dir=raw)
    assert s2["skipped_rejected"] == 1 and s2["calcs_parsed"] == 0
    assert len(list(read_jsonl(rej))) == 1

    # retry_rejected re-attempts it (and re-rejects, appending one more line)
    s3 = parse(manifest, dataset_dir=ds, rejections_path=rej, raw_dir=raw, retry_rejected=True)
    assert s3["skipped_rejected"] == 0
    assert len(list(read_jsonl(rej))) == 2
