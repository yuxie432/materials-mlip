"""Tests for ``dataset_stats`` — the read-only statistics over the harvested datasets.

Needs ``ase`` (shards are written with the harvest's own ``ShardedExtxyzWriter``, so the reader
is tested against the real format); the parameter tests that need pymatgen's POTCAR tables /
MPRelaxSet ``importorskip`` it. Offline.

Run: ``python -m pytest tests/test_dataset_stats.py -q`` from the repo root.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("ase")

from ase import Atoms  # noqa: E402
from ase.build import bulk, fcc111, molecule  # noqa: E402

from dataset_stats import params, structure  # noqa: E402
from dataset_stats.common import EV_A3_TO_GPA, calc_key, concentration, quantiles  # noqa: E402
from dataset_stats.extxyz_fast import (  # noqa: E402
    frame_index,
    parse_comment,
    parse_properties,
    to_flag,
)
from dataset_stats.meta import load_meta, meta_dataset  # noqa: E402
from dataset_stats.report import build_report, subsample_per_calc  # noqa: E402
from dataset_stats.scan import (  # noqa: E402
    F_FORCES,
    F_STRESS,
    MPTRJ_KEYS,
    load_scan,
    scan_dataset,
    scan_shard,
    scan_text,
)
from zenodo_harvest.store import MetadataWriter, ShardedExtxyzWriter  # noqa: E402


# --- text reader -------------------------------------------------------------------------------
def test_parse_comment_quoted_and_bare():
    line = ('Lattice="1 0 0 0 1 0 0 0 1" Properties=species:S:1:pos:R:3 REF_energy=-1.5 '
            'calc_id="zenodo:1:a dir/vasprun.xml" frame_id=x#3 bare pbc="T T F"')
    d = parse_comment(line)
    assert d["Lattice"] == "1 0 0 0 1 0 0 0 1"
    assert d["calc_id"] == "zenodo:1:a dir/vasprun.xml"
    assert d["REF_energy"] == "-1.5" and d["frame_id"] == "x#3"
    assert "bare" not in d and d["pbc"] == "T T F"


def test_parse_properties_and_flags():
    cols, n = parse_properties("species:S:1:pos:R:3:move_mask:L:3:REF_forces:R:3")
    assert cols["pos"] == (1, 3, "R") and cols["REF_forces"] == (7, 3, "R") and n == 10
    assert (to_flag("T"), to_flag("False"), to_flag(None)) == (1, 0, -1)


def test_frame_index_stops_at_torn_tail():
    lines = ["2", "c", "H 0 0 0", "H 0 0 1", "", "3", "c", "H 0 0 0"]
    idx, torn = frame_index(lines)
    assert idx == [(0, 2)] and torn


# --- structure descriptors -------------------------------------------------------------------
def test_formula_and_elements():
    assert structure.reduced_formula({"O": 6, "Fe": 4}) == "Fe2O3"
    assert structure.reduced_formula({"Pt": 12}) == "Pt"
    assert structure.chemsys({"O": 1, "H": 2}) == "H-O"
    assert structure.element_of("Fe_pv") == "Fe"
    assert structure.element_of("H1.25") == "H"


@pytest.mark.parametrize("kind,nvac", [("bulk", 0), ("slab", 1), ("molecule", 3)])
def test_vacuum_axes(kind, nvac):
    if kind == "bulk":
        a = bulk("Cu", "fcc", a=3.6)
    elif kind == "slab":
        a = fcc111("Pt", size=(2, 2, 3), vacuum=7.0)
    else:
        a = molecule("H2O")
        a.center(vacuum=6.0)
    gaps = structure.vacuum_gaps(np.array(a.cell), a.positions)
    assert structure.n_vacuum_axes(gaps) == nvac


def test_min_distance_and_space_group():
    a = bulk("NaCl", "rocksalt", a=5.64)
    d = structure.min_distance(np.array(a.cell), a.positions)
    assert d == pytest.approx(2.82, abs=1e-6)
    pytest.importorskip("spglib")
    assert structure.space_group(np.array(a.cell), a.positions, list(a.numbers)) == 225


# --- parameter normalisation -------------------------------------------------------------------
def _cp(**kw):
    base = {"potcar_symbols": ["PAW_PBE Fe_pv 06Sep2000", "PAW_PBE O 08Apr2002"],
            "parameters": {}, "incar": {}, "kpoints": {"style": "Gamma", "kpts": [[4, 4, 2]]}}
    base.update(kw)
    return base


def test_xc_family_and_labels():
    assert params.xc_family(_cp()) == "PBE"  # no GGA tag: the POTCARs decide
    assert params.xc_family(_cp(incar={"GGA": "RP"})) == "RPBE"
    assert params.xc_family(_cp(incar={"GGA": "PS"})) == "PBEsol"
    assert params.xc_family(_cp(incar={"METAGGA": "R2SCAN"})) == "R2SCAN"
    assert params.xc_family(_cp(parameters={"METAGGA": False, "GGA": "--"})) == "PBE"
    hse = _cp(parameters={"LHFCALC": True, "HFSCREEN": 0.2, "AEXX": 0.25})
    assert params.xc_family(hse) == "HSE06"
    pbe0 = _cp(parameters={"LHFCALC": True, "HFSCREEN": 0.0, "AEXX": 0.25})
    assert params.xc_family(pbe0) == "PBE0/hybrid"
    u = _cp(parameters={"LDAU": True, "LDAUU": [5.3, 0.0], "LDAUJ": [0.0, 0.0]},
            incar={"IVDW": 12})
    assert params.hubbard_u(u) == {"Fe": 5.3}
    assert params.vdw_method(u) == "D3-BJ"
    assert params.functional_label(u) == "PBE+U+D3-BJ"
    # the effective block's IVDW=0 default is not a dispersion correction
    assert params.vdw_method(_cp(parameters={"IVDW": 0})) == "none"


@pytest.mark.parametrize("par,incar,want", [
    ({"IBRION": 0, "NSW": 500}, {}, "md"),
    ({"IBRION": 2, "NSW": 50, "ISIF": 2}, {}, "relax"),
    ({"IBRION": 1, "NSW": 50, "ISIF": 3}, {}, "relax-cell"),
    ({"IBRION": -1, "NSW": 0}, {}, "static"),
    ({"IBRION": 6, "NSW": 1}, {}, "phonon"),
    ({"IBRION": 3, "NSW": 100}, {"IMAGES": 5}, "neb"),
    ({"IBRION": -1, "NSW": 0}, {"ICHARG": 11}, "nscf"),
    ({"IBRION": 0, "NSW": 1000}, {"ML_LMLFF": True}, "mlff"),
    ({"NSW": 10}, {}, "md"),  # VASP's default IBRION is 0 when NSW > 1
])
def test_calc_type(par, incar, want):
    assert params.calc_type(_cp(parameters=par, incar=incar)) == want


def test_kpoint_summary():
    style, grid, ksp, nk = params.kpoint_summary(_cp())
    assert (style, grid, ksp) == ("Gamma", (4, 4, 2), None)
    assert params.kpoint_summary(_cp(kpoints={"style": "Automatic", "kpts": [[30]]}))[1] is None


def test_mp_compatibility():
    pytest.importorskip("pymatgen")
    ok = _cp(parameters={"LDAU": True, "LDAUU": [5.3, 0.0]})  # Fe_pv + O, MP's U for Fe-O
    assert params.mp_compatible(ok) == 1
    assert params.mp_compatible(_cp()) == 0  # an oxide of Fe needs U in MP's recipe
    assert params.mp_compatible(_cp(parameters={"LDAU": True, "LDAUU": [4.0, 0.0]})) == 0
    assert params.mp_compatible(_cp(incar={"GGA": "PS"})) == 0
    metal = _cp(potcar_symbols=["PAW_PBE Pt 04Feb2005"])
    assert params.mp_compatible(metal) in (0, 1)  # depends only on the titel's release
    assert params.mp_compatible(_cp(potcar_symbols=[])) == -1


# --- common ------------------------------------------------------------------------------------
def test_concentration_and_quantiles():
    c = concentration(np.array([10.0, 10.0, 10.0, 10.0]))
    assert c["gini"] == pytest.approx(0.0, abs=1e-9)
    assert c["effective_n_inverse_hhi"] == pytest.approx(4.0)
    c = concentration(np.array([97.0, 1.0, 1.0, 1.0]))
    assert c["top1_share"] == pytest.approx(0.97) and c["effective_n_inverse_hhi"] < 1.1
    q = quantiles(np.array([1.0, 2.0, 3.0, np.nan]), qs=(0.5,))
    assert q["p50"] == pytest.approx(2.0)
    qw = quantiles(np.array([1.0, 100.0]), qs=(0.5,), weights=np.array([9.0, 1.0]))
    assert qw["p50"] == pytest.approx(1.0)


def test_subsample_rule():
    # one calc: E/atom steps of 0, -0.004, -0.008, -0.012 (kept: first, and -0.012 vs 0), one
    # single-frame calc
    epa = np.array([0.0, -0.004, -0.008, -0.012, 5.0])
    kept = subsample_per_calc(epa, np.array([0, 4]), np.array([3, 4]), 0.010)
    assert kept.tolist() == [2, 1]


# --- synthetic dataset end to end --------------------------------------------------------------
def _frame(a: Atoms, cid: str, step: int, e: float, rng, *, stress=True, forces=True) -> Atoms:
    a = a.copy()
    a.info.update(source="zenodo", calc_id=cid, frame_id=f"{cid}#{step}", ionic_step=step,
                  REF_energy=e, E_free=e - 0.01, total_magnetization=0.0, total_charge=0.0,
                  electronic_converged=True, scf_dE=1e-6)
    if forces:
        a.arrays["REF_forces"] = rng.normal(scale=0.3, size=(len(a), 3))
    if stress:
        a.info["REF_stress"] = np.array([0.01, 0.02, 0.03, 0.0, 0.0, 0.0])
    return a


def _meta_rec(cid: str, frames: list[Atoms], shards: set, cp: dict, prov: dict) -> dict:
    return {"calc_id": cid, "calc_parameters": cp, "parser": "pymatgen.Vasprun",
            "quality": {"n_atoms": len(frames[0]), "electronic_converged": True,
                        "ionic_converged": True, "n_frames_scf_unconverged": 0,
                        "n_frames_with_forces": len(frames), "n_frames_with_stress": len(frames),
                        "n_frames_dropped_no_energy": 0, "max_abs_free_minus_e0_per_atom": 0.005},
            "availability": {"dos": True}, "provenance": prov,
            "frame_ids": [f.info["frame_id"] for f in frames], "shards": sorted(shards),
            "electronic": {"net_magnetization": 0.0, "net_charge": 0.0,
                           "magnetization_source": "nonmagnetic",
                           "charge_source": "vasprun_atominfo"}}


@pytest.fixture()
def synthetic(tmp_path: Path) -> Path:
    rng = np.random.default_rng(0)
    ds = tmp_path / "dataset"
    cp = {"run_type": "GGA", "functional": "GGA",
          "potcar_symbols": ["PAW_PBE Na_pv 19Sep2006", "PAW_PBE Cl 06Sep2000"],
          "potcar_set_hash": "h1", "incar": {}, "kpoints": {"style": "Gamma", "kpts": [[4, 4, 4]]},
          "parameters": {"IBRION": 2, "NSW": 50, "ISIF": 3, "ENCUT": 520.0, "ISPIN": 1},
          "encut": 520.0, "ediff": 1e-5, "ismear": 0, "sigma": 0.05, "code_version": "6.4.2"}
    prov = {"source": "zenodo", "record_id": "1", "conceptrecid": "1", "creators": ["A, B"],
            "license": "cc-by-4.0", "publication_date": "2024-01-01", "resource_type": "dataset"}
    nacl = bulk("NaCl", "rocksalt", a=5.64)
    slab = fcc111("Pt", size=(2, 2, 3), vacuum=8.0)
    with ShardedExtxyzWriter(ds, frames_per_shard=7) as w, \
            MetadataWriter(ds / "metadata.jsonl") as m:
        def add(cid, frames, cp_, prov_):
            shards = {w.write(f) for f in frames}
            w.flush()
            m.write(_meta_rec(cid, frames, shards, cp_, prov_))
        add("zenodo:1:relax/vasprun.xml",
            [_frame(nacl, "zenodo:1:relax/vasprun.xml", i, -6.8 - 0.002 * i, rng)
             for i in range(10)], cp, prov)
        cp_md = dict(cp, potcar_symbols=["PAW_PBE Pt 04Feb2005"],
                     parameters={"IBRION": 0, "NSW": 100}, incar={"TEBEG": 500, "GGA": "RP"})
        add("zenodo:2:md/OUTCAR",
            [_frame(slab, "zenodo:2:md/OUTCAR", i, -60.0 + 0.05 * i, rng) for i in range(5)],
            cp_md, dict(prov, record_id="2", conceptrecid="2"))
        add("zenodo:3:copy/vasprun.xml",  # an exact re-upload of calc 1 in another deposit
            [_frame(nacl, "zenodo:3:copy/vasprun.xml", i, -6.8 - 0.002 * i, rng)
             for i in range(10)], cp, dict(prov, record_id="3", conceptrecid="3"))
    return tmp_path


def test_scan_shard_matches_writer(synthetic: Path):
    rows, calcs, agg = scan_shard(synthetic / "dataset" / "shard-00000.extxyz.gz")
    assert agg["frames"] == 7 and len(rows) == 7 and agg["bad_frames"] == 0
    r0 = rows[0]
    assert r0["calc"] == calc_key("zenodo:1:relax/vasprun.xml") and r0["step"] == 0
    assert r0["energy"] == pytest.approx(-6.8) and r0["natoms"] == 2
    assert r0["flags"] & F_FORCES and r0["flags"] & F_STRESS
    # ASE sign: pressure = -trace(stress)/3
    assert r0["pressure"] == pytest.approx(-(0.01 + 0.02 + 0.03) / 3 * EV_A3_TO_GPA, rel=1e-5)
    assert r0["dfree"] == pytest.approx(-0.01, abs=1e-6)
    # identical positions in every frame of calc 1 -> one structure hash
    assert len(set(rows["shash"].tolist())) == 1
    rec = calcs[0]
    assert rec["first"]["formula"] == "ClNa" and rec["first"]["spg"] == 225
    assert rec["last"]["step"] == 6 and rec["n"] == 7


def test_scan_text_with_reference_keys():
    text = ('2\nLattice="3 0 0 0 3 0 0 0 3" Properties=species:S:1:pos:R:3:forces:I:3 '
            'material_id=mp-1 task_id=mp-9 calc_id=0 ionic_step=4 energy=-7.0 '
            'stress="0.1 0 0 0 0.1 0 0 0 0.1" pbc="T T T"\n'
            'Cu 0 0 0 0 0 0\nCu 1.5 1.5 1.5 0 0 0\n')
    rows, calcs, agg = scan_text(text, name="t", keys=MPTRJ_KEYS)
    assert rows[0]["calc"] == calc_key("mp-9-0") and rows[0]["step"] == 4
    assert rows[0]["fmax"] == 0.0 and calcs[0]["g"] == "mp-1"
    assert rows[0]["pressure"] == pytest.approx(-0.1 * EV_A3_TO_GPA, rel=1e-6)


def test_end_to_end_report(synthetic: Path, tmp_path: Path):
    stats = tmp_path / "stats"
    meta_dataset(synthetic / "dataset" / "metadata.jsonl", stats / "zenodo" / "meta", workers=1,
                 parts=2)
    rows, tabs, deps, _agg = load_meta(stats / "zenodo" / "meta")
    assert len(rows) == 3 and sorted(deps) == ["zenodo:1", "zenodo:2", "zenodo:3"]
    res = scan_dataset(synthetic / "dataset", stats / "zenodo" / "scan", workers=1)
    assert res["complete"] and res["shards_failed"] == 0
    # resumable: a second run scans nothing
    assert scan_dataset(synthetic / "dataset", stats / "zenodo" / "scan")["shards_done_now"] == 0
    rep, _ = build_report(stats, ["zenodo"])
    z = rep["sources"]["zenodo"]
    assert z["size"]["calcs"] == 3 and z["size"]["frames_scanned"] == 25
    assert z["size"]["frames_without_metadata"] == 0
    assert z["redundancy"]["duplicate_frames"] == 10           # the re-uploaded calc
    assert z["redundancy"]["exact_duplicate_calcs"]["redundant_calcs"] == 1
    assert z["redundancy"]["exact_duplicate_calcs"]["groups_spanning_deposits"] == 1
    dims = {r["value"]: r["calcs"] for r in z["structure"]["dimensionality"]["threshold_6A"]}
    assert dims == {"bulk (no vacuum)": 2, "slab / 2D (1 vacuum axis)": 1}
    types = {r["value"]: r["calcs"] for r in z["settings"]["ctype"]}
    assert types == {"relax-cell": 2, "md": 1}
    assert z["chemistry"]["n_elements"] == 3
    assert z["curation"]["frames_passing_all"] == 25
    json.dumps(rep, default=str)  # JSON-serialisable


def test_load_scan_roundtrip(synthetic: Path, tmp_path: Path):
    out = tmp_path / "scan"
    scan_dataset(synthetic / "dataset", out)
    rows, calcs, agg = load_scan(sorted(out.glob("shard-*.npz"))[0])
    assert len(rows) == agg["frames"] and all("first" in c for c in calcs)
    assert math.isfinite(float(rows["fmax"][0]))


# --- origin split + formation-energy proxy ----------------------------------------------------
def test_origin_of_paths():
    from dataset_stats.meta import origin_of
    alex = {"calc_id": "nomad:x:calc/vasprun.xml.bz2",
            "provenance": {"mainfile": "xxx_02a-00_agm004014910_spg216/GEO1_vasprun.xml.bz2"}}
    ht = {"calc_id": "nomad:y:calc/vasprun.xml",
          "provenance": {"mainfile": "perovskites/Li_LiF3Cs_xxx_02p-00_spg221b/vasprun.xml"}}
    other = {"calc_id": "zenodo:1:relax/vasprun.xml", "provenance": {"file_path": "relax"}}
    assert (origin_of(alex), origin_of(ht), origin_of(other)) == (1, 2, 0)


def test_origin_subsets_and_formation_proxy(synthetic: Path, tmp_path: Path):
    import gzip
    # mark calc 3 as an Alexandria run by its path, then rebuild the metadata pass
    md = synthetic / "dataset" / "metadata.jsonl"
    recs = [json.loads(line) for line in md.read_text().splitlines()]
    recs[2]["provenance"]["mainfile"] = "xxx_02a-00_agm000000001_spg225/GEO1_vasprun.xml"
    md.write_text("".join(json.dumps(r) + "\n" for r in recs))
    stats = tmp_path / "stats"
    meta_dataset(md, stats / "zenodo" / "meta", workers=1)
    scan_dataset(synthetic / "dataset", stats / "zenodo" / "scan")
    # elemental references: E(Na) = -1.3 eV/atom, E(Cl) = -1.8 eV/atom
    refs = stats / "refs" / "mp_elemental_refs"
    refs.mkdir(parents=True)
    doc = {"Na": {"composition": {"Na": 2.0}, "energy": -2.6},
           "Cl": {"composition": {"Cl": 4.0}, "energy": -7.2},
           "Pt": {"composition": {"Pt": 1.0}, "energy": -6.0}}
    with gzip.open(refs / "refs.json.gz", "wt") as fh:
        json.dump(doc, fh)
    rep, _ = build_report(stats, ["zenodo"])
    assert set(rep["subsets"]) == {"zenodo[long-tail]", "zenodo[alexandria-group]"}
    assert rep["subsets"]["zenodo[alexandria-group]"]["size"]["calcs"] == 1
    assert rep["subsets"]["zenodo[long-tail]"]["size"]["frames_scanned"] == 15
    # the same scan read as a REFERENCE: every frame of a composition with references counts
    from dataset_stats.reference import load_elemental_refs
    from dataset_stats.report import reference_report
    mu = load_elemental_refs(stats / "refs")
    assert mu == {"Na": -1.3, "Cl": -1.8, "Pt": -6.0}
    ref_rep, _ = reference_report("synthetic", stats / "zenodo" / "scan", mu=mu)
    ef = ref_rep["labels"]["formation_energy_vs_mp_elements"]
    assert ef["frames_eligible"] == 25
    # NaCl: E/atom - (mu_Na + mu_Cl)/2; Pt slab: E/atom - mu_Pt
    assert ef["summary"]["min"] == pytest.approx((-6.8 - 0.018) / 2 + 1.55, abs=1e-6)
    assert ef["summary"]["max"] == pytest.approx((-60.0 + 0.2) / 12 + 6.0, abs=1e-6)


def test_xc_tag_normalisation():
    # INCAR echoes keep inline comments; VASP reads only the tag token
    assert params.xc_family(_cp(incar={"GGA": "BF                    ! BEEF"})) == "BEEF-vdW"
    assert params.xc_family(_cp(incar={"GGA": "PE           (PBESOL EXCHANGE-CORRELAT"})) == "PBE"
    assert params.xc_family(_cp(incar={"GGA": ".pe."})) == "PBE"
    assert params.xc_family(_cp(incar={"GGA": "MK !FOR OPTB86B-VDW FUNCTIONAL"})) == "optB86b-vdW"
    assert params.xc_family(_cp(incar={"METAGGA": "R2SCAN ! meta"})) == "R2SCAN"


def test_individual_only_scan_and_report(synthetic: Path, tmp_path: Path):
    from dataset_stats.meta import individual_include
    md = synthetic / "dataset" / "metadata.jsonl"
    recs = [json.loads(line) for line in md.read_text().splitlines()]
    recs[2]["provenance"]["mainfile"] = "xxx_02a-00_agm000000001_spg225/GEO1_vasprun.xml"
    md.write_text("".join(json.dumps(r) + "\n" for r in recs))
    stats = tmp_path / "stats"
    meta_dataset(md, stats / "zenodo" / "meta", workers=1)
    include, info = individual_include(stats / "zenodo" / "meta")
    # 7 frames per shard: calc 1 (10) -> shards 0-1, calc 2 (5) -> 1-2, calc 3 (10) -> 2-3
    assert include is not None and sorted(include) == [0, 1, 2]
    assert info["calcs_excluded"] == 1 and info["frames_excluded"] == 10
    res = scan_dataset(synthetic / "dataset", stats / "zenodo" / "scan", include=include,
                       filter_info=info)
    assert res["shards_total"] == 3 and res["frames_scanned_now"] == 15
    rows, _calcs, agg = load_scan(stats / "zenodo" / "scan" / "shard-00002.npz")
    assert agg["frames_skipped"] == 6 and len(rows) == 1
    with pytest.raises(ValueError):  # never mix filters in one scan dir
        scan_dataset(synthetic / "dataset", stats / "zenodo" / "scan")
    rep, _ = build_report(stats, ["zenodo"])
    z = rep["sources"]["zenodo"]
    assert z["filter"] == {"individual_only": True, "calcs_excluded": 1, "frames_excluded": 10}
    assert z["size"]["calcs"] == 2 and z["size"]["frames_scanned"] == 15
    assert z["size"]["frames_without_metadata"] == 0 and rep["subsets"] == {}
