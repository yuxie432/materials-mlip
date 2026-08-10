"""Offline unit tests for the NOMAD adapter's pure logic (no network, no pymatgen).

Cover the query builder, keyset pagination loop, backoff, DOI normalisation + Zenodo
dedup, the canonical staging-name mapping, primary-file selection, HTTP retry on a fake
503, and the fetched-manifest builder (which runs the shared ``_find_calc_units`` on a
real on-disk tree). The live end-to-end path is exercised by ``nomad_harvest.smoke``.

Run: ``python -m pytest tests/test_nomad.py -q`` from the repo root.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nomad_harvest import client as client_mod
from nomad_harvest.client import (
    EXTERNAL_DBS,
    NomadClient,
    _backoff_seconds,
    direct_upload_vasp_query,
)
from nomad_harvest.harvest import (
    build_fetched_entry,
    canonical_staged_name,
    choose_primary,
    normalize_doi,
    raw_path_rel,
    references_of,
    slim_candidate,
    zenodo_dois,
    zenodo_overlap,
)


# --- query builder --------------------------------------------------------------

def test_direct_upload_query_excludes_external_dbs():
    q = direct_upload_vasp_query()
    clauses = q["and"]
    assert {"results.method.simulation.program_name": "VASP"} in clauses
    assert {"results.method.method_name": "DFT"} in clauses
    not_clause = next(c for c in clauses if "not" in c)
    assert not_clause["not"]["external_db:any"] == EXTERNAL_DBS


def test_direct_upload_query_element_scope():
    q = direct_upload_vasp_query(elements=["Ti", "O"])
    assert {"results.material.elements:all": ["Ti", "O"]} in q["and"]


# --- keyset pagination ----------------------------------------------------------

def test_iter_entries_keyset_advances_and_dedups():
    c = NomadClient()
    pages = [
        {"data": [{"entry_id": "a"}, {"entry_id": "b"}],
         "pagination": {"next_page_after_value": "b"}},
        {"data": [{"entry_id": "c"}], "pagination": {"next_page_after_value": "c"}},
        {"data": [], "pagination": {}},
    ]
    seen_after: list[str | None] = []

    def fake_query_page(query, required=None, page_size=1000, page_after_value=None,
                        order_by="entry_id"):
        seen_after.append(page_after_value)
        return pages[len(seen_after) - 1]

    c.query_page = fake_query_page  # type: ignore[assignment]
    got = [e["entry_id"] for e in c.iter_entries({}, page_size=2)]
    assert got == ["a", "b", "c"]              # every entry, in order, no repeats
    assert seen_after == [None, "b", "c"]      # keyset cursor advanced each page


def test_iter_entries_respects_max_entries():
    c = NomadClient()

    def fake_query_page(query, required=None, page_size=1000, page_after_value=None,
                        order_by="entry_id"):
        return {"data": [{"entry_id": f"e{i}"} for i in range(10)],
                "pagination": {"next_page_after_value": "e9"}}

    c.query_page = fake_query_page  # type: ignore[assignment]
    assert len(list(c.iter_entries({}, max_entries=3))) == 3


def test_backoff_honours_retry_after_and_caps():
    assert _backoff_seconds(0, "10") == 11
    assert _backoff_seconds(0, None) == 2
    assert _backoff_seconds(10, None) == 60          # capped
    assert _backoff_seconds(0, "garbage") == 2       # bad header -> schedule


# --- HTTP retry on a fake 503 ---------------------------------------------------

class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self.headers: dict = {}
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected raise for {self.status_code}")


class _Session:
    def __init__(self, responses):
        self._responses = list(responses)
        self.headers: dict = {}
        self.calls = 0

    def post(self, url, json=None, timeout=None):
        self.calls += 1
        return self._responses.pop(0)


def test_post_retries_on_503(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda *_: None)  # no real delay
    sess = _Session([_Resp(503), _Resp(200, {"pagination": {"total": 7}})])
    c = NomadClient(session=sess, min_interval=0)
    out = c._post("/entries/query", {})
    assert out["pagination"]["total"] == 7
    assert sess.calls == 2                            # retried past the 503


# --- DOI / Zenodo dedup ---------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("https://doi.org/10.5281/zenodo.123", "10.5281/zenodo.123"),
    ("10.1103/PhysRevB.99.014104", "10.1103/physrevb.99.014104"),
    ("doi:10.1234/ABC.def", "10.1234/abc.def"),
    ("no doi here", None),
    ("", None),
])
def test_normalize_doi(text, expected):
    assert normalize_doi(text) == expected


def test_references_of_includes_dataset_dois():
    entry = {"references": ["http://x"], "datasets": [{"doi": "10.5281/zenodo.9"}, {}]}
    assert references_of(entry) == ["http://x", "10.5281/zenodo.9"]


def test_zenodo_overlap_detects_url_and_shared_doi():
    assert zenodo_overlap({"references": ["https://zenodo.org/record/5"]}, set())
    assert zenodo_overlap({"references": ["https://doi.org/10.5281/zenodo.7"]}, set())
    # a shared non-Zenodo DOI (from the known set) also counts
    assert zenodo_overlap({"references": ["https://doi.org/10.1234/abc"]}, {"10.1234/abc"})
    # unrelated reference -> not a duplicate
    assert zenodo_overlap({"references": ["https://materialsproject.org/mp-1"]}, set()) is None


def test_zenodo_dois_reads_metadata(tmp_path):
    meta = tmp_path / "metadata.jsonl"
    rows = [{"provenance": {"doi": "10.5281/zenodo.111"}},
            {"provenance": {"conceptdoi": "https://doi.org/10.5281/zenodo.222"}},
            {"provenance": {}}]
    meta.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert zenodo_dois(meta) == {"10.5281/zenodo.111", "10.5281/zenodo.222"}
    assert zenodo_dois(tmp_path / "absent.jsonl") == set()   # missing file -> empty


# --- staging name / primary selection -------------------------------------------

@pytest.mark.parametrize("remote,role,expected", [
    ("run/vasprun.xml.bz2", "vasprun", "vasprun.xml.bz2"),
    ("GEO3_vasprun.xml.bz2", "vasprun", "vasprun.xml.bz2"),
    ("vasprun.xml.gz", "vasprun", "vasprun.xml.gz"),
    ("vasprun.xml.relax1", "vasprun", "vasprun.xml"),   # no known compression suffix
    ("dir/OUTCAR.gz", "outcar", "OUTCAR.gz"),
    ("OUTCAR", "outcar", "OUTCAR"),
])
def test_canonical_staged_name(remote, role, expected):
    assert canonical_staged_name(remote, role) == expected


@pytest.mark.parametrize("full,mainfile,expected", [
    # file under the mainfile's dir -> path after that dir (verified: full path 404s)
    ("dir/GEO3_vasprun.xml.bz2", "dir/GEO3_vasprun.xml.bz2", "GEO3_vasprun.xml.bz2"),
    ("a/b/OUTCAR", "a/b/vasprun.xml", "OUTCAR"),
    ("nested/sub/vasprun.xml", "nested/vasprun.xml", "sub/vasprun.xml"),
    # mainfile at upload root -> path is already relative
    ("vasprun.xml", "vasprun.xml", "vasprun.xml"),
    # not under mainfile dir -> basename fallback
    ("other/OUTCAR", "a/b/vasprun.xml", "OUTCAR"),
])
def test_raw_path_rel(full, mainfile, expected):
    assert raw_path_rel(full, mainfile) == expected


def test_choose_primary_prefers_vasprun_mainfile():
    rd = {"mainfile": "a/vasprun.xml.bz2",
          "files": [{"path": "a/vasprun.xml.bz2"}, {"path": "a/OUTCAR"}]}
    assert choose_primary(rd) == ("a/vasprun.xml.bz2", "vasprun")


def test_choose_primary_outcar_only_and_none():
    assert choose_primary({"mainfile": "a/OUTCAR", "files": [{"path": "a/OUTCAR"}]}) == (
        "a/OUTCAR", "outcar")
    assert choose_primary({"mainfile": "a/INCAR", "files": [{"path": "a/INCAR"}]}) is None


# --- fetched-manifest builder (runs the shared _find_calc_units on real files) ---

def test_build_fetched_entry_shape(tmp_path):
    raw = tmp_path / "raw"
    dest = raw / "ENTRY1"
    calc = dest / "extracted" / "calc"
    calc.mkdir(parents=True)
    (calc / "vasprun.xml").write_text("<modeling/>")   # name-based; content irrelevant here
    entry = {
        "entry_id": "ENTRY1", "upload_id": "U1", "license": "CC BY 4.0",
        "references": ["https://doi.org/10.1103/PhysRevB.1.1"],
        "authors": [{"name": "A. Author"}],
        "results": {"material": {"elements": ["Si"]},
                    "method": {"simulation": {"program_version": "5.4.4",
                                              "dft": {"xc_functional_names": ["GGA_X_PBE"]}}}},
    }
    rd = {"mainfile": "x/vasprun.xml",
          "files": [{"path": "x/vasprun.xml", "size": 10}, {"path": "x/CHGCAR", "size": 99}]}
    fe = build_fetched_entry(entry, raw, dest, rd)
    assert fe is not None
    assert fe["provenance"]["source"] == "nomad"
    assert fe["provenance"]["record_id"] == "ENTRY1"
    assert fe["provenance"]["doi"] == "10.1103/physrevb.1.1"
    assert fe["local_dir"] == "ENTRY1"
    assert fe["calc_units"][0]["vasprun"] == "ENTRY1/extracted/calc/vasprun.xml"
    assert fe["calc_units"][0]["dir"] == "ENTRY1/extracted/calc"
    # heavy output present in the listing is recorded as availability, never fetched
    assert fe["availability"]["charge_density"] is True
    assert not (calc / "CHGCAR").exists()


def test_slim_candidate_keeps_used_fields_and_drops_bulk():
    entry = {
        "entry_id": "E", "upload_id": "U", "mainfile": "a/vasprun.xml",
        "license": "CC BY 4.0", "references": ["r1"],
        "datasets": [{"doi": "10.5281/zenodo.1", "dataset_name": "big"}],
        "authors": [{"name": "A", "affiliation": "X"}],
        "quantities": ["run.system"] * 500,          # the bulky field we exclude/slim away
        "results": {"material": {"elements": ["Si"], "n_elements": 1, "topology": [1, 2, 3]},
                    "method": {"method_name": "DFT",
                               "simulation": {"program_name": "VASP", "program_version": "5.4",
                                              "dft": {"xc_functional_names": ["GGA_X_PBE"]}}},
                    "properties": {"available_properties": ["x"] * 50}},
    }
    slim = slim_candidate(entry)
    assert "quantities" not in slim
    assert "properties" not in slim["results"]         # bulky sub-tree dropped
    assert slim["results"]["material"]["elements"] == ["Si"]
    assert slim["results"]["method"]["simulation"]["program_name"] == "VASP"
    assert slim["datasets"] == [{"doi": "10.5281/zenodo.1"}]
    assert slim["authors"] == [{"name": "A"}]
    # the slimmed record still feeds build_fetched_entry / dedup unchanged
    assert references_of(slim) == ["r1", "10.5281/zenodo.1"]


def test_build_fetched_entry_none_without_primary(tmp_path):
    raw = tmp_path / "raw"
    d = raw / "E2" / "extracted" / "calc"
    d.mkdir(parents=True)
    (d / "INCAR").write_text("SYSTEM = x")             # inputs only -> no calc unit
    assert build_fetched_entry({"entry_id": "E2"}, raw, raw / "E2", {"files": []}) is None
