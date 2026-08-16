"""Offline unit tests for the NOMAD adapter's pure logic (no network, no pymatgen).

Cover the query builder, keyset pagination loop, backoff, DOI normalisation + Zenodo
dedup, the canonical staging-name mapping, primary-file selection, HTTP retry on a fake
503, and the fetched-manifest builder (which runs the shared ``_find_calc_units`` on a
real on-disk tree). The disk/inode-valve + workers + resume of the production fetch are
exercised against an in-memory ``FakeNomadClient`` (no network), and the parser's
provenance-derived ``source`` (calc_id namespacing + frame tag) is checked here too. The
live end-to-end path is exercised by ``nomad_harvest.smoke``.

Run: ``python -m pytest tests/test_nomad.py -q`` from the repo root.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from zenodo_harvest.fetch import BudgetExceeded, StagingBudget
from zenodo_harvest.manifest import read_jsonl, write_jsonl
from zenodo_harvest.parse import _calc_id, _source_of

from nomad_harvest import client as client_mod
from nomad_harvest.client import (
    EXTERNAL_DBS,
    NomadClient,
    _backoff_seconds,
    direct_upload_vasp_query,
)
from nomad_harvest.harvest import (
    RecordTooBig,
    _member_role,
    build_fetched_entry,
    canonical_staged_name,
    choose_primary,
    fetch_candidates,
    fetch_candidates_bulk,
    nomad_metadata_availability,
    normalize_doi,
    raw_path_rel,
    references_of,
    slim_candidate,
    stage_entry,
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


# --------------------------------------------------------------------------- #
# NOMAD availability from parsed metadata (fix #3). available_properties is the #
# PRIMARY source for DOS/eigenvalues; the filename scan is the fallback for the #
# rest; the shared parser's embedded probe adds any dos/eigen in the vasprun.   #
# --------------------------------------------------------------------------- #

def test_nomad_metadata_availability_mapping():
    def av(props):
        return nomad_metadata_availability(
            {"results": {"properties": {"available_properties": props}}})
    assert av(["dos_electronic_new"]) == {"dos": True}
    assert av(["dos_electronic"]) == {"dos": True}          # legacy spelling
    assert av(["band_structure_electronic"]) == {"eigenvalues": True}
    assert av(["dos_electronic_new", "band_structure_electronic"]) == {
        "dos": True, "eigenvalues": True}
    # the unreliable trajectory flag and unrelated properties are NOT mapped
    assert av(["geometry_optimization", "trajectory", "band_gap"]) == {}
    assert av([]) == {}
    # absent field (older keep-lists) -> nothing asserted, fall back to the filename scan
    assert nomad_metadata_availability({"entry_id": "x"}) == {}


def test_build_fetched_entry_dos_from_metadata_without_doscar_file(tmp_path):
    # A NOMAD entry whose metadata says DOS but whose upload has NO DOSCAR file: the
    # filename scan alone would flag dos False (the under-count). NOMAD's metadata is the
    # authoritative primary source, so dos must be True; eigenvalues stays False (no band
    # structure), and charge_density stays False (no CHGCAR file).
    raw = tmp_path / "raw"
    calc = raw / "E" / "extracted" / "calc"
    calc.mkdir(parents=True)
    (calc / "vasprun.xml").write_text("<modeling/>")
    entry = {
        "entry_id": "E", "upload_id": "U",
        "results": {"material": {"elements": ["Si"]},
                    "properties": {"available_properties": ["dos_electronic_new"]}},
    }
    rd = {"mainfile": "x/vasprun.xml", "files": [{"path": "x/vasprun.xml", "size": 10}]}
    fe = build_fetched_entry(entry, raw, raw / "E", rd)
    assert fe is not None
    assert fe["availability"]["dos"] is True                       # from NOMAD metadata
    assert fe["availability"]["eigenvalues"] is False              # no band structure
    assert fe["availability"]["charge_density"] is False           # no CHGCAR file
    assert fe["availability_files"]["dos"] == "nomad:available_properties"


def test_build_fetched_entry_metadata_and_filename_availability_union(tmp_path):
    # Metadata (band structure -> eigenvalues) OR filename scan (CHGCAR -> charge_density):
    # both sources contribute, neither is dropped.
    raw = tmp_path / "raw"
    calc = raw / "E" / "extracted" / "calc"
    calc.mkdir(parents=True)
    (calc / "vasprun.xml").write_text("<modeling/>")
    entry = {"entry_id": "E", "upload_id": "U",
             "results": {"properties": {"available_properties": ["band_structure_electronic"]}}}
    rd = {"mainfile": "x/vasprun.xml",
          "files": [{"path": "x/vasprun.xml", "size": 10}, {"path": "x/CHGCAR", "size": 99}]}
    fe = build_fetched_entry(entry, raw, raw / "E", rd)
    assert fe["availability"]["eigenvalues"] is True               # from metadata
    assert fe["availability"]["charge_density"] is True            # from the filename scan


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
                    "properties": {"available_properties": ["dos_electronic_new"],
                                   "electronic": {"band_gap": [1.0] * 50},  # bulky -> dropped
                                   "structures": {"x": [1] * 50}}},         # bulky -> dropped
    }
    slim = slim_candidate(entry)
    assert "quantities" not in slim
    # `properties` is kept but SLIMMED to just available_properties (the availability source);
    # its bulky electronic/structures sub-trees are dropped.
    assert set(slim["results"]["properties"]) == {"available_properties"}
    assert slim["results"]["properties"]["available_properties"] == ["dos_electronic_new"]
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


# --- provenance-derived source: calc_id namespacing + frame tag -----------------

def test_nomad_provenance_yields_nomad_calc_id_namespace(tmp_path):
    """A NOMAD fetched record's provenance.source='nomad' makes the SHARED parser namespace
    its calc_id 'nomad:<entry_id>:…' (not 'zenodo:'), so it can't collide with the Zenodo
    dataset at merge. Zenodo/legacy provenance still defaults to 'zenodo' (byte-identical)."""
    raw = tmp_path / "raw"
    calc = raw / "HASH1" / "extracted" / "calc"
    calc.mkdir(parents=True)
    (calc / "vasprun.xml").write_text("<modeling/>")
    entry = {"entry_id": "HASH1", "license": "CC BY 4.0",
             "references": [], "results": {"material": {"elements": ["Si"]}}}
    fe = build_fetched_entry(entry, raw, raw / "HASH1",
                             {"mainfile": "x/vasprun.xml", "files": [{"path": "x/vasprun.xml"}]})
    assert fe["provenance"]["source"] == "nomad"
    base_meta = {"provenance": fe["provenance"],
                 "_extracted_root": str(raw / "HASH1" / "extracted")}
    unit = {k: str(raw / v) for k, v in fe["calc_units"][0].items()}
    assert _source_of(base_meta) == "nomad"
    assert _calc_id(unit, base_meta) == "nomad:HASH1:calc/vasprun.xml"
    # a record with no source field (legacy) still namespaces zenodo:
    assert _source_of({"provenance": {"record_id": "1"}}) == "zenodo"


# --- production fetch: disk/inode valve + workers + resume (in-memory client) ----

class FakeNomadClient:
    """In-memory stand-in for :class:`NomadClient` so the fetch valve/workers/resume logic
    is testable with no network. Serves each entry's ``rawdir`` + raw bytes; ``download_raw_file``
    chunks the blob and calls ``on_chunk`` per chunk (exercising the byte-budget charging),
    writing via a ``.part`` then rename exactly like the real client."""

    def __init__(self, entries: dict[str, dict]) -> None:
        # entries: {entry_id: {"mainfile": path, "files": [{"path","size"}], "blobs": {rel: bytes}}}
        self.entries = entries
        self.downloads = 0

    def rawdir(self, entry_id: str) -> dict:
        e = self.entries[entry_id]
        return {"mainfile": e["mainfile"], "files": e["files"]}

    def download_raw_file(self, entry_id, path, dest, decompress=False, on_chunk=None) -> int:
        blob = self.entries[entry_id]["blobs"][path]
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            with tmp.open("wb") as fh:
                for j in range(0, max(len(blob), 1), 256):   # small chunks -> on_chunk fires
                    chunk = blob[j:j + 256]
                    if on_chunk is not None:
                        on_chunk(len(chunk))
                    fh.write(chunk)
            tmp.replace(dest)
            self.downloads += 1
            return len(blob)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise


def _fake_entry(eid: str, size: int) -> dict:
    """A fake NOMAD entry whose vasprun is ``size`` bytes (mainfile-dir-relative name)."""
    return {"mainfile": f"d/{eid}_vasprun.xml",
            "files": [{"path": f"d/{eid}_vasprun.xml", "size": size}],
            "blobs": {f"{eid}_vasprun.xml": b"x" * size}}


def _keeplist(tmp_path: Path, entry_ids: list[str]) -> Path:
    p = tmp_path / "keep.jsonl"
    write_jsonl(p, [{"entry_id": e, "license": "CC BY 4.0", "references": [],
                     "results": {"material": {"elements": ["Si"]}}} for e in entry_ids])
    return p


def test_stage_entry_charges_budget_then_rolls_back_on_defer(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    client = FakeNomadClient({"A": _fake_entry("A", 600), "B": _fake_entry("B", 600)})
    budget = StagingBudget(max_bytes=1000, max_files=None, used_bytes=0, used_files=0)
    # A (600 B) fits; B (600 B) would push used past 1000 while B alone <= 1000 -> defer.
    stage_entry(client, {"entry_id": "A"}, raw, budget=budget)
    assert budget.used_bytes == 600
    with pytest.raises(BudgetExceeded):
        stage_entry(client, {"entry_id": "B"}, raw, budget=budget)
    assert budget.used_bytes == 600                 # B refunded on rollback
    assert not (raw / "B").exists()                 # B's partial tree deleted
    assert (raw / "A" / "extracted" / "calc" / "vasprun.xml").is_file()


def test_stage_entry_record_too_big_is_skipped_not_deferred(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    client = FakeNomadClient({"BIG": _fake_entry("BIG", 5000)})
    budget = StagingBudget(max_bytes=1000, max_files=None)
    with pytest.raises(RecordTooBig):               # own footprint > whole budget -> skip, never defer
        stage_entry(client, {"entry_id": "BIG"}, raw, budget=budget)
    assert budget.used_bytes == 0 and not (raw / "BIG").exists()
    assert budget.unfittable == 1


def test_stage_entry_inode_valve(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    client = FakeNomadClient({"A": _fake_entry("A", 10)})
    # A stages 3 dirs + 1 file = 4 inodes; a 2-inode budget can never hold it -> RecordTooBig.
    budget = StagingBudget(max_bytes=None, max_files=2)
    with pytest.raises(RecordTooBig):
        stage_entry(client, {"entry_id": "A"}, raw, budget=budget)
    assert not (raw / "A").exists()


def test_fetch_candidates_stops_on_disk_budget_then_resumes(tmp_path):
    raw = tmp_path / "raw"
    keep = _keeplist(tmp_path, ["A", "B", "C"])
    client = FakeNomadClient({e: _fake_entry(e, 600) for e in ("A", "B", "C")})
    out = tmp_path / "fetched.jsonl"
    # Budget ~ 1 record: the second record defers -> stop cleanly, flag set.
    s1 = fetch_candidates(client, keep, raw_dir=raw, out_path=out,
                          max_disk_bytes=1000, max_disk_files=None, workers=1)
    assert s1["stopped_disk_budget"] is True
    assert s1["staged"] == 1
    staged_ids = {r["recid"] for r in read_jsonl(out)}
    assert len(staged_ids) == 1
    # "purge": drop the staged tree of the parsed record, then resume with the SAME budget.
    import shutil
    for rid in staged_ids:
        shutil.rmtree(raw / rid)
    s2 = fetch_candidates(client, keep, raw_dir=raw, out_path=out,
                          max_disk_bytes=1000, max_disk_files=None, workers=1)
    assert s2["skipped_existing"] == 1              # the already-staged record is not re-fetched
    # Iterate purge+resume until every record is staged (the pacing loop the pipeline runs).
    for _ in range(5):
        ids = {r["recid"] for r in read_jsonl(out)}
        if len(ids) == 3:
            break
        for rid in ids:
            if (raw / rid).exists():
                shutil.rmtree(raw / rid)
        fetch_candidates(client, keep, raw_dir=raw, out_path=out,
                         max_disk_bytes=1000, max_disk_files=None, workers=1)
    final = [r["recid"] for r in read_jsonl(out)]
    assert sorted(final) == ["A", "B", "C"]         # all staged, exactly once each
    assert len(final) == len(set(final))


def test_fetch_candidates_workers_stage_all(tmp_path):
    raw = tmp_path / "raw"
    keep = _keeplist(tmp_path, [f"E{i}" for i in range(8)])
    client = FakeNomadClient({f"E{i}": _fake_entry(f"E{i}", 100) for i in range(8)})
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=raw, out_path=out, workers=4)
    ids = [r["recid"] for r in read_jsonl(out)]
    assert summary["staged"] == 8
    assert sorted(ids) == sorted(f"E{i}" for i in range(8))
    assert len(ids) == len(set(ids))                # thread-safe writer: no duplicate lines


def test_fetch_candidates_resume_skips_manifest(tmp_path):
    raw = tmp_path / "raw"
    keep = _keeplist(tmp_path, ["A", "B"])
    client = FakeNomadClient({"A": _fake_entry("A", 50), "B": _fake_entry("B", 50)})
    out = tmp_path / "fetched.jsonl"
    fetch_candidates(client, keep, raw_dir=raw, out_path=out)           # stages A + B
    before = client.downloads
    summary = fetch_candidates(client, keep, raw_dir=raw, out_path=out)  # re-run: nothing new
    assert summary["skipped_existing"] == 2
    assert summary["staged"] == 0
    assert client.downloads == before                                   # no re-downloads
    assert len(list(read_jsonl(out))) == 2                              # no duplicate manifest lines


# --- bulk fetch (fixes #1/#2): batching + exact member mapping + fallback + valve --

class FakeBulkClient(FakeNomadClient):
    """Adds the two bulk endpoints: ``bulk_rawdir`` (batch listings) and ``bulk_raw_zip``
    (writes a REAL zip whose members are ``<upload_id>/<mainfile>`` with each entry's blob
    as content). ``omit`` forces given entries to be MISSING from the zip, exercising the
    per-entry fallback. Inherits ``rawdir``/``download_raw_file`` so that fallback works."""

    def __init__(self, entries: dict, uploads: dict, omit=(), fail_bulk_rawdir=False):
        super().__init__(entries)
        self.uploads = uploads
        self.mains = {eid: e["mainfile"] for eid, e in entries.items()}
        self.omit = set(omit)
        self.fail_bulk_rawdir = fail_bulk_rawdir
        self.zips = 0
        self.rawdirs = 0

    def rawdir(self, entry_id: str) -> dict:
        self.rawdirs += 1
        return super().rawdir(entry_id)

    def bulk_rawdir(self, ids):
        if self.fail_bulk_rawdir:
            raise RuntimeError("simulated bulk_rawdir 5xx for the whole batch")
        return {eid: {"entry_id": eid, "upload_id": self.uploads[eid],
                      "mainfile": self.mains[eid], "files": self.entries[eid]["files"]}
                for eid in ids if eid in self.entries}

    def bulk_raw_zip(self, ids, files, dest, on_chunk=None):
        import zipfile
        dest.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(dest, "w") as zf:
            for eid in ids:
                if eid in self.omit or eid not in self.entries:
                    continue
                base = self.mains[eid].rsplit("/", 1)[-1]
                zf.writestr(f"{self.uploads[eid]}/{self.mains[eid]}",
                            self.entries[eid]["blobs"][base])
        if on_chunk is not None:
            on_chunk(dest.stat().st_size)
        self.zips += 1
        return dest.stat().st_size


def _bulk_setup(tmp_path: Path, specs):
    """specs: [(entry_id, upload_id, mainfile, size)]. Returns (keeplist, entries, uploads)."""
    entries, uploads, rows = {}, {}, []
    for eid, uid, mf, size in specs:
        base = mf.rsplit("/", 1)[-1]
        entries[eid] = {"mainfile": mf, "files": [{"path": mf, "size": size}],
                        "blobs": {base: b"x" * size}}
        uploads[eid] = uid
        rows.append({"entry_id": eid, "upload_id": uid, "mainfile": mf,
                     "license": "CC BY 4.0", "references": [],
                     "results": {"material": {"elements": ["Si"]}}})
    keep = tmp_path / "bulk_keep.jsonl"
    write_jsonl(keep, rows)
    return keep, entries, uploads


@pytest.mark.parametrize("mainfile,role", [
    ("d/vasprun.xml.bz2", "vasprun"), ("GEO3_vasprun.xml", "vasprun"),
    ("run/OUTCAR", "outcar"), ("d/OUTCAR.gz", "outcar"), ("d/INCAR", None),
])
def test_member_role(mainfile, role):
    assert _member_role(mainfile) == role


def test_bulk_fetch_stages_vasprun_and_outcar(tmp_path):
    # one vasprun-primary + one OUTCAR-only entry -> both covered by include_files
    keep, entries, uploads = _bulk_setup(tmp_path, [
        ("V", "U1", "a/vasprun.xml", 40),
        ("O", "U2", "b/OUTCAR", 40)])
    client = FakeBulkClient(entries, uploads)
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw",
                                    out_path=out, batch_size=5)
    assert summary["staged"] == 2 and summary["batches"] == 1 and client.zips == 1
    recs = {r["recid"]: r for r in read_jsonl(out)}
    assert set(recs) == {"V", "O"}
    assert recs["V"]["provenance"]["source"] == "nomad"
    assert recs["V"]["calc_units"][0]["vasprun"].endswith("vasprun.xml")
    assert recs["O"]["calc_units"][0]["outcar"].endswith("OUTCAR")


def test_bulk_fetch_falls_back_for_missing_member(tmp_path):
    # B is omitted from the zip -> must be recovered by the per-entry fallback (no data lost)
    keep, entries, uploads = _bulk_setup(tmp_path, [
        ("A", "U", "d/A_vasprun.xml", 30), ("B", "U", "d/B_vasprun.xml", 30)])
    client = FakeBulkClient(entries, uploads, omit=["B"])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 2                 # both staged (B via fallback)
    assert summary["bulk_fallback"] >= 1
    assert client.downloads >= 1                  # the per-entry download ran for B
    assert {r["recid"] for r in read_jsonl(out)} == {"A", "B"}


def test_bulk_fetch_resume_skips_manifest(tmp_path):
    keep, entries, uploads = _bulk_setup(tmp_path, [
        ("A", "U", "d/A_vasprun.xml", 20), ("B", "U", "d/B_vasprun.xml", 20)])
    client = FakeBulkClient(entries, uploads)
    out = tmp_path / "fetched.jsonl"
    fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    z = client.zips
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["skipped_existing"] == 2 and summary["staged"] == 0
    assert client.zips == z                        # nothing re-fetched
    assert len(list(read_jsonl(out))) == 2         # no duplicate manifest lines


def test_bulk_fetch_recovers_availability_when_bulk_rawdir_fails(tmp_path):
    # bulk_rawdir 5xx's for the batch -> availability (a mandatory field) must be recovered
    # per-entry, NOT written as all-False. The upload holds a CHGCAR, so charge_density=True.
    keep, entries, uploads = _bulk_setup(tmp_path, [("A", "U", "d/vasprun.xml", 30)])
    entries["A"]["files"].append({"path": "d/CHGCAR", "size": 999})   # heavy output present
    client = FakeBulkClient(entries, uploads, fail_bulk_rawdir=True)
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 1
    assert client.rawdirs == 1                        # per-entry rawdir recovered the listing
    rec = next(iter(read_jsonl(out)))
    assert rec["availability"]["charge_density"] is True   # NOT silently lost


def test_bulk_fetch_valve_defers(tmp_path):
    # two 600 B members, one batch, 1000 B budget: first stages, second defers (reclaimable)
    keep, entries, uploads = _bulk_setup(tmp_path, [
        ("A", "U", "d/A_vasprun.xml", 600), ("B", "U", "d/B_vasprun.xml", 600)])
    client = FakeBulkClient(entries, uploads)
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                                    max_disk_bytes=1000, batch_size=5)
    assert summary["staged"] == 1                  # only the first fit
    assert summary["stopped_disk_budget"] is True  # signals pipeline to reclaim + resume


# --- NOMAD data-root separation + status (the standalone-tree design) -----------

from nomad_harvest.cli import nomad_paths  # noqa: E402
from zenodo_harvest import config  # noqa: E402


def test_nomad_paths_sibling_of_absolute_zenodo_root(monkeypatch):
    # On CSD3 the Zenodo root is absolute (.../hpc-work/zenodo): NOMAD is a SIBLING (.../nomad),
    # never nested inside the Zenodo tree, so the two harvests never share a staging dir.
    monkeypatch.delenv("NOMAD_HARVEST_DATA", raising=False)
    monkeypatch.setattr(config, "DATA_ROOT", Path("/rds/user/me/hpc-work/zenodo"))
    root, man, raw, ds = nomad_paths()
    assert root == Path("/rds/user/me/hpc-work/nomad")
    assert (man, raw, ds) == (root / "manifests", root / "raw", root / "dataset")


def test_nomad_paths_nested_under_relative_local_default(monkeypatch):
    # Local default root is the relative "data" (gitignored): nest NOMAD under it (data/nomad)
    # rather than a repo-root sibling, so it stays inside the ignored tree.
    monkeypatch.delenv("NOMAD_HARVEST_DATA", raising=False)
    monkeypatch.setattr(config, "DATA_ROOT", Path("data"))
    assert nomad_paths()[0] == Path("data/nomad")


def test_nomad_paths_env_override_wins(monkeypatch):
    monkeypatch.setenv("NOMAD_HARVEST_DATA", "/scratch/nomad")
    monkeypatch.setattr(config, "DATA_ROOT", Path("/rds/user/me/hpc-work/zenodo"))
    assert nomad_paths()[0] == Path("/scratch/nomad")


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(path, rows)


def test_nomad_status_uses_nomad_manifest_names(tmp_path):
    # Build a minimal NOMAD tree and assert the shared status walker, told NOMAD's names,
    # counts the keep-list as BOTH discover + triage, the standalone nomad_fetched.jsonl for
    # FETCH, and both NOMAD rejection logs — using NOMAD filenames, not the Zenodo ones.
    from zenodo_harvest.status import status_report

    man, raw, ds = tmp_path / "manifests", tmp_path / "raw", tmp_path / "dataset"
    _write(man / "nomad_keep.jsonl",
           [{"entry_id": f"E{i}", "mainfile": "d/vasprun.xml.bz2"} for i in range(5)])
    _write(man / "nomad_fetched.jsonl",
           [{"recid": f"E{i}", "n_calc_units": 1,
             "provenance": {"source": "nomad", "record_id": f"E{i}"}} for i in range(3)])
    _write(man / "nomad_rejections.jsonl",
           [{"stage": "nomad_discover", "id": "E7", "reason": "non_redistributable_license"}])
    _write(man / "nomad_fetch_rejections.jsonl",
           [{"stage": "nomad_fetch", "id": "E4", "reason": "no_vasp_primary"}])
    _write(ds / "metadata.jsonl",
           [{"calc_id": f"nomad:E{i}:d/vasprun.xml",
             "provenance": {"source": "nomad", "record_id": f"E{i}"},
             "quality": {"n_frames": 2, "n_frames_with_forces": 2}} for i in range(2)])

    r = status_report(
        manifests_dir=man, raw_dir=raw, dataset_dir=ds,
        candidate_globs=["nomad_keep.jsonl"], keep_name="nomad_keep.jsonl",
        extra_rejection_names=("nomad_rejections.jsonl", "nomad_fetch_rejections.jsonl"),
        fetched_globs=["*.fetched.jsonl", "nomad_fetched.jsonl"], staging_walk=False)

    assert r["discover"]["candidates"] == 5          # nomad_keep.jsonl (not candidates*.jsonl)
    assert r["triage"]["keep"] == 5
    assert r["fetch"]["fetched_records"] == 3         # standalone nomad_fetched.jsonl counted
    assert r["fetch"]["calc_units"] == 3
    assert r["parse"]["calcs_parsed"] == 2
    assert r["parse"]["frames"] == 4
    assert r["errors"]["rejections"] == 2             # BOTH nomad rejection logs folded in
    assert r["errors"]["by_reason"]["no_vasp_primary"] == 1
    # RECORDS breakdown bounded by keep membership (keep-list keyed by entry_id): E0-E2 fetched,
    # E4 fetch-rejected -> 4 attempted, 1 untouched (E3), 1 fetch-rejected.
    assert r["records"]["attempted"] == 4
    assert r["records"]["fetched"] == 3
    assert r["records"]["fetch_rejected"] == 1
    assert r["records"]["untouched"] == 1


def test_status_zenodo_defaults_unchanged(tmp_path):
    # Regression: without the NOMAD params, the walker keeps its Zenodo behaviour (candidates*
    # glob, keep.jsonl, fetch stage exactly "fetch").
    from zenodo_harvest.status import status_report

    man, raw, ds = tmp_path / "manifests", tmp_path / "raw", tmp_path / "dataset"
    _write(man / "candidates.jsonl", [{"recid": f"z{i}"} for i in range(4)])
    _write(man / "keep.jsonl", [{"recid": f"z{i}"} for i in range(4)])
    _write(man / "fetched.jsonl", [{"recid": "z0", "n_calc_units": 2}])  # standalone name
    _write(man / "rejections.jsonl",
           [{"stage": "fetch", "id": "z3", "reason": "no_vasp_files"}])
    r = status_report(manifests_dir=man, raw_dir=raw, dataset_dir=ds, staging_walk=False)
    assert r["discover"]["candidates"] == 4
    assert r["triage"]["keep"] == 4
    assert r["fetch"]["fetched_records"] == 1          # fetched.jsonl now counted too
    assert r["records"]["fetch_rejected"] == 1


# --- chunked, resilient bulk_rawdir (rawdir/query 500s on a big id list) ---------

def test_bulk_rawdir_chunks_and_covers_all(monkeypatch):
    # rawdir/query 500s on a ~300-id query (live-verified), so bulk_rawdir must split into
    # RAWDIR_CHUNK-sized sub-requests. Record the sub-request sizes and assert full coverage.
    client = NomadClient()
    seen_sizes: list[int] = []

    def fake_post(path, body):
        assert path == "/entries/rawdir/query"
        ids = body["query"]["entry_id:any"]
        seen_sizes.append(len(ids))
        assert len(ids) <= client.RAWDIR_CHUNK        # never an oversized query
        return {"data": [{"entry_id": e, "mainfile": "d/vasprun.xml", "files": []} for e in ids]}

    monkeypatch.setattr(client, "_post", fake_post)
    ids = [f"E{i}" for i in range(60)]
    out = client.bulk_rawdir(ids, chunk_size=25)
    assert seen_sizes == [25, 25, 10]                 # 60 split into 25+25+10
    assert set(out) == set(ids)                       # every entry covered


def test_bulk_rawdir_partial_on_chunk_failure(monkeypatch):
    # A failing sub-request must NOT discard the good chunks — those entries are simply absent
    # (the caller recovers them per-entry). No exception escapes bulk_rawdir.
    client = NomadClient()
    calls = {"n": 0}

    def flaky_post(path, body):
        calls["n"] += 1
        if calls["n"] == 2:                            # 2nd chunk always fails
            raise RuntimeError("HTTP 500 rawdir/query")
        ids = body["query"]["entry_id:any"]
        return {"data": [{"entry_id": e, "files": []} for e in ids]}

    monkeypatch.setattr(client, "_post", flaky_post)
    ids = [f"E{i}" for i in range(75)]                 # 3 chunks of 25
    out = client.bulk_rawdir(ids, chunk_size=25)
    assert len(out) == 50                              # chunks 1 + 3 kept; chunk 2 (E25..E49) lost
    assert "E0" in out and "E50" in out and "E25" not in out


# --- bulk download: empty-zip fallback + --no-rawdir (batch ceiling / rawdir cost) ---

def test_bulk_fetch_recovers_from_empty_zip(tmp_path):
    # NOMAD returns an EMPTY 200 (0-byte body) for a too-big raw/query batch -> zipfile raises
    # BadZipFile. That must fall back per-entry (no data lost), NEVER crash the fetch.
    keep, entries, uploads = _bulk_setup(tmp_path, [
        ("A", "U", "d/A_vasprun.xml", 30), ("B", "U", "d/B_vasprun.xml", 30)])

    class EmptyZipClient(FakeBulkClient):
        def bulk_raw_zip(self, ids, files, dest, on_chunk=None):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"")           # empty 200 -> 0-byte "zip"
            self.zips += 1
            return 0

    client = EmptyZipClient(entries, uploads)
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                                    batch_size=5)
    assert summary["staged"] == 2           # both recovered per-entry, no crash
    assert client.rawdirs >= 2              # the per-entry fallback ran
    assert set(r["recid"] for r in read_jsonl(out)) == {"A", "B"}


def test_bulk_fetch_no_rawdir_skips_listing(tmp_path):
    # rawdir_listing=False: the availability LISTING is skipped entirely (bulk_rawdir AND the
    # per-entry rawdir recovery). Availability then comes only from available_properties
    # (DOS/eigenvalues) — charge_density (a file-listing-only flag) stays False.
    keep, entries, uploads = _bulk_setup(tmp_path, [("A", "U", "d/A_vasprun.xml", 30)])
    entries["A"]["files"].append({"path": "d/CHGCAR", "size": 999})   # heavy file IS present
    rows = list(read_jsonl(keep))
    rows[0].setdefault("results", {})["properties"] = {
        "available_properties": ["dos_electronic_new"]}                # DOS asserted in metadata
    write_jsonl(keep, rows)
    # fail_bulk_rawdir=True would RAISE if bulk_rawdir were called — proving it is NOT.
    client = FakeBulkClient(entries, uploads, fail_bulk_rawdir=True)
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates_bulk(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                                    batch_size=5, rawdir_listing=False)
    assert summary["staged"] == 1
    assert client.rawdirs == 0              # NO per-entry rawdir recovery either
    rec = next(iter(read_jsonl(out)))
    assert rec["availability"]["dos"] is True              # from available_properties (free)
    assert rec["availability"]["charge_density"] is False  # listing skipped -> heavy file unseen
