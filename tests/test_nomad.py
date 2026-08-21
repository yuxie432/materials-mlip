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
from nomad_harvest import upload_zip
from nomad_harvest.cli import _part_is_complete
from nomad_harvest.harvest import (
    _DEAD_UPLOAD_MAX_FAILS,
    RecordTooBig,
    _load_dead_uploads,
    _should_whole_stream,
    build_fetched_entry,
    canonical_staged_name,
    choose_primary,
    fetch_candidates,
    nomad_metadata_availability,
    normalize_doi,
    raw_path_rel,
    references_of,
    slim_candidate,
    split_by_upload,
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
                          max_disk_bytes=1000, max_disk_files=None)
    assert s1["stopped_disk_budget"] is True
    assert s1["staged"] == 1
    staged_ids = {r["recid"] for r in read_jsonl(out)}
    assert len(staged_ids) == 1
    # "purge": drop the staged tree of the parsed record, then resume with the SAME budget.
    import shutil
    for rid in staged_ids:
        shutil.rmtree(raw / rid)
    s2 = fetch_candidates(client, keep, raw_dir=raw, out_path=out,
                          max_disk_bytes=1000, max_disk_files=None)
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
                         max_disk_bytes=1000, max_disk_files=None)
    final = [r["recid"] for r in read_jsonl(out)]
    assert sorted(final) == ["A", "B", "C"]         # all staged, exactly once each
    assert len(final) == len(set(final))


def test_fetch_candidates_fallback_stages_all(tmp_path):
    # Entries with no addressable upload zip (this fake serves only the per-entry endpoints)
    # fall back to entries/{id}/raw and are all staged — coverage is never lost.
    raw = tmp_path / "raw"
    keep = _keeplist(tmp_path, [f"E{i}" for i in range(8)])
    client = FakeNomadClient({f"E{i}": _fake_entry(f"E{i}", 100) for i in range(8)})
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=raw, out_path=out)
    ids = [r["recid"] for r in read_jsonl(out)]
    assert summary["staged"] == 8
    assert summary["per_entry_fallback"] == 8       # all via the fallback path
    assert sorted(ids) == sorted(f"E{i}" for i in range(8))
    assert len(ids) == len(set(ids))


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


# --- targeted extraction from the pre-packed upload zip (the fast fetch path) ----

import io  # noqa: E402
import struct  # noqa: E402
import zipfile  # noqa: E402


def _make_zip(members: dict[str, bytes]) -> bytes:
    """A real STORED zip whose members are ``{name: bytes}`` (like NOMAD's raw-public.zip)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _parse_ranges(range_header: str, total: int) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for part in range_header.split("=", 1)[1].split(","):
        a, b = part.split("-")
        if a == "":                                    # suffix: bytes=-N
            out.append((max(0, total - int(b)), total - 1))
        else:
            out.append((int(a), min(int(b) if b else total - 1, total - 1)))
    return out


class _FakeResp:
    """A minimal stand-in for a streamed requests.Response over a byte range (single or
    multi-range), matching what :mod:`nomad_harvest.upload_zip` reads."""

    def __init__(self, data: bytes, ranges: list[tuple[int, int]]):
        total = len(data)
        if len(ranges) == 1:
            s, e = ranges[0]
            self.content = data[s:e + 1]
            self.headers = {"Content-Range": f"bytes {s}-{e}/{total}",
                            "Content-Type": "application/zip"}
        else:
            b = b"BOUND"
            buf = b""
            for s, e in ranges:
                buf += b"--" + b + b"\r\n"
                buf += f"Content-Range: bytes {s}-{e}/{total}\r\n\r\n".encode()
                buf += data[s:e + 1] + b"\r\n"
            buf += b"--" + b + b"--\r\n"
            self.content = buf
            self.headers = {"Content-Type": "multipart/byteranges; boundary=BOUND"}
        self.status_code = 206

    def iter_content(self, n):
        for i in range(0, len(self.content), n):
            yield self.content[i:i + n]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass


class FakeUploadClient:
    """Serves an in-memory pre-packed upload zip over HTTP Range (single + multi-range) via
    ``upload_raw_get``, plus the per-entry ``rawdir``/``download_raw_file`` endpoints for the
    fallback path. Uploads NOT present raise (→ UploadNotAvailable → per-entry fallback)."""

    def __init__(self, uploads: dict[str, bytes], entries: dict | None = None):
        self.uploads = uploads
        self.entries = entries or {}
        self.upload_gets = 0
        self.downloads = 0

    def upload_raw_get(self, upload_id, range_header, stream=False):
        self.upload_gets += 1
        if upload_id not in self.uploads:
            import requests
            raise requests.HTTPError(f"no such upload {upload_id}")
        data = self.uploads[upload_id]
        return _FakeResp(data, _parse_ranges(range_header, len(data)))

    def rawdir(self, entry_id):
        e = self.entries[entry_id]
        return {"mainfile": e["mainfile"], "files": e["files"]}

    def download_raw_file(self, entry_id, path, dest, decompress=False, on_chunk=None):
        blob = self.entries[entry_id]["blobs"][path]
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as fh:
            if on_chunk is not None:
                on_chunk(len(blob))
            fh.write(blob)
        tmp.replace(dest)
        self.downloads += 1
        return len(blob)


def test_read_central_directory_parses_all_members(tmp_path):
    members = {"calc1/vasprun.xml": b"<modeling>1</modeling>",
               "calc1/CHGCAR": b"chg" * 50,
               "calc2/vasprun.xml.bz2": b"\x42\x5a\x68payload"}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, total = upload_zip.read_central_directory(client, "U")
    assert set(cd) == set(members)
    assert cd["calc1/vasprun.xml"].method == 0               # STORED
    assert cd["calc1/vasprun.xml"].uncomp_size == len(members["calc1/vasprun.xml"])


def test_fetch_members_extracts_and_verifies_crc(tmp_path):
    members = {f"c{i}/vasprun.xml": (f"<modeling>{i}</modeling>".encode() * (i + 1))
               for i in range(5)}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    items = [(cd[name], tmp_path / f"out{i}.xml") for i, name in enumerate(members)]
    results = upload_zip.fetch_members(client, "U", items)
    assert all(results.values())
    for (m, dest), name in zip(items, members):
        assert dest.read_bytes() == members[name]           # exact bytes, CRC-verified


def test_fetch_members_flags_crc_mismatch(tmp_path, monkeypatch):
    members = {"c/vasprun.xml": b"<modeling/>"}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    m = cd["c/vasprun.xml"]
    m.crc = (m.crc ^ 0xFFFF) & 0xFFFFFFFF                    # corrupt the expected CRC
    results = upload_zip.fetch_members(client, "U", [(m, tmp_path / "out.xml")])
    assert results[tmp_path / "out.xml"] is False
    assert not (tmp_path / "out.xml").exists()              # a CRC-failed member is not written


def _upload_keep(tmp_path, specs):
    """specs: [(entry_id, upload_id, mainfile)] -> keep-list path."""
    rows = [{"entry_id": e, "upload_id": u, "mainfile": mf, "license": "CC BY 4.0",
             "references": [], "results": {"material": {"elements": ["Si"]}}}
            for e, u, mf in specs]
    p = tmp_path / "keep.jsonl"
    write_jsonl(p, rows)
    return p


def test_fetch_candidates_upload_path_stages_and_availability(tmp_path):
    # Two entries in ONE upload; the second's directory holds a CHGCAR -> charge_density=True,
    # derived from the central directory (no rawdir/query call).
    zbytes = _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>",
                        "b/vasprun.xml": b"<modeling>b</modeling>",
                        "b/CHGCAR": b"z" * 100})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"), ("B", "U", "b/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 2 and summary["uploads"] == 1
    assert summary["per_entry_fallback"] == 0               # everything via the fast path
    recs = {r["recid"]: r for r in read_jsonl(out)}
    assert set(recs) == {"A", "B"}
    assert recs["A"]["provenance"]["source"] == "nomad"
    assert recs["A"]["calc_units"][0]["vasprun"].endswith("vasprun.xml")
    assert recs["A"]["availability"]["charge_density"] is False
    assert recs["B"]["availability"]["charge_density"] is True   # CHGCAR sibling seen in the CD
    # the actual staged file is byte-identical to the zip member
    staged = tmp_path / "raw" / "A" / "extracted" / "calc" / "vasprun.xml"
    assert staged.read_bytes() == b"<modeling>a</modeling>"


def test_fetch_candidates_upload_falls_back_for_missing_member(tmp_path):
    # B's mainfile is absent from the zip -> recovered via the per-entry fallback (no loss).
    zbytes = _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>"})
    entries = {"B": {"mainfile": "b/vasprun.xml",
                     "files": [{"path": "b/vasprun.xml", "size": 11}],
                     "blobs": {"vasprun.xml": b"<modeling/>"}}}
    client = FakeUploadClient({"U": zbytes}, entries=entries)
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"), ("B", "U", "b/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 2
    assert summary["per_entry_fallback"] == 1               # B recovered per-entry
    assert client.downloads == 1
    assert {r["recid"] for r in read_jsonl(out)} == {"A", "B"}


def test_fetch_candidates_upload_valve_defers(tmp_path):
    # two members in one upload, budget ~1 record: first stages, second defers (reclaimable).
    big = b"x" * 600
    zbytes = _make_zip({"a/vasprun.xml": big, "b/vasprun.xml": big})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"), ("B", "U", "b/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                               max_disk_bytes=1000)
    assert summary["staged"] == 1
    assert summary["stopped_disk_budget"] is True


def test_fetch_candidates_upload_want_outcar(tmp_path):
    zbytes = _make_zip({"c/vasprun.xml": b"<modeling/>", "c/OUTCAR": b"outcar-bytes"})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "c/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out, want_outcar=True)
    calc = tmp_path / "raw" / "A" / "extracted" / "calc"
    assert (calc / "vasprun.xml").read_bytes() == b"<modeling/>"
    assert (calc / "OUTCAR").read_bytes() == b"outcar-bytes"    # sibling OUTCAR also staged


def test_fetch_candidates_upload_resume_skips(tmp_path):
    zbytes = _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>"})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    gets = client.upload_gets
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["skipped_existing"] == 1 and summary["staged"] == 0
    assert client.upload_gets == gets                        # no re-fetch (upload skipped wholesale)
    assert len(list(read_jsonl(out))) == 1


# --- whole-upload streaming extraction (the hybrid fetch's fast path) -----------

def test_stream_members_extracts_all_in_one_request(tmp_path):
    members = {f"c{i}/vasprun.xml": (f"<modeling>{i}</modeling>".encode() * (i + 1))
               for i in range(6)}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    items = [(cd[name], tmp_path / f"out{i}.xml") for i, name in enumerate(members)]
    client.upload_gets = 0
    results = upload_zip.stream_members(client, "U", items)
    assert all(results.values())
    assert client.upload_gets == 1                           # ONE streaming request for all members
    for (m, dest), name in zip(items, members):
        assert dest.read_bytes() == members[name]            # exact bytes, CRC-verified


def test_stream_members_crc_failure_does_not_desync(tmp_path):
    # A CRC-failed member is not written AND does not knock the stream out of alignment: the
    # next member still extracts (its bytes were consumed either way).
    members = {"c/vasprun.xml": b"<modeling/>", "d/vasprun.xml": b"<modeling>d</modeling>"}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    cd["c/vasprun.xml"].crc ^= 0xFFFF                        # corrupt the FIRST member's expected CRC
    dests = {n: tmp_path / f"{n.split('/')[0]}.xml" for n in members}
    results = upload_zip.stream_members(client, "U", [(cd[n], dests[n]) for n in members])
    assert results[dests["c/vasprun.xml"]] is False and not dests["c/vasprun.xml"].exists()
    assert results[dests["d/vasprun.xml"]] is True
    assert dests["d/vasprun.xml"].read_bytes() == members["d/vasprun.xml"]


def _vm(name, off, size):
    return upload_zip.ZipMember(name, 0, size, size, 0, off)   # STORED member at a synthetic offset


_FAST = 50 << 20      # MB/s where the fetch is throttle-bound (favours whole)
_SLOW = 1 << 20       # MB/s where the fetch is transfer-bound (favours targeted)


def _spaced_members(n, size, stride):
    """n members of `size` bytes at offsets 0, stride, 2*stride, ... (stride>size => interior bloat)."""
    members, entries = {}, []
    for i in range(n):
        name = f"c{i}/vasprun.xml"
        members[name] = _vm(name, i * stride, size)
        entries.append({"mainfile": name})
    return members, entries


def test_should_whole_stream_true_for_low_bloat_high_entry():
    members, entries = _spaced_members(300, 1000, 1040)      # contiguous (span ~= wanted): whole wins
    assert _should_whole_stream(members, entries, False, _FAST) is True


def test_should_whole_stream_false_for_high_bloat():
    members, entries = _spaced_members(300, 1000, 2_001_040)  # vaspruns ~2 MB apart -> ~600 MB span
    assert _should_whole_stream(members, entries, False, _FAST) is False   # even fast: too much bloat


def test_should_whole_stream_false_for_few_entries():
    members, entries = _spaced_members(50, 1000, 1040)       # <= MAX_RANGES_PER_REQUEST: 1 batch
    assert _should_whole_stream(members, entries, False, _FAST) is False


def test_should_whole_stream_bandwidth_dependent_for_medium_bloat():
    # THE fix: a MEDIUM-bloat upload (span ~1.35x wanted) is a whole-stream when the server is fast
    # (throttle-bound: fewer requests win) but TARGETED when it's slow (transfer-bound: fewer bytes
    # win — whole would over-fetch the interior bloat). 300 x 400 KB members, 540 KB stride.
    members, entries = _spaced_members(300, 400_000, 540_000)   # wanted 120 MB, span ~162 MB (1.35x)
    assert _should_whole_stream(members, entries, False, _FAST) is True     # fast -> whole
    assert _should_whole_stream(members, entries, False, _SLOW) is False    # slow -> targeted


def test_stream_members_chunks_large_span(tmp_path, monkeypatch):
    # With a tiny chunk cap, a multi-member span splits into SEVERAL requests, each extracting its
    # own members; all files still land byte-correct (a broken chunk would fail only its members).
    members = {f"c{i}/vasprun.xml": (f"<modeling>{i}</modeling>".encode() * 20) for i in range(6)}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    items = [(cd[name], tmp_path / f"out{i}.xml") for i, name in enumerate(members)]
    monkeypatch.setattr(upload_zip, "_STREAM_CHUNK_BYTES", 200)   # force ~1 member per chunk
    client.upload_gets = 0
    results = upload_zip.stream_members(client, "U", items)
    assert all(results.values())
    assert client.upload_gets >= 3                           # multiple chunk requests, not one
    for (m, dest), name in zip(items, members):
        assert dest.read_bytes() == members[name]


def test_fallback_aborts_after_consecutive_failures(tmp_path):
    # Upload absent from the zip store -> UploadNotAvailable -> per-entry fallback; every rawdir
    # call raises -> the fallback must ABORT after _FALLBACK_MAX_CONSEC_FAIL, not grind all N
    # (the live stall: one 500ing upload's thousands of entries burned the whole wallclock).
    from nomad_harvest.harvest import _FALLBACK_MAX_CONSEC_FAIL
    client = FakeUploadClient({}, entries={})               # no uploads, no per-entry data -> raises
    keep = _upload_keep(tmp_path, [(f"E{i}", "U", f"c{i}/vasprun.xml") for i in range(30)])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 0
    assert summary["failed"] <= _FALLBACK_MAX_CONSEC_FAIL + 1   # bailed early, did NOT process all 30
    assert summary["failed"] < 30


def test_fetch_candidates_global_dataset_skip(tmp_path):
    # An entry already parsed into the dataset is skipped globally (not re-downloaded), even though
    # it's not in this part's fetched.jsonl — the fix that makes a PARTS change free of re-fetch.
    zbytes = _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>",
                        "b/vasprun.xml": b"<modeling>b</modeling>"})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"), ("B", "U", "b/vasprun.xml")])
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    write_jsonl(dataset / "metadata.jsonl",                     # entry A already parsed
                [{"calc_id": "nomad:A:0", "provenance": {"source": "nomad", "record_id": "A"}}])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                               dataset_dir=dataset)
    assert summary["dataset_skipped"] == 1                      # A skipped because it's in the dataset
    recs = {r["recid"] for r in read_jsonl(out)}
    assert recs == {"B"}                                        # only B fetched; A NOT re-downloaded


def test_fetch_candidates_whole_stream_path_stages_identically(tmp_path):
    # >256 low-bloat entries in ONE upload -> the chooser picks whole-stream: everything staged in
    # ONE request, byte-identical, with the SAME fetched.jsonl the targeted path would produce
    # (so parse/store/verify see identical input + metadata).
    # realistically-sized members (data >> local-header overhead) so span/wanted ~ 1 (low bloat);
    # 22-byte toy members would make the header dominate the span and (correctly) pick targeted.
    body = {i: (f"<modeling>{i}</modeling>".encode() * 100) for i in range(260)}   # ~2.2 KB each
    members = {f"c{i}/vasprun.xml": body[i] for i in range(260)}
    client = FakeUploadClient({"U": _make_zip(members)})
    keep = _upload_keep(tmp_path, [(f"E{i}", "U", f"c{i}/vasprun.xml") for i in range(260)])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out)
    assert summary["staged"] == 260 and summary["per_entry_fallback"] == 0
    assert client.upload_gets == 2                           # 1 CD read + 1 whole-stream (not 260!)
    recs = {r["recid"]: r for r in read_jsonl(out)}
    assert len(recs) == 260
    assert recs["E7"]["provenance"]["source"] == "nomad"
    assert recs["E7"]["calc_units"][0]["vasprun"].endswith("vasprun.xml")
    staged = tmp_path / "raw" / "E7" / "extracted" / "calc" / "vasprun.xml"
    assert staged.read_bytes() == body[7]


# --- split by upload ------------------------------------------------------------

def test_split_by_upload_keeps_uploads_whole_and_balances(tmp_path):
    # 3 uploads (sizes 5,3,2) over 2 parts: each upload lands whole in ONE part; LPT balances.
    rows = ([{"entry_id": f"u1-{i}", "upload_id": "U1"} for i in range(5)]
            + [{"entry_id": f"u2-{i}", "upload_id": "U2"} for i in range(3)]
            + [{"entry_id": f"u3-{i}", "upload_id": "U3"} for i in range(2)])
    keep = tmp_path / "keep.jsonl"
    write_jsonl(keep, rows)
    info = split_by_upload(keep, 2, tmp_path / "parts")
    assert info["uploads"] == 3
    part_uploads = []
    for pw in info["parts_written"]:
        ups = {r["upload_id"] for r in read_jsonl(Path(pw["path"]))}
        part_uploads.append(ups)
    # every upload appears in exactly one part
    all_ups = set().union(*part_uploads)
    assert all_ups == {"U1", "U2", "U3"}
    assert sum(len(u) for u in part_uploads) == 3            # no upload split across parts
    # LPT: U1(5) -> part0; U2(3) -> part1; U3(2) -> part1  => 5 vs 5, balanced
    assert sorted(pw["lines"] for pw in info["parts_written"]) == [5, 5]


def test_split_by_upload_deterministic(tmp_path):
    rows = [{"entry_id": f"e{i}", "upload_id": f"U{i % 4}"} for i in range(20)]
    keep = tmp_path / "keep.jsonl"
    write_jsonl(keep, rows)
    a = split_by_upload(keep, 3, tmp_path / "p1")
    b = split_by_upload(keep, 3, tmp_path / "p2")
    assert [pw["lines"] for pw in a["parts_written"]] == [pw["lines"] for pw in b["parts_written"]]


# --- zip64 central-directory parsing (the >4 GB upload case) ---------------------

def test_parse_central_directory_zip64_extra():
    # A single CD header whose comp/uncomp/offset are the 32-bit sentinel, with the real
    # 64-bit values in the zip64 extra (0x0001) — exactly NOMAD's >4 GB uploads.
    name = b"deep/vasprun.xml"
    uncomp = comp = 5_000_000_000
    offset = 6_000_000_000
    extra = struct.pack("<HH", 0x0001, 24) + struct.pack("<QQQ", uncomp, comp, offset)
    S = 0xFFFFFFFF
    cdh = struct.pack("<IHHHHHHIIIHHHHHII",
                      0x02014b50, 20, 20, 0, 0, 0, 0, 0x1234,   # sig..crc
                      S, S,                                       # comp, uncomp (sentinels)
                      len(name), len(extra), 0, 0, 0, 0, S)       # nlen,elen,clen,disk,iattr,eattr,offset
    members = upload_zip._parse_central_directory(cdh + name + extra)
    assert len(members) == 1
    m = members[0]
    assert m.name == "deep/vasprun.xml"
    assert m.comp_size == comp and m.uncomp_size == uncomp and m.local_offset == offset


def test_fetch_members_big_member_streams_to_disk(tmp_path, monkeypatch):
    # Force the single-member streaming path (used for large OUTCARs > MAX_BATCH_BYTES).
    monkeypatch.setattr(upload_zip, "MAX_BATCH_BYTES", 1)
    members = {"c/OUTCAR": b"outcar-line\n" * 5000, "c/vasprun.xml": b"<modeling/>"}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    dests = {name: tmp_path / name.replace("/", "_") for name in members}
    results = upload_zip.fetch_members(client, "U", [(cd[n], dests[n]) for n in members])
    assert all(results.values())
    for name in members:
        assert dests[name].read_bytes() == members[name]        # exact bytes, CRC-verified


def test_fetch_members_big_member_crc_mismatch(tmp_path, monkeypatch):
    monkeypatch.setattr(upload_zip, "MAX_BATCH_BYTES", 1)
    members = {"c/OUTCAR": b"x" * 4000}
    client = FakeUploadClient({"U": _make_zip(members)})
    cd, _ = upload_zip.read_central_directory(client, "U")
    m = cd["c/OUTCAR"]
    m.crc ^= 0x1
    dest = tmp_path / "OUTCAR"
    assert upload_zip.fetch_members(client, "U", [(m, dest)])[dest] is False
    assert not dest.exists()


# --- dead-upload skip-list + hoisted dataset-ids + .done marker decision (2026-08-21) -----

def test_fetch_candidates_dead_upload_skiplist(tmp_path):
    # An upload whose pre-packed zip is unreadable (absent -> UploadNotAvailable) AND whose
    # per-entry fallback stages nothing is a failed pass; after _DEAD_UPLOAD_MAX_FAILS passes it
    # is skipped WHOLESALE (no CD read attempted) instead of re-burning time every resume.
    client = FakeUploadClient({"U": _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>"})})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"),
                                   ("D1", "D", "x/vasprun.xml"), ("D2", "D", "y/vasprun.xml")])
    out, raw = tmp_path / "fetched.jsonl", tmp_path / "raw"
    dead_file = tmp_path / "manifests" / "nomad_dead_uploads.json"
    for _ in range(_DEAD_UPLOAD_MAX_FAILS):                 # attempted each pass, count increments
        s = fetch_candidates(client, keep, raw_dir=raw, out_path=out)
        assert s["failed"] >= 2 and s["dead_skipped"] == 0
    assert _load_dead_uploads(dead_file).get("D") == _DEAD_UPLOAD_MAX_FAILS
    gets_before = client.upload_gets
    s = fetch_candidates(client, keep, raw_dir=raw, out_path=out)   # now past threshold -> skipped
    assert s["dead_skipped"] == 2 and s["uploads"] == 0
    assert client.upload_gets == gets_before               # no CD read attempted for the dead upload
    assert {r["recid"] for r in read_jsonl(out)} == {"A"}  # A was staged on pass 1, never lost


def test_fetch_candidates_dead_upload_recovers_clears_count(tmp_path):
    # A transient failure that later recovers is NOT abandoned: staging any entry clears the count.
    client = FakeUploadClient({"U": _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>"})})
    keep = _upload_keep(tmp_path, [("D1", "D", "d/vasprun.xml")])
    out, raw = tmp_path / "fetched.jsonl", tmp_path / "raw"
    dead_file = tmp_path / "manifests" / "nomad_dead_uploads.json"
    fetch_candidates(client, keep, raw_dir=raw, out_path=out)       # D absent -> fails, count 1
    assert _load_dead_uploads(dead_file).get("D") == 1
    client.uploads["D"] = _make_zip({"d/vasprun.xml": b"<modeling>d</modeling>"})   # now available
    s = fetch_candidates(client, keep, raw_dir=raw, out_path=out)   # recovers -> staged, count cleared
    assert s["staged"] == 1
    assert "D" not in _load_dead_uploads(dead_file)


def test_fetch_candidates_hoisted_dataset_record_ids(tmp_path):
    # The precomputed dataset_record_ids set skips already-parsed entries WITHOUT reading
    # metadata.jsonl per call (the hoist that removes the per-part resume churn). No dataset dir
    # exists on disk, proving the read is skipped when the set is supplied.
    zbytes = _make_zip({"a/vasprun.xml": b"<modeling>a</modeling>",
                        "b/vasprun.xml": b"<modeling>b</modeling>"})
    client = FakeUploadClient({"U": zbytes})
    keep = _upload_keep(tmp_path, [("A", "U", "a/vasprun.xml"), ("B", "U", "b/vasprun.xml")])
    out = tmp_path / "fetched.jsonl"
    summary = fetch_candidates(client, keep, raw_dir=tmp_path / "raw", out_path=out,
                               dataset_record_ids={"A"})
    assert summary["dataset_skipped"] == 1
    assert {r["recid"] for r in read_jsonl(out)} == {"B"}   # A skipped, only B fetched


def test_part_is_complete_marker_decision():
    ok = {"stopped_disk_budget": False, "dead_pending": 0}
    assert _part_is_complete(ok, {"rejections": 0}) is True          # clean pass -> mark done
    assert _part_is_complete(ok, {"rejections": 2}) is False         # a new rejection -> retry
    assert _part_is_complete({"stopped_disk_budget": True, "dead_pending": 0},
                             {"rejections": 0}) is False             # valve-deferred -> not done
    assert _part_is_complete({"stopped_disk_budget": False, "dead_pending": 1},
                             {"rejections": 0}) is False             # dead upload pending -> not done
    assert _part_is_complete(ok, None) is False                      # parse failed/absent -> not done
