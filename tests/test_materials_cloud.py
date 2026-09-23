"""Offline tests for the Materials Cloud adapter (no network).

Cover the pure logic (record normalisation, licence policy, DOI/mcid parsing, AiiDA-aware file
classification, overlap flags), the API client's paging/retry against a fake session, the
ZIP64-aware remote central-directory reader + AiiDA-format detection + single-member fetch
against in-memory zips served over fake HTTP Range, the triage evidence policy + file pruning +
fetch-unit split, the backward-compatible hooks added to the SHARED fetch (provenance passthrough,
declared ``archive_kind``, ``session_factory``), and — when pymatgen/ASE and ASE's bundled VASP
fixtures are available — a full triage → shared fetch → shared parse → verify run on a
legacy-AiiDA-export-shaped zip, proving calc_ids are namespaced ``materials_cloud:`` and IDENTICAL
whether or not a record is split into fetch units. The live path is ``cli smoke``.

Run: ``python -m pytest tests/test_materials_cloud.py -q`` from the repo root.
"""

from __future__ import annotations

import hashlib
import io
import json
import struct
import zipfile
from pathlib import Path
from typing import Any

import pytest

from zenodo_harvest import fetch as zfetch
from zenodo_harvest.manifest import read_jsonl

from materials_cloud_harvest import client as mc_client
from materials_cloud_harvest import cli as mc_cli
from materials_cloud_harvest.client import MaterialsCloudClient, content_url, new_session
from materials_cloud_harvest.discover import discover, nomad_mc_references, zenodo_index
from materials_cloud_harvest.records import (
    classify_mc_files,
    licence_id,
    licence_verdict,
    mcid_base,
    normalize_doi,
    record_to_candidate,
    title_similarity,
    title_tokens,
    vasp_mentioned,
)
from materials_cloud_harvest.remote_zip import (
    RemoteZipError,
    aiida_format,
    fetch_member,
    peek_archive,
    read_central_directory,
    zip_evidence,
)
from materials_cloud_harvest.triage import split_units, triage, unit_id

BASE = mc_client.BASE


# --------------------------------------------------------------------------- #
# helpers: fake MC records + a fake HTTP session serving bytes with Range      #
# --------------------------------------------------------------------------- #

def _entry(key: str, size: int, md5: str = "0" * 32) -> dict:
    return {"id": "x", "key": key, "size": size, "checksum": f"md5:{md5}",
            "ext": key.rsplit(".", 1)[-1], "mimetype": "application/octet-stream"}


def mc_record(rid: str, files: dict[str, int] | None = None, *, title: str = "A dataset",
              description: str = "Some DFT data.", rights: list[dict] | None = None,
              subjects: list[str] | None = None, parent: str | None = None,
              access: dict | None = None, related: list[dict] | None = None,
              mcid: str = "2020.0001/v1", checksums: dict[str, str] | None = None) -> dict:
    files = files if files is not None else {"data.zip": 100}
    checksums = checksums or {}
    return {
        "id": rid, "created": "2020-01-01T00:00:00+00:00", "updated": "2020-01-02T00:00:00+00:00",
        "links": {"self_html": f"{BASE}/records/{rid}"},
        "parent": {"id": parent or f"p-{rid}",
                   "pids": {"doi": {"identifier": f"10.24435/materialscloud:c-{rid}"}}},
        "versions": {"is_latest": True, "index": 1},
        "pids": {"doi": {"identifier": f"10.24435/materialscloud:{mcid}"},
                 "mcid": {"identifier": mcid}},
        "metadata": {
            "resource_type": {"id": "dataset"}, "title": title, "description": description,
            "publication_date": "2020-01-01",
            "creators": [{"person_or_org": {"name": "Doe, Jane"}}],
            "subjects": [{"subject": s} for s in (subjects or [])],
            "rights": rights if rights is not None else [{"id": "cc-by-4.0"}],
            "related_identifiers": related or [],
        },
        "custom_fields": {"mc_references": [{
            "ref_citation": "J. Doe, Journal 1 (2020)", "ref_resource_type": "publication-article",
            "ref_link": {"ref_scheme": "doi", "ref_identifier": "10.1000/paper.1"}}]},
        "access": access or {"record": "public", "files": "public",
                             "embargo": {"active": False}, "status": "open"},
        "files": {"enabled": True, "count": len(files), "total_bytes": sum(files.values()),
                  "entries": {k: _entry(k, v, checksums.get(k, "0" * 32))
                              for k, v in files.items()}},
    }


class FakeResp:
    def __init__(self, status: int, body: bytes = b"", headers: dict | None = None,
                 json_obj: Any = None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self._json = json_obj

    @property
    def content(self) -> bytes:
        return self._body

    def json(self) -> Any:
        return self._json

    def iter_content(self, n: int = 1 << 20):
        for i in range(0, len(self._body), n):
            yield self._body[i:i + n]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def close(self) -> None:
        pass

    def __enter__(self) -> "FakeResp":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


class FakeFileSession:
    """Serves ``{url: bytes}`` with HTTP Range semantics (single range / suffix range) — the
    behaviour MC's presigned S3 side exhibits after the 302. Records every request's headers."""

    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs
        self.headers: dict[str, str] = {}
        self.requests: list[tuple[str, dict]] = []

    def get(self, url: str, headers: dict | None = None, **_: Any) -> FakeResp:
        hdrs = {**self.headers, **(headers or {})}
        self.requests.append((url, hdrs))
        body = self.blobs.get(url)
        if body is None:
            return FakeResp(404)
        rng = hdrs.get("Range")
        if not rng:
            return FakeResp(200, body, {"Content-Length": str(len(body))})
        spec = rng.split("=", 1)[1]
        n = len(body)
        if spec.startswith("-"):
            start, end = max(0, n - int(spec[1:])), n - 1
        else:
            a, _, b = spec.partition("-")
            start, end = int(a), (int(b) if b else n - 1)
        if start >= n:
            return FakeResp(416, b"", {"Content-Range": f"bytes */{n}"})
        end = min(end, n - 1)
        return FakeResp(206, body[start:end + 1],
                        {"Content-Range": f"bytes {start}-{end}/{n}",
                         "Content-Length": str(end - start + 1)})

    def close(self) -> None:
        pass

    def __enter__(self) -> "FakeFileSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


def md5s(blobs: dict[str, bytes]) -> dict[str, str]:
    return {k: hashlib.md5(b).hexdigest() for k, b in blobs.items()}


def make_zip(members: dict[str, bytes], method: int = zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=method) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def make_zip64(members: dict[str, bytes], monkeypatch: pytest.MonkeyPatch) -> bytes:
    """A real ZIP64 archive (ZIP64 EOCD record + locator) whose classic EOCD carries the
    0xFFFFFFFF sentinels — so a reader MUST follow the ZIP64 record to find the central directory."""
    monkeypatch.setattr(zipfile, "ZIP_FILECOUNT_LIMIT", 1)     # force the ZIP64 end records
    data = bytearray(make_zip(members))
    i = data.rfind(b"PK\x05\x06")
    data[i + 12:i + 20] = struct.pack("<II", 0xFFFFFFFF, 0xFFFFFFFF)
    return bytes(data)


# --------------------------------------------------------------------------- #
# records: normalisation, licence policy, identifiers                          #
# --------------------------------------------------------------------------- #

def test_content_url_percent_encodes_keys():
    url = content_url("ab12-cd34", "Figs. 1c–e, 2.zip")
    assert url == f"{BASE}/api/records/ab12-cd34/files/Figs.%201c%E2%80%93e%2C%202.zip/content"
    assert content_url("r", "a/b.tar.gz").endswith("/files/a%2Fb.tar.gz/content")


def test_licence_id_skips_id_less_addendum():
    meta = {"rights": [{"title": {"en": "License addendum"}}, {"id": "cc-by-sa-4.0"}]}
    assert licence_id(meta) == "cc-by-sa-4.0"
    assert licence_id({"rights": []}) is None


@pytest.mark.parametrize("lic,policy,keep", [
    ("cc-by-4.0", "nc-ok", True), ("cc0-1.0", "nc-ok", True), ("mit", "nc-ok", True),
    ("cc-by-sa-4.0", "nc-ok", True),
    ("cc-by-nc-4.0", "nc-ok", True), ("cc-by-nc-sa-4.0", "nc-ok", True),
    ("cc-by-nc-4.0", "strict", False),
    ("cc-by-nd-4.0", "nc-ok", False), ("cc-by-nc-nd-4.0", "nc-ok", False),
    ("mcloud-ne-1.0", "nc-ok", False), ("asl", "nc-ok", False), ("mcloud-ne-1.0", "strict", False),
    (None, "nc-ok", False), ("notspecified", "nc-ok", False),
    ("mcloud-ne-1.0", "none", True), (None, "none", True),
])
def test_licence_verdict_policies(lic, policy, keep):
    assert licence_verdict(lic, policy)[0] is keep


def test_licence_verdict_rejects_unknown_policy():
    with pytest.raises(ValueError):
        licence_verdict("cc-by-4.0", "lenient")


def test_normalize_doi_and_mcid():
    assert normalize_doi("https://doi.org/10.24435/materialscloud:2020.0006/v1") == \
        "10.24435/materialscloud:2020.0006/v1"
    assert normalize_doi("see 10.5281/zenodo.4683140.") == "10.5281/zenodo.4683140"
    assert normalize_doi(None) is None
    assert mcid_base("10.24435/materialscloud:2020.0006/v1") == "2020.0006"
    assert mcid_base("https://archive.materialscloud.org/record/2021.12/v3") == "2021.12"
    assert mcid_base("2020.0006/v1") == "2020.0006"
    assert mcid_base("10.24435/materialscloud:wm-6j") is None       # concept DOI: no mcid


def test_vasp_mention_uses_record_text_not_affiliations():
    meta = {"title": "Equations of state", "description": "AiiDA common workflows.",
            "creators": [{"affiliations": [{"name": "VASP Software GmbH"}]}]}
    assert not vasp_mentioned(meta)
    assert vasp_mentioned({"title": "x", "description": "computed with VASP 6.3"})
    assert vasp_mentioned({"title": "x", "description": "", "subjects": [{"subject": "vasprun"}]})


def test_classify_mc_files_ranks_aiida_only_record_as_archive():
    fc = classify_mc_files([{"key": "export.aiida", "size": 5}, {"key": "README.txt", "size": 1}])
    assert fc["category"] == "archive" and fc["rank"] == 3
    assert "export.aiida" in fc["archives"]
    # a normal archive record is unchanged
    assert classify_mc_files([{"key": "a.tar.gz", "size": 5}])["category"] == "archive"


def test_record_to_candidate_shape_and_provenance():
    rec = mc_record("ab12-cd34", {"b.tar.gz": 30, "a README.txt": 1, "c.zip": 20},
                    description="We used VASP and Quantum ESPRESSO.",
                    related=[{"identifier": "10.5281/zenodo.99", "scheme": "doi",
                              "relation_type": {"id": "issupplementto"}}])
    c = record_to_candidate(rec)
    assert c["recid"] == "ab12-cd34" and c["conceptrecid"] == "p-ab12-cd34"
    assert [f["key"] for f in c["files"]] == ["a README.txt", "b.tar.gz", "c.zip"]  # sorted
    f = c["files"][1]
    assert f["ext"] == ".tar.gz" and f["checksum"].startswith("md5:")
    assert f["download"] == content_url("ab12-cd34", "b.tar.gz")
    assert c["vasp_mention"] and "quantum_espresso" in c["other_codes"]
    assert c["access_right"] == "open" and c["license"] == "cc-by-4.0"
    prov = c["provenance"]
    assert prov["source"] == "materials_cloud" and prov["record_id"] == "ab12-cd34"
    assert prov["doi"] == "10.24435/materialscloud:2020.0001/v1"
    assert prov["conceptdoi"] == "10.24435/materialscloud:c-ab12-cd34"
    assert prov["mcid"] == "2020.0001/v1" and prov["license"] == "cc-by-4.0"
    assert prov["references"][0]["identifier"] == "10.1000/paper.1"
    assert prov["related_identifiers"][0]["relation"] == "issupplementto"
    json.dumps(c)                                     # keep-list rows must be JSON-serialisable


def test_record_to_candidate_non_public_access():
    rec = mc_record("r1", access={"record": "public", "files": "restricted", "status": "restricted"})
    assert record_to_candidate(rec)["access_right"] == "restricted"


def test_title_similarity():
    a = title_tokens("Hidden spontaneous polarisation in the chalcohalide absorber Sn2SbS2I3")
    b = title_tokens("Hidden Spontaneous Polarisation in the ns2-Cation Sn2SbS2I3 Chalcohalide Absorber")
    assert title_similarity(a, b) >= 0.6
    assert title_similarity(a, title_tokens("Protein folding kinetics")) == 0.0


# --------------------------------------------------------------------------- #
# client                                                                       #
# --------------------------------------------------------------------------- #

class _PagingSession:
    def __init__(self, pages: list[dict], statuses: list[int] | None = None):
        self.pages, self.calls = pages, []
        self.statuses = list(statuses or [])
        self.headers: dict[str, str] = {}

    def get(self, url, params=None, timeout=None, **_):
        self.calls.append(dict(params or {}))
        if self.statuses:
            code = self.statuses.pop(0)
            if code != 200:
                return FakeResp(code, headers={"Retry-After": "0"})
        page = int((params or {}).get("page", 1))
        return FakeResp(200, json_obj=self.pages[page - 1])


def test_iter_records_pages_and_dedups(monkeypatch):
    monkeypatch.setattr(mc_client.time, "sleep", lambda s: None)
    pages = [{"hits": {"total": 3, "hits": [{"id": "a"}, {"id": "b"}]}},
             {"hits": {"total": 3, "hits": [{"id": "b"}, {"id": "c"}]}}]
    c = MaterialsCloudClient(min_interval=0, session=_PagingSession(pages))  # type: ignore[arg-type]
    assert [r["id"] for r in c.iter_records(page_size=2)] == ["a", "b", "c"]
    assert c.session.calls[0]["sort"] == "oldest"                       # type: ignore[attr-defined]


def test_iter_records_refuses_past_search_window(monkeypatch):
    pages = [{"hits": {"total": 10_001, "hits": [{"id": "a"}]}}]
    c = MaterialsCloudClient(min_interval=0, session=_PagingSession(pages))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        list(c.iter_records())


def test_client_waits_out_429_and_retries_5xx(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(mc_client.time, "sleep", lambda s: slept.append(s))
    pages = [{"hits": {"total": 1, "hits": [{"id": "a"}]}}]
    sess = _PagingSession(pages, statuses=[429, 503, 200])
    c = MaterialsCloudClient(min_interval=0, session=sess)  # type: ignore[arg-type]
    assert [r["id"] for r in c.iter_records()] == ["a"]
    assert len(sess.calls) == 3 and len(slept) >= 2


def test_new_session_is_anonymous_even_with_zenodo_token(monkeypatch):
    monkeypatch.setenv("ZENODO_TOKEN", "zenodo-secret")
    s = new_session()
    assert "Authorization" not in s.headers and "materials-mlip-mc" in s.headers["User-Agent"]


# --------------------------------------------------------------------------- #
# remote zip: ZIP64 central directory, AiiDA formats, member fetch             #
# --------------------------------------------------------------------------- #

def test_read_central_directory_plain_zip():
    z = make_zip({"calc/vasprun.xml": b"<xml/>", "calc/CHGCAR": b"x" * 100, "notes.txt": b"n"})
    s = FakeFileSession({"u": z})
    members, total = read_central_directory(s, "u")   # type: ignore[arg-type]
    assert total == len(z) and {m.name for m in members} == {
        "calc/vasprun.xml", "calc/CHGCAR", "notes.txt"}


def test_read_central_directory_follows_zip64_records(monkeypatch):
    z = make_zip64({f"nodes/aa/bb/{i}/path/vasprun.xml": b"<x/>" for i in range(3)}, monkeypatch)
    i = z.rfind(b"PK\x05\x06")
    assert struct.unpack("<I", z[i + 16:i + 20])[0] == 0xFFFFFFFF      # sentinel really set
    members, _ = read_central_directory(FakeFileSession({"u": z}), "u")  # type: ignore[arg-type]
    assert len(members) == 3 and all(m.name.endswith("vasprun.xml") for m in members)


def test_read_central_directory_rejects_non_zip_and_huge_cd():
    s = FakeFileSession({"tar": b"\x1f\x8b" + b"\0" * 5000, "z": make_zip({"a": b"1"})})
    with pytest.raises(RemoteZipError, match="not a zip"):
        read_central_directory(s, "tar")                  # type: ignore[arg-type]
    with pytest.raises(RemoteZipError, match="> cap"):
        read_central_directory(s, "z", max_cd_bytes=10)   # type: ignore[arg-type]


def test_aiida_format_detection():
    assert aiida_format(["metadata.json", "data.json", "nodes/ab/cd/x/path/OUTCAR"]) == "legacy"
    assert aiida_format(["repo/abc123", "metadata.json", "db.sqlite3"]) == "sqlite_zip"
    assert aiida_format(["calc/vasprun.xml"]) is None


def test_zip_evidence_rules_match_fetch():
    from nomad_harvest.upload_zip import ZipMember
    ms = [ZipMember(n, 8, 1, 10, 0, 0) for n in (
        "run1/vasprun.xml", "run1/INCAR", "run2/OUTCAR.gz", "__MACOSX/run1/._vasprun.xml",
        "run3/._OUTCAR", "inner/more.tar.gz", "structures.json.gz", "dir/")]
    ev = zip_evidence(ms)
    assert ev.primary == ["run1/vasprun.xml", "run2/OUTCAR.gz"]      # AppleDouble junk skipped
    assert ev.nested == ["inner/more.tar.gz"]                         # bare .gz member != archive
    assert ev.vasp_any == 3 and ev.primary_bytes == 20


def test_peek_archive_statuses():
    s = FakeFileSession({"ok": make_zip({"a/OUTCAR": b"x"}), "tar": b"\0" * 2000})
    ev, st = peek_archive(s, "ok")          # type: ignore[arg-type]
    assert st == "ok" and ev is not None and ev.primary == ["a/OUTCAR"]
    assert peek_archive(s, "tar")[1] == "not_zip"    # type: ignore[arg-type]
    assert peek_archive(s, "missing")[1].startswith("peek_failed")  # type: ignore[arg-type]


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_fetch_member_streams_and_verifies(tmp_path, method):
    payload = b"SQLite format 3\0" + bytes(range(256)) * 50
    z = make_zip({"repo/0a": b"zz", "db.sqlite3": payload}, method=method)
    s = FakeFileSession({"u": z})
    members, _ = read_central_directory(s, "u")   # type: ignore[arg-type]
    db = next(m for m in members if m.name == "db.sqlite3")
    assert fetch_member(s, "u", db, tmp_path / "db.sqlite3")   # type: ignore[arg-type]
    assert (tmp_path / "db.sqlite3").read_bytes() == payload
    bad = type(db)(db.name, db.method, db.comp_size, db.uncomp_size, db.crc ^ 1, db.local_offset)
    assert not fetch_member(s, "u", bad, tmp_path / "bad")     # type: ignore[arg-type]
    assert not (tmp_path / "bad").exists()


# --------------------------------------------------------------------------- #
# discover: gates + overlap flags                                              #
# --------------------------------------------------------------------------- #

def _write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return path


def test_discover_gates_and_order(tmp_path):
    recs = [
        mc_record("r-qe", description="Quantum ESPRESSO phonons"),
        mc_record("r-vasp", description="VASP relaxations"),
        mc_record("r-nc", rights=[{"id": "cc-by-nc-4.0"}]),
        mc_record("r-nd", rights=[{"id": "cc-by-nd-4.0"}]),
        mc_record("r-ne", rights=[{"id": "mcloud-ne-1.0"}]),
        mc_record("r-priv", access={"record": "public", "files": "restricted"}),
        mc_record("r-excl"),
    ]
    out = tmp_path / "m" / "mc_candidates.jsonl"
    s = discover(MaterialsCloudClient(), out, records=recs, exclude_ids=["r-excl"])
    ids = [c["recid"] for c in read_jsonl(out)]
    assert ids[0] == "r-vasp"                               # VASP-mentioning first
    assert set(ids) == {"r-vasp", "r-qe", "r-nc"}           # NC admitted under nc-ok
    assert s["dropped_by_reason"] == {"non_redistributable_license": 2, "not_open_access": 1,
                                      "manually_excluded": 1}
    rej = {r["id"]: r["reason"] for r in read_jsonl(tmp_path / "m" / "mc_rejections.jsonl")}
    assert rej["r-ne"] == "non_redistributable_license:mc_nonopen_licence"
    assert rej["r-nd"] == "non_redistributable_license:no_derivatives"


def test_discover_strict_policy_drops_nc(tmp_path):
    out = tmp_path / "c.jsonl"
    discover(MaterialsCloudClient(), out, records=[mc_record("r-nc", rights=[{"id": "cc-by-nc-4.0"}])],
             licence_policy="strict")
    assert list(read_jsonl(out)) == []


def test_discover_flags_but_keeps_zenodo_and_nomad_overlap(tmp_path):
    zmeta = _write_jsonl(tmp_path / "z" / "metadata.jsonl", [
        {"calc_id": "zenodo:4683140:a", "provenance": {
            "source": "zenodo", "record_id": "4683140", "doi": "10.5281/zenodo.4683140",
            "title": "Hidden Spontaneous Polarisation in the ns2-Cation Sn2SbS2I3 Chalcohalide"}},
        {"calc_id": "zenodo:4683140:b", "provenance": {"record_id": "4683140"}}])
    nmeta = _write_jsonl(tmp_path / "n" / "metadata.jsonl", [
        {"calc_id": "nomad:e1:x", "provenance": {"references": [
            "https://doi.org/10.24435/materialscloud:2021.0042/v1"]}},
        {"calc_id": "nomad:e2:x", "provenance": {"references": [
            "https://archive.materialscloud.org/record/2021.0042/v2"]}},
        {"calc_id": "nomad:e3:x", "provenance": {"references": ["10.1000/unrelated"]}}])
    refs = nomad_mc_references(nmeta)
    assert refs["mcid:2021.0042"] == {0, 1}                  # both calcs, by DOI and by old URL
    assert refs["doi:10.24435/materialscloud:2021.0042/v1"] == {0}
    zi = zenodo_index(zmeta)
    assert zi["dois"] == {"10.5281/zenodo.4683140": "4683140"}
    rec = mc_record("hmdsb", title="Hidden spontaneous polarisation in the chalcohalide Sn2SbS2I3",
                    mcid="2021.0042/v1",
                    related=[{"identifier": "10.5281/zenodo.4683140", "scheme": "doi",
                              "relation_type": {"id": "issupplementto"}}])
    out = tmp_path / "c.jsonl"
    s = discover(MaterialsCloudClient(), out, records=[rec], zenodo_metadata=zmeta,
                 nomad_metadata=nmeta)
    (c,) = list(read_jsonl(out))                          # FLAGGED, not dropped
    assert s["flagged_overlap"] == 1
    assert c["overlap"]["zenodo_linked_in_dataset"] == ["10.5281/zenodo.4683140 (zenodo:4683140)"]
    assert c["overlap"]["zenodo_title_similar"][0]["zenodo_record_id"] == "4683140"
    assert c["overlap"]["nomad_calcs_citing"] == 2
    assert c["provenance"]["linked_harvested"] == c["overlap"]
    out2 = tmp_path / "c2.jsonl"
    discover(MaterialsCloudClient(), out2, records=[rec], zenodo_metadata=zmeta, drop_linked=True)
    assert list(read_jsonl(out2)) == []                   # opt-in drop still available


# --------------------------------------------------------------------------- #
# triage: evidence policy, pruning, AiiDA handling, allowlist, fetch units     #
# --------------------------------------------------------------------------- #

def _triage_case(tmp_path: Path, recs: list[dict], blobs: dict[tuple[str, str], bytes],
                 **kw: Any) -> tuple[dict, list[dict], dict]:
    cands = [record_to_candidate(r) for r in recs]
    cpath = _write_jsonl(tmp_path / "m" / "cand.jsonl", cands)
    sess = FakeFileSession({content_url(rid, key): b for (rid, key), b in blobs.items()})
    kp = tmp_path / "m" / "keep.jsonl"
    s = triage(cpath, kp, session=sess, interval=0.0, peek_retry_wait=0.0,  # type: ignore[arg-type]
               **kw)
    report = json.loads((tmp_path / "m" / "keep.report.json").read_text())
    return s, list(read_jsonl(kp)), report


def test_triage_mention_and_evidence_policy(tmp_path):
    vz = make_zip({"calcs/Si/vasprun.xml": b"<x/>", "calcs/Si/CHGCAR": b"c" * 50})
    empty = make_zip({"structures/POSCAR_1": b"p", "readme.md": b"r"})
    recs = [
        mc_record("m-yes", {"a.zip": len(vz)}, description="VASP"),           # mention + VASP
        mc_record("m-empty", {"a.zip": len(empty)}, description="VASP"),      # mention, proven empty
        mc_record("m-tar", {"a.tar.gz": 999}, description="VASP"),            # mention, unpeekable
        mc_record("q-hidden", {"a.zip": len(vz)}, description="QE"),          # blind spot recovered
        mc_record("q-tar", {"a.tar.gz": 999}, description="QE"),              # no evidence -> drop
        mc_record("q-empty", {"a.zip": len(empty)}, description="QE"),
    ]
    blobs = {("m-yes", "a.zip"): vz, ("m-empty", "a.zip"): empty, ("q-hidden", "a.zip"): vz,
             ("q-empty", "a.zip"): empty}
    s, keep, report = _triage_case(tmp_path, recs, blobs)
    by = {u["unit"]["record_id"]: u for u in keep}
    assert set(by) == {"m-yes", "m-tar", "q-hidden"}
    assert by["q-hidden"]["triage_reason"] == "vasp_evidence"
    assert by["m-tar"]["triage_reason"] == "vasp_mention"
    assert by["m-yes"]["vasp_category"] == "vasp_direct"
    assert s["blind_spot_recovered"] == ["q-hidden"]
    assert s["decisions"]["peek_proved_no_vasp"] == 1 and s["decisions"]["no_vasp_evidence"] == 2
    rej = {r["id"]: r["reason"] for r in read_jsonl(tmp_path / "m" / "mc_rejections.jsonl")}
    assert rej == {"m-empty": "peek_proved_no_vasp", "q-tar": "no_vasp_evidence",
                   "q-empty": "no_vasp_evidence"}
    assert {r["recid"] for r in report["records"]} == {u["recid"] for u in recs_ids(recs)}


def recs_ids(recs: list[dict]) -> list[dict]:
    return [{"recid": r["id"]} for r in recs]


def test_triage_prunes_proven_empty_zips_and_keeps_unpeekables(tmp_path):
    vz = make_zip({"r/OUTCAR": b"o"})
    empty = make_zip({"figs/f1.png": b"p"})
    rec = mc_record("m", {"a.zip": len(vz), "b.zip": len(empty), "c.tar.gz": 5, "README.md": 3},
                    description="VASP")
    _, keep, _ = _triage_case(tmp_path, [rec], {("m", "a.zip"): vz, ("m", "b.zip"): empty},
                              split=False)
    (u,) = keep
    assert [f["key"] for f in u["files"]] == ["README.md", "a.zip", "c.tar.gz"]  # b.zip pruned


def test_triage_aiida_legacy_marked_zip_sqlite_is_gap(tmp_path):
    legacy = make_zip({"metadata.json": b"{}", "data.json": b"{}",
                       "nodes/ab/cd/uuid1/path/vasprun.xml": b"<x/>",
                       "nodes/ab/cd/uuid1/path/DOSCAR": b"d"})
    sqlite = make_zip({"metadata.json": b"{}", "repo/" + "a" * 64: b"blob", "db.sqlite3": b"s"})
    recs = [mc_record("leg", {"x.aiida": len(legacy)}, description="QE"),
            mc_record("sq", {"y.aiida": len(sqlite)}, description="VASP")]
    s, keep, report = _triage_case(tmp_path, recs, {("leg", "x.aiida"): legacy,
                                                    ("sq", "y.aiida"): sqlite})
    (u,) = keep
    assert u["unit"]["record_id"] == "leg" and u["files"][0]["archive_kind"] == "zip"
    assert u["recid"] == unit_id("leg", "x.aiida")
    sq = next(r for r in report["records"] if r["recid"] == "sq")
    assert sq["reason"] == "evidence_gap_only" and "sqlite_zip" in sq["gaps"][0]
    assert s["records_with_gaps"] == 1


def test_triage_default_allowlist_restricts_bosoni(tmp_path):
    vz = make_zip({"nodes/aa/bb/u/path/vasprun.xml": b"<x/>", "data.json": b"{}"})
    qe = make_zip({"nodes/cc/dd/u/path/aiida.out": b"qe", "data.json": b"{}"})
    rec = mc_record("yf0rj-w3r97", {"acwf_unaries_results_vasp.aiida": len(vz),
                                    "acwf_unaries_results_quantum_espresso.aiida": len(qe),
                                    "BigDFT_acwf_chunked.tar": 10**9})
    s, keep, _ = _triage_case(tmp_path, [rec], {
        ("yf0rj-w3r97", "acwf_unaries_results_vasp.aiida"): vz,
        ("yf0rj-w3r97", "acwf_unaries_results_quantum_espresso.aiida"): qe})
    (u,) = keep
    assert [f["key"] for f in u["files"]] == ["acwf_unaries_results_vasp.aiida"]
    assert s["peeked"] == 1                                   # the QE export was never read


def test_triage_splits_multi_archive_records_into_units(tmp_path):
    z1, z2 = make_zip({"a/vasprun.xml": b"1"}), make_zip({"b/OUTCAR": b"2"})
    rec = mc_record("big", {"p1.zip": len(z1), "p2.zip": len(z2), "t.tar.gz": 7,
                            "vasprun.xml": 11, "README.md": 1}, description="VASP")
    s, keep, _ = _triage_case(tmp_path, [rec], {("big", "p1.zip"): z1, ("big", "p2.zip"): z2})
    assert [u["recid"] for u in keep] == [unit_id("big", "p1.zip"), unit_id("big", "p2.zip"),
                                          unit_id("big", "t.tar.gz"), "big~loose"]
    assert [[f["key"] for f in u["files"]] for u in keep] == [
        ["p1.zip"], ["p2.zip"], ["t.tar.gz"], ["README.md", "vasprun.xml"]]
    assert all(u["provenance"]["record_id"] == "big" for u in keep)
    assert all(u["unit"]["record_id"] == "big" and u["unit"]["of"] == 4 for u in keep)
    assert s["kept_records"] == 1 and s["fetch_units"] == 4
    assert all(":" not in u["recid"] and "/" not in u["recid"] for u in keep)


def test_unit_ids_are_stable_whatever_else_survives_triage():
    # the id depends only on the archive's own key: dropping B (e.g. a peek that later proves it
    # empty) must NOT renumber C — a renumbered id would inherit B's terminal rejection
    abc = split_units({"recid": "R"}, [{"key": "A.zip"}, {"key": "B.zip"}, {"key": "C.zip"}])
    ac = split_units({"recid": "R"}, [{"key": "A.zip"}, {"key": "C.zip"}])
    ids_abc = {u["files"][0]["key"]: u["recid"] for u in abc}
    ids_ac = {u["files"][0]["key"]: u["recid"] for u in ac}
    assert ids_abc["A.zip"] == ids_ac["A.zip"] and ids_abc["C.zip"] == ids_ac["C.zip"]
    assert len(set(ids_abc.values())) == 3
    (single,) = split_units({"recid": "one"}, [{"key": "a b/c.zip"}])
    assert single["recid"] == unit_id("one", "a b/c.zip") and " " not in single["recid"]
    # a record with only loose inputs (no VASP output) forms no unit at all
    assert split_units({"recid": "x"}, [{"key": "POSCAR"}, {"key": "README.md"}]) == []


def test_triage_peek_cache_reused_on_rerun(tmp_path):
    vz = make_zip({"a/vasprun.xml": b"1"})
    rec = mc_record("r", {"a.zip": len(vz)}, description="VASP")
    s1, _, _ = _triage_case(tmp_path, [rec], {("r", "a.zip"): vz})
    s2, keep, _ = _triage_case(tmp_path, [rec], {("r", "a.zip"): vz})
    assert s1["peeked"] == 1 and s2["peeked"] == 0 and s2["peeks_cached"] == 1 and keep


def test_triage_nested_only_zip_non_mention_dropped_with_gap(tmp_path):
    nz = make_zip({"runs.tar.gz": b"t"})
    rec = mc_record("n", {"a.zip": len(nz)}, description="QE")
    _, keep, report = _triage_case(tmp_path, [rec], {("n", "a.zip"): nz})
    assert keep == [] and "nested archive" in report["records"][0]["gaps"][0]


# --------------------------------------------------------------------------- #
# shared-fetch hooks (backward compatibility)                                  #
# --------------------------------------------------------------------------- #

def test_record_provenance_zenodo_default_unchanged():
    rec = {"conceptrecid": "1", "doi": "d", "conceptdoi": "cd", "zenodo_url": "u", "title": "t",
           "creators": ["c"], "license": "cc-by-4.0", "resource_type": "dataset",
           "publication_date": "2020", "keywords": ["k"]}
    assert zfetch._record_provenance(rec, "42") == {
        "source": "zenodo", "record_id": "42", "conceptrecid": "1", "doi": "d",
        "conceptdoi": "cd", "url": "u", "title": "t", "creators": ["c"], "license": "cc-by-4.0",
        "resource_type": "dataset", "publication_date": "2020", "keywords": ["k"]}


def test_record_provenance_passthrough_keeps_source_record_id():
    prov = {"source": "materials_cloud", "record_id": "abc", "doi": "10.24435/x"}
    assert zfetch._record_provenance({"provenance": prov}, "abc~03") == prov
    assert zfetch._record_provenance({"provenance": {"source": "x"}}, "r")["record_id"] == "r"


def test_fetch_uses_session_factory_never_the_token_session(tmp_path, monkeypatch):
    monkeypatch.setenv("ZENODO_TOKEN", "zenodo-secret")

    def _boom(token):  # the Zenodo session must not be built when a factory is given
        raise AssertionError("Zenodo token session used")
    monkeypatch.setattr(zfetch, "_session", _boom)
    z = make_zip({"calc/vasprun.xml": b"<x/>"})
    cand = record_to_candidate(mc_record("r1", {"a.zip": len(z)}, checksums=md5s({"a.zip": z})))
    kp = _write_jsonl(tmp_path / "keep.jsonl", [cand])
    sess = FakeFileSession({content_url("r1", "a.zip"): z})
    for workers in (1, 2):
        out = tmp_path / f"f{workers}.jsonl"
        zfetch.fetch(kp, out_path=out, raw_dir=tmp_path / f"raw{workers}",
                     rejections_path=tmp_path / "rej.jsonl", max_bytes=None, workers=workers,
                     session_factory=lambda: sess)
        (fe,) = list(read_jsonl(out))
        assert fe["provenance"]["source"] == "materials_cloud" and fe["n_calc_units"] == 1
    assert all("Authorization" not in h for _, h in sess.requests)


def test_declared_archive_kind_extracts_aiida_and_unknown_kind_is_ignored(tmp_path):
    legacy = make_zip({"data.json": b"{}", "nodes/ab/cd/u1/path/vasprun.xml": b"<x/>",
                       "nodes/ab/cd/u1/path/DOSCAR": b"d", "nodes/ef/gh/u2/path/INCAR": b"i"})
    cand = record_to_candidate(mc_record("r", {"e.aiida": len(legacy)},
                                         checksums=md5s({"e.aiida": legacy})))
    sess = FakeFileSession({content_url("r", "e.aiida"): legacy})
    rej = zfetch.RejectionLogger(tmp_path / "rej.jsonl")
    bad = {**cand, "files": [{**cand["files"][0], "archive_kind": "bogus"}]}
    assert zfetch.fetch_record(bad, sess, tmp_path / "raw0", None, rej) is None  # name-sniffed: skip
    good = {**cand, "files": [{**cand["files"][0], "archive_kind": "zip"}]}
    fe = zfetch.fetch_record(good, sess, tmp_path / "raw", None, rej)
    rej.close()
    assert fe is not None and fe["n_calc_units"] == 1
    (unit,) = fe["calc_units"]
    assert unit["vasprun"].endswith("e.aiida/nodes/ab/cd/u1/path/vasprun.xml")
    assert fe["calc_availability"][0]["dos"] is True          # DOSCAR in the SAME node dir
    assert not (tmp_path / "raw" / "r" / "extracted" / "e.aiida" / "nodes" / "ef").exists()


# --------------------------------------------------------------------------- #
# paths + status                                                               #
# --------------------------------------------------------------------------- #

def test_mc_paths_precedence(monkeypatch, tmp_path):
    from zenodo_harvest import config
    monkeypatch.delenv("MC_HARVEST_DATA", raising=False)
    monkeypatch.setattr(config, "DATA_ROOT", Path("/rds/user/x/hpc-work/zenodo"))
    assert mc_cli.mc_paths()[0] == Path("/rds/user/x/hpc-work/materials_cloud")
    monkeypatch.setattr(config, "DATA_ROOT", Path("data"))
    assert mc_cli.mc_paths()[0] == Path("data/materials_cloud")
    monkeypatch.setenv("MC_HARVEST_DATA", str(tmp_path / "mc"))
    root, man, raw, ds = mc_cli.mc_paths()
    assert (root, man, raw, ds) == (tmp_path / "mc", tmp_path / "mc" / "manifests",
                                    tmp_path / "mc" / "raw", tmp_path / "mc" / "dataset")


def test_status_uses_mc_manifest_names(tmp_path, capsys):
    man = tmp_path / "manifests"
    _write_jsonl(man / "mc_candidates.jsonl", [{"recid": "a"}, {"recid": "b"}])
    _write_jsonl(man / "mc_keep.jsonl", [{"recid": "a~00"}, {"recid": "a~01"}])
    _write_jsonl(man / "mc_keep.pipeline_parts" / "mc_keep.part-000.fetched.jsonl",
                 [{"recid": "a~00", "n_calc_units": 3}])
    _write_jsonl(man / "mc_fetch_rejections.jsonl",
                 [{"stage": "fetch", "id": "a~01", "reason": "no_vasp_files_fetched"}])
    rc = mc_cli.main(["status", "--manifests-dir", str(man), "--raw-dir", str(tmp_path / "raw"),
                      "--dataset-dir", str(tmp_path / "ds"), "--json"])
    rep = json.loads(capsys.readouterr().out)
    assert rc == 0 and rep["discover"]["candidates"] == 2 and rep["triage"]["keep"] == 2
    assert rep["records"]["attempted"] == 2 and rep["errors"]["rejections"] == 1


# --------------------------------------------------------------------------- #
# full offline run: triage -> shared fetch -> shared parse -> verify           #
# --------------------------------------------------------------------------- #

def _fixture(name: str) -> bytes:
    ase = pytest.importorskip("ase")
    pytest.importorskip("pymatgen")
    p = Path(ase.__file__).parent / "test" / "testdata" / "vasp" / name
    if not p.is_file():
        pytest.skip("ASE VASP test fixtures not available")
    return p.read_bytes()


def _aiida_like_zip() -> bytes:
    """A legacy-AiiDA-export-shaped zip: two retrieved folders (vasprun+DOSCAR, OUTCAR) + a
    CalcJob inputs folder, laid out as nodes/<uuid-shards>/path/<file> like the real export."""
    return make_zip({
        "metadata.json": b'{"export_version": "0.10"}', "data.json": b"{}",
        "nodes/fb/16/c207-aaaa/path/vasprun.xml": _fixture("vasprun_dfpt.xml"),
        "nodes/fb/16/c207-aaaa/path/DOSCAR": b"dos",
        "nodes/59/8e/207e-bbbb/path/OUTCAR": _fixture("OUTCAR_example_1"),
        "nodes/11/22/calc-cccc/path/INCAR": b"ENCUT = 400\n",
        "nodes/11/22/calc-cccc/path/_aiidasubmit.sh": b"#!/bin/bash\n",
    })


def _run_end_to_end(tmp_path: Path, split: bool) -> list[dict]:
    from zenodo_harvest.dataset_ops import verify_dataset
    from zenodo_harvest.parse import parse
    aiida = _aiida_like_zip()
    plain = make_zip({"extra/vasprun.xml": _fixture("vasprun_dfpt.xml")})
    rec = mc_record("e2e-1", {"acwf_x_results_vasp.aiida": len(aiida), "more.zip": len(plain)},
                    description="QE data",      # NO VASP mention: kept on peek evidence alone
                    checksums=md5s({"acwf_x_results_vasp.aiida": aiida, "more.zip": plain}))
    cands = [record_to_candidate(rec)]
    man = tmp_path / "manifests"
    cpath = _write_jsonl(man / "cand.jsonl", cands)
    sess = FakeFileSession({content_url("e2e-1", "acwf_x_results_vasp.aiida"): aiida,
                            content_url("e2e-1", "more.zip"): plain})
    t = triage(cpath, man / "keep.jsonl", session=sess, interval=0.0, split=split)  # type: ignore[arg-type]
    assert t["kept_records"] == 1 and t["blind_spot_recovered"] == ["e2e-1"]
    f = zfetch.fetch(man / "keep.jsonl", out_path=man / "fetched.jsonl", raw_dir=tmp_path / "raw",
                     rejections_path=man / "rej.jsonl", max_bytes=None, workers=2,
                     session_factory=lambda: sess)
    assert f["calc_units"] == 3
    ds = tmp_path / "dataset"
    p = parse(man / "fetched.jsonl", dataset_dir=ds, raw_dir=tmp_path / "raw",
              rejections_path=ds / "rejections.jsonl")
    assert p["calcs_parsed"] == 3 and p["frames"] > 0
    v = verify_dataset(ds)
    assert v["ok"], v
    return list(read_jsonl(ds / "metadata.jsonl"))


def test_end_to_end_aiida_export_calc_ids_identical_split_or_not(tmp_path):
    metas_split = _run_end_to_end(tmp_path / "split", split=True)
    metas_whole = _run_end_to_end(tmp_path / "whole", split=False)
    ids = sorted(m["calc_id"] for m in metas_split)
    assert ids == sorted(m["calc_id"] for m in metas_whole)
    assert ids == [
        "materials_cloud:e2e-1:acwf_x_results_vasp.aiida/nodes/59/8e/207e-bbbb/path/OUTCAR",
        "materials_cloud:e2e-1:acwf_x_results_vasp.aiida/nodes/fb/16/c207-aaaa/path/vasprun.xml",
        "materials_cloud:e2e-1:more/extra/vasprun.xml"]
    by = {m["calc_id"].rsplit("/", 1)[-1] + m["calc_id"].split(":")[2][:4]: m for m in metas_split}
    m = next(x for x in metas_split if x["calc_id"].endswith("c207-aaaa/path/vasprun.xml"))
    assert m["provenance"]["source"] == "materials_cloud"
    assert m["provenance"]["record_id"] == "e2e-1"
    assert m["provenance"]["doi"] == "10.24435/materialscloud:2020.0001/v1"
    assert m["availability"]["dos"] is True
    assert m["parser"] == "pymatgen.Vasprun"
    assert by                                             # (keeps the mapping exercised)


# --------------------------------------------------------------------------- #
# in-run retries of transient fetch failures (fetching.py)                     #
# --------------------------------------------------------------------------- #

from materials_cloud_harvest.fetching import fetch_with_retries, pending_units  # noqa: E402


def _keep(tmp_path: Path, ids: list[str]) -> Path:
    return _write_jsonl(tmp_path / "keep.jsonl", [{"recid": i} for i in ids])


def test_pending_units_excludes_fetched_and_terminal(tmp_path):
    kp = _keep(tmp_path, ["a", "b~00", "b~01", "c"])
    out = _write_jsonl(tmp_path / "f.jsonl", [{"recid": "a"}])
    rej = _write_jsonl(tmp_path / "r.jsonl", [
        {"stage": "fetch", "id": "b~00", "reason": "no_vasp_files_fetched"},        # terminal
        {"stage": "fetch", "id": "b~01", "reason": "fetch_failed_transient"},       # transient
        {"stage": "fetch", "id": "c:x.zip", "reason": "no_vasp_files_fetched"}])    # per-file id
    assert pending_units(kp, out, rej) == {"b~01", "c"}


def test_fetch_with_retries_repeats_until_nothing_pending(tmp_path):
    kp = _keep(tmp_path, ["a", "b"])
    out, rej = tmp_path / "f.jsonl", tmp_path / "r.jsonl"
    calls: list[int] = []

    def fake_fetch() -> dict:
        calls.append(1)
        done = ["a"] if len(calls) == 1 else ["a", "b"]        # b lands on the 2nd pass
        _write_jsonl(out, [{"recid": r} for r in done])
        return {"stopped_disk_budget": False}
    s = fetch_with_retries(fake_fetch, kp, out, rej, retries=4, sleep=lambda _s: None)
    assert len(calls) == 2 and s["fetch_passes"] == 2 and s["pending_after_retries"] == []


def test_fetch_with_retries_bounded_and_reports_leftovers(tmp_path):
    kp = _keep(tmp_path, ["a"])
    s = fetch_with_retries(lambda: {"stopped_disk_budget": False}, kp, tmp_path / "f.jsonl",
                           tmp_path / "r.jsonl", retries=2, sleep=lambda _s: None)
    assert s["fetch_passes"] == 3 and s["pending_after_retries"] == ["a"]


def test_fetch_with_retries_hands_disk_budget_stops_back(tmp_path):
    kp = _keep(tmp_path, ["a"])
    calls: list[int] = []

    def fake_fetch() -> dict:
        calls.append(1)
        return {"stopped_disk_budget": True}
    s = fetch_with_retries(fake_fetch, kp, tmp_path / "f.jsonl", tmp_path / "r.jsonl",
                           retries=4, sleep=lambda _s: None)
    assert len(calls) == 1 and s["pending_after_retries"] == []


class _FlakyResp(FakeResp):
    def __init__(self, *a: Any, drop_after: int | None = None, **k: Any):
        super().__init__(*a, **k)
        self.drop_after = drop_after

    def iter_content(self, n: int = 1 << 20):
        import requests
        sent = 0
        for chunk in super().iter_content(n):
            if self.drop_after is not None and sent + len(chunk) > self.drop_after:
                yield chunk[: self.drop_after - sent]
                raise requests.exceptions.ChunkedEncodingError("IncompleteRead (simulated)")
            sent += len(chunk)
            yield chunk


class FlakyFileSession(FakeFileSession):
    """Drops the FIRST full-file transfer part way (like the flaky WSL->CSCS link did live)."""

    def __init__(self, blobs: dict[str, bytes], drop_after: int):
        super().__init__(blobs)
        self.drop_after: int | None = drop_after

    def get(self, url: str, headers: dict | None = None, **k: Any) -> FakeResp:
        r = super().get(url, headers=headers, **k)
        if r.status_code in (200, 206) and self.drop_after is not None and \
                len(r.content) > self.drop_after and not (headers or {}).get("Range", "").startswith("bytes=-"):
            d, self.drop_after = self.drop_after, None
            return _FlakyResp(r.status_code, r.content, r.headers, drop_after=d)
        return r


def test_transient_drop_is_resumed_in_run_over_range(tmp_path):
    big = make_zip({f"run{i}/vasprun.xml": bytes(range(256)) * 4000 for i in range(3)},
                   method=zipfile.ZIP_STORED)                       # ~3 MB, > one 1 MiB chunk
    cand = record_to_candidate(mc_record("fl", {"a.zip": len(big)}, checksums=md5s({"a.zip": big})))
    kp = _write_jsonl(tmp_path / "keep.jsonl", [cand])
    out, rej = tmp_path / "f.jsonl", tmp_path / "rej.jsonl"
    sess = FlakyFileSession({content_url("fl", "a.zip"): big}, drop_after=1_500_000)
    s = fetch_with_retries(
        lambda: zfetch.fetch(kp, out_path=out, raw_dir=tmp_path / "raw", rejections_path=rej,
                             max_bytes=None, workers=1, zip_stream=False,
                             session_factory=lambda: sess),
        kp, out, rej, retries=3, sleep=lambda _s: None)
    (fe,) = list(read_jsonl(out))
    assert fe["n_calc_units"] == 3 and s["fetch_passes"] == 2 and s["pending_after_retries"] == []
    ranges = [h.get("Range") for u, h in sess.requests if u.endswith("/a.zip/content")]
    assert any(r and r.startswith("bytes=") and not r.startswith("bytes=0-") and
               not r.startswith("bytes=-") for r in ranges)          # 2nd pass resumed mid-file
    reasons = [r["reason"] for r in read_jsonl(rej)]
    assert "fetch_failed_transient" in reasons                         # 1st pass: transient, kept



# --------------------------------------------------------------------------- #
# regressions from the independent review (2026-09-23)                         #
# --------------------------------------------------------------------------- #

def test_evidence_uses_fetchs_own_unit_seeding_names(tmp_path):
    # OUTCAR1 / vasprun1.xml are primaries for fetch (_unit_role) — triage must agree, else it
    # prunes or drops an archive fetch would have turned into calc units
    from nomad_harvest.upload_zip import ZipMember
    ev = zip_evidence([ZipMember(n, 8, 1, 10, 0, 0) for n in (
        "calc/INCAR", "calc/POSCAR", "calc/OUTCAR1", "calc/vasprun1.xml", "calc/vaspout1.h5")])
    assert sorted(ev.primary) == ["calc/OUTCAR1", "calc/vaspout1.h5", "calc/vasprun1.xml"]
    z = make_zip({"calc/INCAR": b"i", "calc/POSCAR": b"p", "calc/OUTCAR1": b"o",
                  "calc/vasprun1.xml": b"<x/>"})
    recs = [mc_record("m", {"a.zip": len(z)}, description="VASP"),
            mc_record("q", {"a.zip": len(z)}, description="QE")]
    s, keep, _ = _triage_case(tmp_path, recs, {("m", "a.zip"): z, ("q", "a.zip"): z})
    assert {u["unit"]["record_id"] for u in keep} == {"m", "q"}
    assert s["blind_spot_recovered"] == ["q"]
    # and fetch really makes a calc unit out of it
    cand = record_to_candidate(mc_record("m", {"a.zip": len(z)}, checksums=md5s({"a.zip": z})))
    rej = zfetch.RejectionLogger(tmp_path / "rej.jsonl")
    fe = zfetch.fetch_record(cand, FakeFileSession({content_url("m", "a.zip"): z}),
                             tmp_path / "raw", None, rej)
    rej.close()
    assert fe is not None and fe["n_calc_units"] == 1


def test_legacy_aiida_with_only_a_nested_archive_is_kept_for_mention_records(tmp_path):
    x = make_zip({"data.json": b"{}", "nodes/ab/cd/u1/path/retrieved.tar.gz": b"t"})
    rec = mc_record("m", {"x.aiida": len(x)}, description="VASP")
    _, keep, report = _triage_case(tmp_path, [rec], {("m", "x.aiida"): x})
    (u,) = keep
    assert u["triage_reason"] == "vasp_mention" and u["files"][0]["archive_kind"] == "zip"
    assert "nested archive" in report["records"][0]["gaps"][0]


def test_loose_inputs_are_not_positive_evidence(tmp_path):
    rec = mc_record("q", {"POSCAR": 10, "kpoints.dat": 5, "incarnation_notes.txt": 5,
                          "big.tar.gz": 10**11}, description="Quantum ESPRESSO")
    s, keep, _ = _triage_case(tmp_path, [rec], {})
    assert keep == [] and s["decisions"] == {"no_vasp_evidence": 1}
    assert s["unpeekable_skipped"] == {"files": 1, "bytes": 10**11}
    rec2 = mc_record("q2", {"OUTCAR": 10, "big.tar.gz": 10**11}, description="QE")
    s2, keep2, _ = _triage_case(tmp_path / "b", [rec2], {})
    assert s2["decisions"] == {"vasp_evidence": 1}                 # a loose OUTCAR IS evidence


def test_central_directory_tolerates_prepended_data_and_eocd_in_comment():
    z = make_zip({"run/OUTCAR": b"o" * 100, "run/INCAR": b"i"})
    pre = b"#!/bin/sh self-extractor stub\n" + b"x" * 4000 + z
    members, _ = read_central_directory(FakeFileSession({"u": pre}), "u")   # type: ignore[arg-type]
    assert {m.name for m in members} == {"run/OUTCAR", "run/INCAR"}
    # the corrected offsets really point at the local headers
    assert all(pre[m.local_offset:m.local_offset + 4] == b"PK\x03\x04" for m in members)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a/vasprun.xml", b"<x/>")
        zf.comment = b"note: PK\x05\x06 appears in this comment"
    members2, _ = read_central_directory(FakeFileSession({"c": buf.getvalue()}), "c")  # type: ignore[arg-type]
    assert [m.name for m in members2] == ["a/vasprun.xml"]


def test_inconsistent_central_directory_is_a_failed_peek_not_empty(tmp_path):
    z = bytearray(make_zip({"a/OUTCAR": b"o", "b/OUTCAR": b"o"}))
    i = z.rfind(b"PK\x05\x06")
    z[i + 10:i + 12] = struct.pack("<H", 5)                    # claims 5 entries, has 2
    ev, st = peek_archive(FakeFileSession({"u": bytes(z)}), "u")   # type: ignore[arg-type]
    assert ev is None and st.startswith("peek_failed")


def test_failed_and_cap_dependent_peeks_are_not_cached(tmp_path):
    z = make_zip({"a/vasprun.xml": b"1"})
    rec = mc_record("r", {"a.zip": len(z)}, description="VASP")
    s1, _, _ = _triage_case(tmp_path, [rec], {("r", "a.zip"): z}, max_cd_bytes=5)
    assert s1["peeked"] == 1                                   # cd_too_large at a 5 B cap
    s2, keep, _ = _triage_case(tmp_path, [rec], {("r", "a.zip"): z})
    assert s2["peeked"] == 1 and s2["peeks_cached"] == 0       # re-peeked under the default cap
    s3, _, _ = _triage_case(tmp_path, [rec], {("r", "a.zip"): z})
    assert s3["peeks_cached"] == 1                             # the 'ok' verdict IS cached


def test_aiida_export_with_top_level_folder_and_plain_zip_named_aiida(tmp_path):
    assert aiida_format(["export/data.json", "export/nodes/ab/cd/u/path/OUTCAR"]) == "legacy"
    assert aiida_format(["x/repo/" + "a" * 64, "x/db.sqlite3"]) == "sqlite_zip"
    plain = make_zip({"calc/vasprun.xml": b"<x/>"})           # names visible, no AiiDA layout
    rec = mc_record("r", {"odd.aiida": len(plain)}, description="VASP")
    _, keep, _ = _triage_case(tmp_path, [rec], {("r", "odd.aiida"): plain})
    (u,) = keep
    assert u["files"][0]["archive_kind"] == "zip"


def test_failed_zip_peek_is_a_reported_gap_and_nested_aiida_too(tmp_path):
    inner = make_zip({"export.aiida": b"z"})
    recs = [mc_record("q", {"broken.zip": 123}, description="QE"),      # 404 -> peek fails
            mc_record("n", {"wrap.zip": len(inner)}, description="QE")]
    s, keep, report = _triage_case(tmp_path, recs, {("n", "wrap.zip"): inner})
    assert keep == []
    by = {r["recid"]: r for r in report["records"]}
    assert by["q"]["reason"] == "evidence_gap_only" and "zip peek_failed" in by["q"]["gaps"][0]
    assert "AiiDA export" in by["n"]["gaps"][0]


def test_budget_exceeding_units_are_not_pending_and_are_reported(tmp_path):
    from materials_cloud_harvest.fetching import units_needing_bigger_budget
    kp = _keep(tmp_path, ["a", "b", "c"])
    out = _write_jsonl(tmp_path / "f.jsonl", [{"recid": "c"}])
    rej = _write_jsonl(tmp_path / "r.jsonl", [
        {"stage": "fetch", "id": "a", "reason": "record_exceeds_disk_budget", "transient": True},
        {"stage": "fetch", "id": "c", "reason": "record_exceeds_disk_budget"},   # staged truncated
        {"stage": "fetch", "id": "b", "reason": "fetch_failed_transient"}])
    assert pending_units(kp, out, rej) == {"b"}
    assert units_needing_bigger_budget(kp, out, rej) == {"a", "c"}
    s = fetch_with_retries(lambda: {"stopped_disk_budget": False}, kp, out, rej, retries=1,
                           sleep=lambda _s: None)
    assert s["pending_after_retries"] == ["b"] and s["units_exceeding_disk_budget"] == ["a", "c"]


def test_fetch_member_returns_false_when_the_header_read_fails(tmp_path):
    from nomad_harvest.upload_zip import ZipMember
    m = ZipMember("db.sqlite3", 8, 10, 20, 0, 0)
    assert fetch_member(FakeFileSession({}), "missing", m, tmp_path / "db") is False  # type: ignore[arg-type]


def test_zenodo_token_is_only_attached_to_zenodo_hosts(monkeypatch):
    import requests
    s = zfetch._session("secret")
    for url, expect in [("https://zenodo.org/api/records/1/files/a/content", True),
                        ("https://sandbox.zenodo.org/x", True),
                        ("https://archive.materialscloud.org/api/records/r/files/a/content", False),
                        ("https://evilzenodo.org/x", False)]:
        prep = s.prepare_request(requests.Request("GET", url))
        assert ("Authorization" in prep.headers) is expect, url
        if expect:
            assert prep.headers["Authorization"] == "Bearer secret"
    assert "Authorization" not in zfetch._session(None).prepare_request(
        requests.Request("GET", "https://zenodo.org/x")).headers


def test_status_counts_split_units_by_record(tmp_path, capsys):
    man, ds = tmp_path / "manifests", tmp_path / "ds"
    _write_jsonl(man / "mc_candidates.jsonl", [{"recid": "R"}])
    _write_jsonl(man / "mc_keep.jsonl", [{"recid": "R~a", "record_id": "R"},
                                         {"recid": "R~b", "record_id": "R"}])
    _write_jsonl(man / "mc_keep.pipeline_parts" / "mc_keep.part-000.fetched.jsonl", [
        {"recid": "R~a", "n_calc_units": 2, "provenance": {"record_id": "R"}},
        {"recid": "R~b", "n_calc_units": 3, "provenance": {"record_id": "R"}}])
    _write_jsonl(ds / "metadata.jsonl", [
        {"calc_id": f"materials_cloud:R:x/{i}/OUTCAR", "provenance": {"record_id": "R"},
         "quality": {"n_frames": 1, "n_frames_with_forces": 1}} for i in range(5)])
    mc_cli.main(["status", "--manifests-dir", str(man), "--raw-dir", str(tmp_path / "raw"),
                 "--dataset-dir", str(ds), "--json"])
    rep = json.loads(capsys.readouterr().out)
    assert rep["parse"]["pct"] == 100.0 and rep["parse"]["this_run_parsed"] == 5
    assert rep["fetch"]["calc_units"] == 5 and rep["records"]["with_frames"] == 1
    # after the parts dir is cleared, parsed units are still "done", not "untouched"
    import shutil
    shutil.rmtree(man / "mc_keep.pipeline_parts")
    mc_cli.main(["status", "--manifests-dir", str(man), "--raw-dir", str(tmp_path / "raw"),
                 "--dataset-dir", str(ds), "--json"])
    rep = json.loads(capsys.readouterr().out)
    assert rep["records"]["untouched"] == 0 and rep["records"]["fetched"] == 2
    # but while a split record is mid-fetch, a not-yet-fetched sibling unit is NOT reported done
    _write_jsonl(man / "mc_keep.pipeline_parts" / "mc_keep.part-000.fetched.jsonl", [
        {"recid": "R~a", "n_calc_units": 2, "provenance": {"record_id": "R"}}])
    mc_cli.main(["status", "--manifests-dir", str(man), "--raw-dir", str(tmp_path / "raw"),
                 "--dataset-dir", str(ds), "--json"])
    rep = json.loads(capsys.readouterr().out)
    assert rep["records"]["fetched"] == 1 and rep["records"]["untouched"] == 1


# --------------------------------------------------------------------------- #
# parallel triage peeks + CSD3 bench helpers                                   #
# --------------------------------------------------------------------------- #

def test_parallel_peeks_give_identical_keep_lists(tmp_path):
    vz = make_zip({"r/vasprun.xml": b"<x/>"})
    empty = make_zip({"r/POSCAR": b"p"})
    recs, blobs = [], {}
    for i in range(12):
        body = vz if i % 3 else empty
        recs.append(mc_record(f"r{i:02d}", {"a.zip": len(body), "b.zip": len(empty)},
                              description="VASP" if i % 2 else "QE"))
        blobs[(f"r{i:02d}", "a.zip")] = body
        blobs[(f"r{i:02d}", "b.zip")] = empty
    outs = []
    for workers in (1, 4):
        d = tmp_path / f"w{workers}"
        _, keep, _ = _triage_case(d, recs, blobs, peek_workers=workers)
        # everything except the candidate-build timestamp must be identical
        outs.append([{k: v for k, v in u.items() if k != "retrieved_at"} for u in keep])
    assert outs[0] == outs[1] and outs[0]


def test_pacer_spaces_request_starts():
    import time as _t
    from materials_cloud_harvest.triage import _Pacer
    p = _Pacer(0.05)
    t0 = _t.monotonic()
    for _ in range(4):
        p.wait()
    assert _t.monotonic() - t0 >= 0.14


def _bench_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "csd3_mc_bench", Path(__file__).resolve().parents[1] / "scripts" / "csd3" /
        "materials_cloud" / "csd3_mc_bench.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_bench_sample_selection_is_stratified_and_budgeted():
    b = _bench_module()
    units = [
        {"recid": "big~a", "bytes_total": 4_400_000_000, "files": [{"key": "run1.tar.gz"}]},
        {"recid": "huge~b", "bytes_total": 9_000_000_000, "files": [{"key": "all.tar.gz"}]},
        {"recid": "yf0rj-w3r97~u", "bytes_total": 1_270_000_000, "unit": {"record_id": b.BOSONI},
         "files": [{"key": "x_results_vasp.aiida"}]},
        {"recid": "yf0rj-w3r97~o", "bytes_total": 1_850_000_000, "unit": {"record_id": b.BOSONI},
         "files": [{"key": "y_results_vasp.aiida"}]},
        *[{"recid": f"s{i}", "bytes_total": 300_000_000, "files": [{"key": "d.zip"}]}
          for i in range(30)],
    ]
    pick = b.select_sample(units, budget_bytes=12_000_000_000, seed=1)
    ids = [u["recid"] for u in pick]
    assert ids[0] == "big~a"                      # largest tar unit within half the budget
    assert ids[1] == "yf0rj-w3r97~u"              # the smaller Bosoni export
    assert "huge~b" not in ids and "yf0rj-w3r97~o" not in ids[:2]
    assert sum(u["bytes_total"] for u in pick) <= 12_000_000_000
    assert pick == b.select_sample(units, budget_bytes=12_000_000_000, seed=1)   # deterministic


def test_bench_projection_scales_and_names_the_bottleneck():
    b = _bench_module()
    p = b.project(total_bytes=90e9, sample_bytes=9e9, fetch_wall_s=600, n_small=1000,
                  small_serial_s=0.6, small_parallel_s=0.2, big_gb=10, big_s_per_gb=120)
    assert p["scale_factor"] == 10.0 and p["fetch_hours"] == round(600 * 10 / 3600, 2)
    assert p["parse_hours_serial"] == round((1000 * 0.6 + 1200) * 10 / 3600, 2)
    assert p["parse_hours_with_workers"] == round((1000 * 0.2 + 1200) * 10 / 3600, 2)
    assert p["likely_bottleneck"] == "parse"
