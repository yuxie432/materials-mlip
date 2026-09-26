"""Offline tests for the Zenodo census (zenodo_census/): census enumeration + resume + dedup,
the offline signals and tier rule (incl. the real metadata of the keyword harvest's known misses),
exclusions/seeds/scoring, the link channels (DataCite / Europe PMC / OpenAlex parsing), the tar
head-peek over real compressed streams, triage verdicts/decisions, and one full offline run:
census -> score -> triage -> SHARED fetch -> SHARED parse -> verify, served over fake HTTP Range.
"""

from __future__ import annotations

import bz2
import gzip
import hashlib
import io
import json
import lzma
import os
import struct
import tarfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from zenodo_census import census as zc
from zenodo_census import headpeek as hp
from zenodo_census import links as zl
from zenodo_census import score as zs
from zenodo_census import signals as sg
from zenodo_census import triage as zt
from zenodo_harvest.manifest import read_jsonl
from zenodo_harvest.models import Candidate

ZBASE = "https://zenodo.org/api/records"


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #

def url(rid: str, key: str) -> str:
    return f"{ZBASE}/{rid}/files/{key}/content"


def zhit(rid: str, files: dict[str, bytes | int] | None = None, *, title: str = "A dataset",
         description: str = "", keywords: list[str] | None = None,
         creators: list[dict] | None = None, owners: list[str] | None = None,
         communities: list[str] | None = None, related: list[dict] | None = None,
         license: str | None = "cc-by-4.0", access: str = "open", rtype: str = "dataset",
         concept: str | None = None, created: str = "2025-01-01T00:00:00+00:00") -> dict:
    """A Zenodo /api/records search hit (legacy serializer shape)."""
    files = files if files is not None else {"data.zip": 100}
    fl = []
    for k, v in files.items():
        data = v if isinstance(v, bytes) else None
        size = len(v) if isinstance(v, bytes) else int(v)
        fl.append({"id": "x", "key": k, "size": size,
                   "checksum": "md5:" + (hashlib.md5(data).hexdigest() if data else "0" * 32),
                   "links": {"self": url(rid, k)}})
    return {
        "id": int(rid), "recid": rid, "conceptrecid": concept or str(int(rid) - 1),
        "doi": f"10.5281/zenodo.{rid}", "conceptdoi": f"10.5281/zenodo.{concept or int(rid) - 1}",
        "created": created, "owners": [{"id": o} for o in (owners or ["1"])],
        "links": {"self_html": f"https://zenodo.org/records/{rid}"},
        "stats": {"downloads": 3}, "swh": {},
        "metadata": {
            "title": title, "description": description, "keywords": keywords or [],
            "resource_type": {"title": rtype.title(), "type": rtype},
            "license": {"id": license} if license else None, "access_right": access,
            "publication_date": "2025-01-01",
            "creators": creators or [{"name": "Doe, Jane", "affiliation": "Uni"}],
            "communities": [{"id": c} for c in (communities or [])],
            "related_identifiers": related or [],
        },
        "files": fl,
    }


class FakeResp:
    def __init__(self, status: int, body: bytes = b"", headers: dict | None = None,
                 json_obj: Any = None, text: str | None = None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self._json = json_obj
        self.text = text if text is not None else body.decode("utf-8", "replace")

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


class FakeRangeSession:
    """Serves ``{url: bytes}`` with HTTP Range semantics; records every request."""

    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs
        self.headers: dict[str, str] = {}
        self.requests: list[tuple[str, dict]] = []

    def get(self, u: str, headers: dict | None = None, **_: Any) -> FakeResp:
        hdrs = {**self.headers, **(headers or {})}
        self.requests.append((u, hdrs))
        body = self.blobs.get(u)
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
        return FakeResp(206, body[start:end + 1], {"Content-Range": f"bytes {start}-{end}/{n}",
                                                   "Content-Length": str(end - start + 1)})

    def close(self) -> None:
        pass

    def __enter__(self) -> "FakeRangeSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


def make_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def make_zip64(members: dict[str, bytes], monkeypatch: pytest.MonkeyPatch) -> bytes:
    """A real ZIP64 archive whose classic EOCD carries 0xFFFFFFFF sentinels."""
    monkeypatch.setattr(zipfile, "ZIP_FILECOUNT_LIMIT", 1)
    data = bytearray(make_zip(members))
    i = data.rfind(b"PK\x05\x06")
    data[i + 12:i + 20] = struct.pack("<II", 0xFFFFFFFF, 0xFFFFFFFF)
    return bytes(data)


def make_tar(members: dict[str, bytes], fmt: int = tarfile.USTAR_FORMAT) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=fmt) as tf:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def compress(data: bytes, kind: str) -> bytes:
    if kind == "gz":
        return gzip.compress(data)
    if kind == "bz2":
        return bz2.compress(data)
    if kind == "xz":
        return lzma.compress(data)
    if kind == "zst":
        zstandard = pytest.importorskip("zstandard")
        return zstandard.ZstdCompressor().compress(data)
    return data


def _write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return path


# --------------------------------------------------------------------------- #
# census                                                                       #
# --------------------------------------------------------------------------- #

def test_slim_hit_keeps_everything_candidate_reads():
    full = zhit("101", {"calcs.tar.gz": 5000, "readme.txt": 10}, title="T", description="<p>d</p>",
                keywords=["k"], related=[{"identifier": "10.1/x", "relation": "isSupplementTo",
                                          "scheme": "doi"}], communities=["wmd-group"])
    full["metadata"]["creators"] = [{"name": "Kavanagh, Seán R.", "orcid": "0000-0003-4577-9647",
                                     "affiliation": "Harvard"}]
    slim = zc.slim_hit(full)
    a, b = Candidate.from_record(full).to_dict(), Candidate.from_record(slim).to_dict()
    a.pop("retrieved_at"), b.pop("retrieved_at")
    assert a == b
    assert slim["owners"] == [{"id": "1"}] and "stats" not in slim and "swh" not in slim
    assert slim["metadata"]["creators"][0]["orcid"] == "0000-0003-4577-9647"
    assert slim["metadata"]["communities"] == [{"id": "wmd-group"}]
    assert slim["metadata"]["related_identifiers"][0]["identifier"] == "10.1/x"


def test_census_query_covers_archives_and_loose_vasp():
    assert "files.entries.ext:(zip OR gz" in zc.CENSUS_QUERY and "aiida" in zc.CENSUS_QUERY
    assert "files.entries.key:(*OUTCAR*" in zc.CENSUS_QUERY and "*vasprun*" in zc.CENSUS_QUERY


class FakeCensusClient:
    token = "t"

    def __init__(self, windows: list[tuple[datetime, datetime, list[dict]]]):
        self.windows = windows
        self.paged: list[datetime] = []
        self.sizes: list[int] = []

    def iter_records(self, query: str, start: Any = None, end: Any = None, size: int = 25,
                     extra: Any = None, should_skip: Any = None, on_window_done: Any = None):
        self.sizes.append(size)
        for s, e, hits in self.windows:
            if should_skip and should_skip(s, e):
                continue
            self.paged.append(s)
            yield from hits
            if on_window_done:
                on_window_done(s, e)


def _windows() -> list[tuple[datetime, datetime, list[dict]]]:
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [(t, t + timedelta(days=1), [zhit("11", concept="10"), zhit("21", concept="20")]),
            (t + timedelta(days=1), t + timedelta(days=2),
             [zhit("31", concept="30"), zhit("12", concept="10")])]   # 12 = newer version of 11


def test_run_census_resumes_at_window_granularity_and_dedups(tmp_path):
    out = tmp_path / "census.jsonl"
    c1 = FakeCensusClient(_windows())
    s1 = zc.run_census(c1, out, max_records=3)        # stops inside window 2
    assert s1["written"] == 3 and s1["windows_done"] == 1 and c1.sizes == [100]
    c2 = FakeCensusClient(_windows())
    s2 = zc.run_census(c2, out)
    assert s2["windows_skipped"] == 1 and s2["windows_done"] == 1 and s2["written"] == 2
    assert len(c2.paged) == 1                          # window 1 was not re-paged
    recs = list(zc.iter_census(out))
    assert sorted(r["id"] for r in recs) == [12, 21, 31]   # newest version per concept, once


def test_iter_census_skips_torn_lines_anywhere(tmp_path):
    out = tmp_path / "c.jsonl"
    good = [zc.slim_hit(zhit("5", concept="4")), zc.slim_hit(zhit("7", concept="6"))]
    out.write_text(json.dumps(good[0]) + "\n" + '{"id": 9, "conceptrecid": "8", "metad'
                   + "\n" + json.dumps(good[1]) + "\n")
    assert [r["id"] for r in zc.iter_census(out)] == [5, 7]


def test_torn_final_line_is_terminated_before_appending(tmp_path):
    out = tmp_path / "c.jsonl"
    out.write_text(json.dumps(zc.slim_hit(zhit("5"))) + "\n" + '{"id": 9, "conc')
    zc.run_census(FakeCensusClient([(datetime(2024, 1, 1, tzinfo=timezone.utc),
                                     datetime(2024, 1, 2, tzinfo=timezone.utc),
                                     [zhit("7", concept="6")])]), out)
    assert [r["id"] for r in zc.iter_census(out)] == [5, 7]


def test_census_client_paces_request_starts(monkeypatch):
    clock = {"t": 100.0}
    sleeps: list[float] = []
    monkeypatch.setattr(zc.time, "monotonic", lambda: clock["t"])

    def fake_sleep(d: float) -> None:
        sleeps.append(d)
        clock["t"] += d

    monkeypatch.setattr(zc.time, "sleep", fake_sleep)
    c = zc.CensusClient(token="x", min_interval=2.0)
    c._throttle()
    clock["t"] += 1.5            # the request took 1.5 s
    c._throttle()
    assert sleeps == [pytest.approx(0.5)]   # waits 2.0 s from the previous START, not the end


# --- records Zenodo's default serializer breaks on (the first CSD3 census died on one) ----------

def test_legacy_from_native_reproduces_the_default_serializer():
    """Real hits of four records in both serializations: the conversion is exact for every field
    the census keeps (notes, journal, keywords, relations, community slug, licence renames,
    multiple rights, no licence, several affiliations, a publication subtype)."""
    doc = json.loads((Path(__file__).parent / "zenodo_serializer_pairs.json").read_text())
    for legacy, native in doc["pairs"]:
        conv = zc.legacy_from_native(native)
        assert conv["_serializer"] == "inveniordm"
        got, want = zc.slim_hit(conv), zc.slim_hit(legacy)
        assert got.pop("_serializer") == "inveniordm"
        assert got == want, legacy["id"]
        a, b = Candidate.from_record(got).to_dict(), Candidate.from_record(want).to_dict()
        a.pop("retrieved_at")
        b.pop("retrieved_at")
        assert a == b


def to_native(h: dict) -> dict:
    """A legacy-shaped test hit (``zhit``) in the native serializer's shape."""
    m = h["metadata"]
    rt = m["resource_type"]
    return {
        "id": str(h["id"]), "created": h["created"],
        "pids": {"doi": {"identifier": h["doi"]}},
        "parent": {"id": h["conceptrecid"],
                   "access": {"owned_by": {"user": h["owners"][0]["id"]}},
                   "communities": {"entries": [{"id": "u" + c["id"], "slug": c["id"]}
                                               for c in m["communities"]]},
                   "pids": {"doi": {"identifier": h["conceptdoi"]}}},
        "access": {"record": "public", "files": "public", "embargo": {"active": False},
                   "status": "open"},
        "links": {"self_html": h["links"]["self_html"]},
        "metadata": {
            "title": m["title"], "description": m["description"],
            "resource_type": {"id": rt["type"], "title": {"en": rt["title"]}},
            "rights": [{"id": m["license"]["id"]}] if m["license"] else [],
            "publication_date": m["publication_date"],
            "subjects": [{"subject": k} for k in m["keywords"]],
            "creators": [{"person_or_org": {"name": c["name"]},
                          "affiliations": [{"name": c["affiliation"]}]} for c in m["creators"]],
            "related_identifiers": [{"identifier": r["identifier"], "scheme": r["scheme"],
                                     "relation_type": {"id": r["relation"].lower()}}
                                    for r in m["related_identifiers"]]},
        "files": {"entries": {f["key"]: {k: f[k] for k in ("id", "key", "size", "checksum")}
                              for f in h["files"]}},
    }


class SearchResp(FakeResp):
    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class FakeSearchServer:
    """``/api/records`` over one newest-first record list. The default serializer fails (500) on
    any page holding a ``poison`` id; both serializers fail on a ``dead`` id; ``down(n)`` makes
    the next n requests fail with 503 (an outage); ``bad_request`` answers everything with 400."""

    def __init__(self, records: list[dict], poison: tuple = (), dead: tuple = ()):
        self.records, self.poison, self.dead = records, set(poison), set(dead)
        self.headers: dict[str, str] = {}
        self.outage = 0
        self.bad_request = False
        self.log: list[tuple[int, int, bool, int]] = []      # (page, size, native, status)

    def down(self, n: int) -> None:
        self.outage = n

    def get(self, u: str, params: dict | None = None, headers: dict | None = None,
            **_: Any) -> SearchResp:
        p = params or {}
        page, size = int(p.get("page", 1)), int(p["size"])
        native = (headers or {}).get("Accept") == zc.NATIVE_ACCEPT
        records = self.records
        if str(p.get("q", "")).startswith("recid:("):             # resolve_versions batches
            wanted = set(str(p["q"])[len("recid:("):-1].split(" OR "))
            records = [r for r in records if str(r["id"]) in wanted]
        chunk = records[(page - 1) * size: page * size]
        ids = {str(r["id"]) for r in chunk}
        if self.bad_request or page * size > 10_000:
            status = 400
        elif self.outage > 0:
            self.outage -= 1
            status = 503
        elif ids & self.dead or (not native and ids & self.poison):
            status = 500
        else:
            status = 200
        self.log.append((page, size, native, status))
        if status != 200:
            return SearchResp(status, json_obj={"status": status})
        hits = [to_native(r) if native else r for r in chunk]
        return SearchResp(200, json_obj={"hits": {"hits": hits, "total": len(records)}})


def _recs(n: int) -> list[dict]:
    return [zhit(str(1000 + i), {f"d{i}.zip": 10 + i}, title=f"record {i}", keywords=[f"k{i}"],
                 owners=[str(i % 3)], communities=["c1"] if i % 4 == 0 else [],
                 related=[{"identifier": f"10.1000/p{i}", "relation": "isSupplementTo",
                           "scheme": "doi"}] if i % 5 == 0 else [],
                 concept=str(900 + i))
            for i in range(n)]


def _client(server: FakeSearchServer, monkeypatch: pytest.MonkeyPatch,
            events: list | None = None) -> zc.CensusClient:
    monkeypatch.setattr(zc.time, "sleep", lambda s: None)
    c = zc.CensusClient(token="t", min_interval=0.0, session=server)
    c.outage_delay, c.outage_patience = 1.0, 8.0
    if events is not None:
        c.on_poison = events.append
    return c


def test_census_client_isolates_records_the_default_serializer_breaks_on(monkeypatch):
    recs = _recs(250)
    server = FakeSearchServer(recs, poison=("1137", "1138", "1205"))    # two adjacent + one
    events: list[dict] = []
    got = list(_client(server, monkeypatch, events).iter_window("q", size=100))
    assert [r["id"] for r in got] == [r["id"] for r in recs]            # all, in order
    for r, want in zip(got, recs):
        slim = zc.slim_hit(r)
        slim.pop("_serializer", None)
        assert slim == zc.slim_hit(want)                                # converted ones exact
    assert {r["id"] for r in got if r.get("_serializer")} == {1137, 1138, 1205}
    assert [(e["status"], e["offset"], e["id"]) for e in events] == [
        ("converted", 137, 1137), ("converted", 138, 1138), ("converted", 205, 1205)]
    assert all(e["native"]["id"] == str(e["id"]) for e in events)
    # bounded: pages 2 and 3 are tried once (with the usual retries), then split 100 -> 10 -> 1
    assert len([x for x in server.log if x[1] == 100]) == 1 + 6 + 6
    assert len(server.log) < 70


def test_census_client_counts_a_window_whose_newest_record_is_poison(monkeypatch):
    recs = _recs(30)
    server = FakeSearchServer(recs, poison=("1000",))
    events: list[dict] = []
    c = _client(server, monkeypatch, events)
    assert c.count("q") == 30                     # shared count (size=1, page 1) fails -> probe
    assert (10_000, 1, True, 200) in server.log   # the probe serializes no hit
    got = list(c.iter_window("q", size=10))
    assert [r["id"] for r in got] == [r["id"] for r in recs]
    assert [e["offset"] for e in events] == [0]


def test_census_client_waits_out_an_outage_instead_of_skipping(monkeypatch):
    recs = _recs(40)
    server = FakeSearchServer(recs)
    events: list[dict] = []
    c = _client(server, monkeypatch, events)
    it = c.iter_window("q", size=10)
    first = [next(it) for _ in range(10)]
    server.down(9)            # page 2: 6 failed attempts, then 3 failed probes, then back
    got = first + list(it)
    assert [r["id"] for r in got] == [r["id"] for r in recs] and events == []
    assert not any(r.get("_serializer") for r in got)


def test_census_client_stops_on_a_long_outage_and_logs_nothing(monkeypatch):
    server = FakeSearchServer(_recs(40))
    events: list[dict] = []
    c = _client(server, monkeypatch, events)
    it = c.iter_window("q", size=10)
    [next(it) for _ in range(10)]
    server.down(10_000)
    with pytest.raises(zc.ZenodoOutage):
        list(it)
    assert events == []                       # an outage is never recorded as broken records


def test_census_client_restarts_a_page_when_an_outage_begins_mid_isolation(monkeypatch):
    recs = _recs(30)
    server = FakeSearchServer(recs, poison=("1015",))
    events: list[dict] = []
    c = _client(server, monkeypatch, events)
    real_single = zc.CensusClient._single
    calls = {"n": 0}

    def flaky_single(self, query, offset, sort, extra):
        calls["n"] += 1
        if calls["n"] == 3:                   # Zenodo goes down for a while mid-isolation
            server.down(12)
        return real_single(self, query, offset, sort, extra)

    monkeypatch.setattr(zc.CensusClient, "_single", flaky_single)
    got = list(c.iter_window("q", size=10))
    assert [r["id"] for r in got] == [r["id"] for r in recs]
    assert [(e["status"], e["id"]) for e in events] == [("converted", 1015)]


def test_census_client_reports_a_record_neither_serializer_returns(monkeypatch):
    recs = _recs(25)
    server = FakeSearchServer(recs, dead=("1012",))
    events: list[dict] = []
    got = list(_client(server, monkeypatch, events).iter_window("q", size=10))
    assert [r["id"] for r in got] == [r["id"] for r in recs if r["id"] != 1012]
    assert [(e["status"], e["offset"], e["query"]) for e in events] == [("unresolved", 12, "q")]


def test_census_client_does_not_isolate_client_errors(monkeypatch):
    import requests
    server = FakeSearchServer(_recs(5))
    server.bad_request = True
    with pytest.raises(requests.HTTPError):
        list(_client(server, monkeypatch).iter_window("q", size=5))
    assert len(server.log) == 1               # a 400 is our bug: no retries, no isolation


def test_resolve_versions_survives_a_batch_the_default_serializer_breaks_on(tmp_path,
                                                                           monkeypatch):
    recs = _recs(8)
    server = FakeSearchServer(recs, poison=("1003",))
    out = tmp_path / "versions.jsonl"
    s = zl.resolve_versions(["1001", "1003", "1005", "77"], out, _client(server, monkeypatch))
    assert s["resolved"] == 3 and s["unknown"] == 1
    got = {r["id"]: r["conceptrecid"] for r in read_jsonl(out)}
    assert got == {"1001": "901", "1003": "903", "1005": "905", "77": None}
    assert any(native and status == 200 for _, _, native, status in server.log)


def test_run_census_logs_poison_records_and_resumes(tmp_path, monkeypatch):
    from datetime import date as _date
    recs = _recs(12)
    out = tmp_path / "census.jsonl"
    server = FakeSearchServer(recs, poison=("1007",))
    s = zc.run_census(_client(server, monkeypatch), out, start=_date(2024, 1, 1),
                      end=_date(2024, 1, 1), page_size=5)
    assert s["written"] == 12 and s["poison_converted"] == 1 and s["poison_unresolved"] == 0
    lines = [json.loads(x) for x in out.read_text().splitlines()]
    assert [r["id"] for r in lines] == [r["id"] for r in recs]
    assert [r.get("_serializer") for r in lines].count("inveniordm") == 1
    assert [r["id"] for r in zc.iter_census(out)] == [r["id"] for r in recs]
    poison = [json.loads(x) for x in (tmp_path / "census.jsonl.poison.jsonl").read_text()
              .splitlines()]
    assert [(p["status"], p["id"], p["offset"]) for p in poison] == [("converted", 1007, 7)]
    s2 = zc.run_census(_client(server, monkeypatch), out)        # resume: window already done
    assert s2["windows_skipped"] == 1 and s2["written"] == 0
    from zenodo_census.cli import _poison_summary
    with (tmp_path / "census.jsonl.poison.jsonl").open("a") as fh:     # a re-paged window, torn line
        fh.write(json.dumps(poison[0]) + "\n" + '{"status": "unres')
    assert _poison_summary(tmp_path / "census.jsonl.poison.jsonl") == {"converted": 1}


# --------------------------------------------------------------------------- #
# signals                                                                      #
# --------------------------------------------------------------------------- #

def test_clean_text_splits_nbsp_glued_words():
    t = sg.clean_text("<p>Using ab initio&nbsp;defect techniques</p>")
    assert t == "Using ab initio defect techniques"
    assert sg.DFT_RE.search(t) and sg.MATERIALS_RE.search(t)


@pytest.mark.parametrize("text,rx", [
    ("anharmonic potential energy surfaces", sg.DFT_RE),
    ("trained interatomic potentials", sg.MLIP_RE),
    ("grain boundaries in thin films", sg.MATERIALS_RE),
    ("band structures and densities of states", sg.DFT_RE),
    ("projector augmented waves and plane waves", sg.DFT_RE),
])
def test_phrases_match_their_plurals(text, rx):
    assert rx.search(text)


def test_formula_hits():
    assert sg.formula_hits("LiNiO2, CdTe, Sb2O5, MoS2 and Sn2SbS2I3") == [
        "LiNiO2", "CdTe", "Sb2O5", "MoS2", "Sn2SbS2I3"]
    assert sg.formula_hits("SSP5 CSV CO2 H2O NaCl HeLa SARS-CoV-2 DNA In As") == []


def test_negative_files_see_through_compression():
    files = [{"key": "reads.fastq.gz"}, {"key": "traj.xtc"}, {"key": "data.zip"}]
    assert sg.negative_files(files) == ["reads.fastq.gz", "traj.xtc"]


def test_paper_dois_from_related_ids_description_and_arxiv():
    meta = {"related_identifiers": [
        {"identifier": "10.1039/D4EE04647A", "relation": "isSupplementTo", "scheme": "doi"},
        {"identifier": "10.5281/zenodo.13888306", "relation": "isVersionOf", "scheme": "doi"},
        {"identifier": "https://doi.org/10.1103/PhysRevB.1.2", "relation": "cites", "scheme": "url"},
        {"identifier": "arXiv:2301.01234", "relation": "isSupplementTo", "scheme": "arxiv"},
        {"identifier": "10.1021/x.1", "relation": "isPartOf", "scheme": "doi"}],
        "description": '<p>Article <a href="https://pubs.acs.org/doi/10.1021/acsenergylett.1c00380">'
                       "here</a>; data 10.6084/m9.figshare.1 and arxiv.org/abs/2402.00001v2.</p>"}
    # most direct first: related identifiers in their order, then the description
    assert sg.paper_dois(meta) == [
        "10.1039/d4ee04647a", "10.1103/physrevb.1.2", "10.48550/arxiv.2301.01234",
        "10.1021/acsenergylett.1c00380", "10.48550/arxiv.2402.00001"]


def test_norm_name_and_licence_class():
    assert sg.norm_name("Kavanagh, Seán R.") == sg.norm_name("Seán R. Kavanagh") == "kavanagh|s"
    assert sg.norm_name("BAM") is None
    assert [sg.licence_class(x) for x in ("cc-by-4.0", "cc-zero", "mit", "cc-by-nc-4.0",
                                          "cc-by-nc-nd-4.0", "cc-by-nd-4.0", None,
                                          "notspecified")] == [
        "open", "open", "open", "nc", "nd", "nd", "none", "none"]


# The real metadata of three records the keyword harvest missed (live, 2026-09-25).
KAV_4541602 = zhit(
    "4541602", {"V_Cd_Zenodo.tar.gz": 4828279516}, title="Rapid Recombination by Cadmium "
    "Vacancies in CdTe", keywords=["CdTe, vacancies, point defects, non-radiative recombination, "
                                   "carrier capture, photovoltaic efficiency"],
    description="<p>CdTe is a key thin-film photovoltaic technology. ... Using ab initio&nbsp;"
                "defect techniques, we calculate a negative-U double-acceptor level for V<sub>Cd"
                "</sub> ... anharmonic potential energy surfaces for carrier capture.</p>\n\n<p>"
                "Open-access ACS Energy Letters article&nbsp;<a href=\"https://pubs.acs.org/doi/"
                "10.1021/acsenergylett.1c00380\">here</a>.</p>", communities=["wmd-group"])
KAV_13888307 = zhit(
    "13888307", {"Rasmus_Expt_Data.xlsx": 275071, "Zenodo.zip": 19592469794},
    title="Intrinsic point defect tolerance in selenium for indoor and tandem photovoltaics",
    description="<p>Accompanying data &amp; analysis notebooks for 'Intrinsic point defect "
                "tolerance in selenium for indoor and tandem photovoltaics'.<br><br>Article link: "
                "https://doi.org/10.1039/D4EE04647A</p>",
    related=[{"identifier": "10.26434/chemrxiv-2024-91h02-v2", "relation": "isSupplementTo",
              "scheme": "doi"},
             {"identifier": "10.1039/D4EE04647A", "relation": "isSupplementTo", "scheme": "doi"}])
KAV_12518256 = zhit(
    "12518256", {"archive.tar.gz": 5869888},
    title="Code and data for Oxygen dimerization as a defect-driven process in bulk LiNiO2",
    description="<p>Data and code required to generate figures in the article \"Oxygen "
                "dimerization as a defect-driven process in bulk LiNiO2\".<br><br>Available as a "
                "preprint at 10.26434/chemrxiv-2024-lcmk</p>",
    related=[{"identifier": "10.1021/acsenergylett.4c01307", "relation": "isPublishedIn",
              "scheme": "doi"}])


def _tier(hit: dict, seeds: sg.Seeds | None = None, **papers: Any) -> tuple[str, list[str]]:
    rec = zc.slim_hit(hit)
    sig = {**sg.text_signals(rec), **sg.identity_signals(rec, seeds or sg.Seeds()), **papers}
    return sg.tier_of(sig)


def test_known_misses_reach_the_top_tiers_on_text_alone():
    assert _tier(KAV_4541602) == ("T1", ["text_dft_materials"])
    t, why = _tier(KAV_13888307)
    assert t == "T2" and "text_materials" in why
    t, why = _tier(KAV_12518256)
    assert t == "T2" and "formula" in why
    # the linked paper citing VASP (OpenAlex) lifts the sparse record to T1
    assert _tier(KAV_13888307, paper_vasp=["10.1039/d4ee04647a"])[0] == "T1"


def test_tier_rule_negatives_and_ambiguous_words():
    assert sg.tier_of({"dft": ["neb"], "negative": ["proteins", "enzymes"]})[0] == "T0"
    assert sg.tier_of({"dft": ["neb", "nudged elastic band"]})[0] == "T1"
    # an ambiguous DFT word needs clear materials context (2 cues) for T1
    assert sg.tier_of({"dft": ["dft"], "materials": ["perovskite"]})[0] == "T2"
    assert sg.tier_of({"dft": ["dft"], "materials": ["perovskite", "oxide"]})[0] == "T1"
    # negative file types count like words: one stray type does not outvote a materials cue
    assert sg.tier_of({"neg_files": ["x.fastq.gz"], "materials": ["oxide"]})[0] == "T2"
    assert sg.tier_of({"neg_files": ["x.fastq.gz", "y.bam"], "materials": ["oxide"]})[0] == "T0"
    assert sg.tier_of({"negative": ["fish"]})[0] == "T0"
    assert sg.tier_of({})[0] == "T3"
    assert sg.tier_of({"name": ["wang|y"]})[0] == "T3"          # a name alone is too weak
    assert sg.tier_of({"name": ["wang|y"], "materials": ["oxides"]}) == (
        "T2", ["sparse_with_materials_cue", "seed_name"])
    assert sg.tier_of({"owner": ["9"]})[0] == "T1"
    assert sg.tier_of({"owner": ["9"], "bulk_owner": True})[0] == "T2"
    assert sg.tier_of({"owner": ["9"], "negative": ["fish", "birds"]})[0] == "T0"
    assert sg.tier_of({"paper_fields": ["Materials Science"]})[0] == "T2"
    assert sg.tier_of({"epmc": ["PMC1"]})[0] == "T1"
    assert sg.tier_of({"loose_primary": ["OUTCAR"]})[0] == "T1"


def test_identity_signals_respect_frequency_caps():
    seeds = sg.Seeds()
    seeds.add_record(zc.slim_hit(zhit("1", owners=["77"], communities=["wmd-group", "eu"],
                                      creators=[{"name": "Kavanagh, Seán", "orcid": "O-1"},
                                                {"name": "Wang, Y."}])))
    seeds.name_df = {"kavanagh|s": 5, "wang|y": 5000}
    seeds.community_size = {"wmd-group": 40, "eu": 90000}
    seeds.owner_size = {"77": 12}
    rec = zc.slim_hit(zhit("2", owners=["77"], communities=["wmd-group", "eu"],
                           creators=[{"name": "S. Kavanagh"}, {"name": "Y. Wang"},
                                     {"name": "X", "orcid": "O-1"}]))
    sig = sg.identity_signals(rec, seeds)
    assert sig == {"owner": ["77"], "orcid": ["O-1"], "name": ["kavanagh|s"],
                   "community": ["wmd-group"], "bulk_owner": False, "bulk_orcid": False}
    seeds.owner_size["77"] = 5000
    assert sg.identity_signals(rec, seeds)["bulk_owner"] is True
    seeds.orcid_size["O-1"] = 5000          # a seed author on thousands of records: weak only
    sig = sg.identity_signals(rec, seeds)
    assert sig["orcid"] == [] and sig["bulk_orcid"] is True


# --------------------------------------------------------------------------- #
# score                                                                        #
# --------------------------------------------------------------------------- #

def _dataset_meta(tmp_path: Path, recids: list[tuple[str, str]]) -> Path:
    rows = [{"calc_id": f"zenodo:{r}:c{i}", "provenance": {
        "source": "zenodo", "record_id": r, "conceptrecid": c,
        "creators": ["Kavanagh, Seán R."]}} for i, (r, c) in enumerate(recids)]
    return _write_jsonl(tmp_path / "metadata.jsonl", rows)


def test_exclusions_and_seeds(tmp_path):
    ex = zs.Exclusions()
    ex.add_dataset(_dataset_meta(tmp_path, [("100", "99"), ("100", "99")]))
    assert ex.add_manifest(_write_jsonl(tmp_path / "keep.jsonl", [
        {"recid": "200", "conceptrecid": "199"}, {"recid": "300~a.zip-1234"}])) == 2
    assert ex.status({"id": 100}) == "in_dataset"
    assert ex.status({"id": 105, "conceptrecid": "99"}) == "in_dataset"   # a newer version
    assert ex.status({"id": 201, "conceptrecid": "199"}) == "evaluated"
    assert ex.status({"id": 300}) == "evaluated"
    assert ex.status({"id": 400, "conceptrecid": "398"}) is None
    census = _write_jsonl(tmp_path / "census.jsonl", [
        zc.slim_hit(zhit("100", concept="99", owners=["5"],
                         creators=[{"name": "Kavanagh, Seán R.", "orcid": "0000-0003-4577-9647"}],
                         communities=["wmd-group"])),
        zc.slim_hit(zhit("400", concept="398"))])
    seeds = zs.build_seeds(census, ex)
    assert seeds.owners == {"5"} and seeds.orcids == {"0000-0003-4577-9647"}
    assert "kavanagh|s" in seeds.names and seeds.communities == {"wmd-group"}
    assert seeds.owner_size == {"5": 1, "1": 1}


def test_score_end_to_end_with_links(tmp_path):
    census = _write_jsonl(tmp_path / "census.jsonl", [
        zc.slim_hit(h) for h in (
            KAV_13888307, KAV_4541602, KAV_12518256,
            zhit("500", {"x.zip": 10}, title="Fish populations in the Baltic", keywords=["fish"]),
            zhit("600", {"y.zip": 10}, title="Data", concept="599"),
            zhit("700", {"z.zip": 10}, title="Data", concept="699", access="restricted"),
            zhit("10630244", {"Sb2O5_zenodo.zip": 5}, title="Computational Prediction of an "
                 "Antimony-based n-type Transparent Conducting Oxide", license=None,
                 description="we use hybrid density functional theory"))])
    ex = zs.Exclusions()
    seeds = zs.build_seeds(census, ex)
    citing = {"599": {"10.1103/physrevb.9.9"}}
    oa = {"10.1039/d4ee04647a": {"doi": "10.1039/d4ee04647a", "status": 200, "cites_vasp": True,
                                 "field": "Materials Science"},
          "10.1103/physrevb.9.9": {"doi": "10.1103/physrevb.9.9", "status": 200,
                                   "cites_vasp": False, "field": "Physics and Astronomy"}}
    dois = zs.lookup_dois(census, seeds, ex, citing)
    assert "10.1039/d4ee04647a" in dois and "10.1103/physrevb.9.9" in dois
    assert "10.1021/acsenergylett.1c00380" not in dois      # 4541602 is T1 already
    rep = zs.score(census, tmp_path / "scored.jsonl", excl=ex, seeds=seeds, citing=citing,
                   epmc={"4541602": {"PMC9"}}, openalex=oa)
    rows = {r["recid"]: r for r in read_jsonl(tmp_path / "scored.jsonl")}
    assert rows["13888307"]["tier"] == "T1" and "paper_cites_vasp" in rows["13888307"]["reasons"]
    assert rows["4541602"]["tier"] == "T1" and "epmc_mention" in rows["4541602"]["reasons"]
    assert rows["12518256"]["tier"] == "T2"
    assert rows["500"]["tier"] == "T0"
    assert rows["600"]["tier"] == "T2" and rows["600"]["reasons"] == ["paper_field"]
    assert rows["700"]["tier"] == "X"
    assert rows["10630244"]["licence_class"] == "none" and not rows["10630244"]["licence_admitted"]
    assert rows["13888307"]["n_zip"] == 1 and rows["4541602"]["n_unpeekable"] == 1
    assert rep["probes"]["13888307"]["tier"] == "T1" and rep["counts"]["tier_T1"] >= 3


# --------------------------------------------------------------------------- #
# links                                                                        #
# --------------------------------------------------------------------------- #

def test_zenodo_ids_in_text():
    t = ("see https://doi.org/10.5281/zenodo.13888307, zenodo.org/records/4541602 and "
         "https://zenodo.org/record/12518256; 10.5281/zenodo.13888307 again")
    assert zl.zenodo_ids_in(t) == ["13888307", "4541602", "12518256"]


class ScriptedSession:
    """Answers GETs from a list of (predicate, response) rules; records calls."""

    def __init__(self, rules: list[tuple[Any, FakeResp]]):
        self.rules = rules
        self.calls: list[tuple[str, dict | None]] = []
        self.headers: dict[str, str] = {}

    def get(self, u: str, params: dict | None = None, **_: Any) -> FakeResp:
        self.calls.append((u, params))
        for pred, resp in self.rules:
            if pred(u, params):
                return resp
        return FakeResp(404)


def test_datacite_references_pages_and_resumes(tmp_path):
    def ev(subj: str, obj: str) -> dict:
        return {"attributes": {"subj-id": f"https://doi.org/{subj}",
                               "obj-id": f"https://doi.org/{obj}",
                               "occurred-at": "2025-01-02T00:00:00Z"}}
    page1 = FakeResp(200, json_obj={"data": [ev("10.1000/A", "10.5281/zenodo.11"),
                                             ev("10.1000/B", "10.5281/zenodo.12")],
                                    "links": {"next": "https://api.datacite.org/events?c=2"}})
    page2 = FakeResp(200, json_obj={"data": [ev("10.1000/C", "10.5281/zenodo.11")], "links": {}})
    s = ScriptedSession([(lambda u, p: u.endswith("c=2"), page2),
                         (lambda u, p: p and p.get("prefix") == "10.5281", page1)])
    out = tmp_path / "refs.jsonl"
    r1 = zl.datacite_references(out, s, max_pages=1)
    assert r1["links"] == 2 and not r1["complete"]
    r2 = zl.datacite_references(out, s)
    assert r2["links"] == 1 and r2["complete"]
    assert zl.load_citing(out) == {"11": {"10.1000/a", "10.1000/c"}, "12": {"10.1000/b"}}
    assert zl.datacite_references(out, s)["status"] == "already complete"


def test_epmc_mentions(tmp_path):
    search = FakeResp(200, json_obj={"resultList": {"result": [
        {"pmcid": "PMC1", "doi": "10.1/p1", "title": "VASP study", "pubYear": "2024"},
        {"pmcid": "PMC2", "doi": "10.1/p2", "title": "other"},
        {"id": "PPR9", "source": "PPR", "title": "a VASP preprint"},
        {"pmid": "3"}]}, "nextCursorMark": "*"})
    ft1 = FakeResp(200, text="<article>Data at https://doi.org/10.5281/zenodo.777 and "
                            "zenodo.org/records/888</article>")
    s = ScriptedSession([(lambda u, p: u.endswith("/search"), search),
                         (lambda u, p: "PMC1/fullTextXML" in u, ft1),
                         (lambda u, p: "PPR9/fullTextXML" in u,
                          FakeResp(200, text="doi:10.5281/zenodo.999")),
                         (lambda u, p: "PMC2/fullTextXML" in u, FakeResp(404))])
    out = tmp_path / "epmc.jsonl"
    r = zl.epmc_mentions(out, s, interval=0)
    assert r["hits"] == 4 and r["fetched"] == 2 and r["with_zenodo"] == 2
    assert zl.load_epmc(out) == {"777": {"PMC1"}, "888": {"PMC1"}, "999": {"PPR9"}}
    r2 = zl.epmc_mentions(out, s, interval=0)                 # resumable per paper
    assert r2["skipped_done"] == 2


def test_openalex_row_and_lookup_cache(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENALEX_API_KEY", raising=False)
    work = {"id": "https://openalex.org/W1", "type": "article", "publication_year": 2024,
            "referenced_works": ["https://openalex.org/W2083222334", "https://openalex.org/W5"],
            "referenced_works_count": 2,
            "primary_topic": {"display_name": "Perovskites", "field": {"display_name":
                              "Materials Science"}, "subfield": {"display_name": "Materials"}}}
    row = zl.openalex_row("10.1/x", work, 200)
    assert row["cites_vasp"] and row["field"] == "Materials Science" and row["n_refs"] == 2
    # a linked DOI that IS a VASP method paper (it does not cite itself)
    kresse = {"id": "https://openalex.org/W2083222334", "referenced_works": []}
    assert zl.openalex_row("10.1103/physrevb.54.11169", kresse, 200)["cites_vasp"]
    sessions: list[ScriptedSession] = []

    def factory() -> ScriptedSession:
        s = ScriptedSession([(lambda u, p: u.endswith("doi:10.1/x"), FakeResp(200, json_obj=work)),
                             (lambda u, p: u.endswith("doi:10.1/gone"), FakeResp(404)),
                             (lambda u, p: u.endswith("doi:10.1/bad"), FakeResp(400))])
        sessions.append(s)
        return s
    cache = tmp_path / "oa.jsonl"
    r = zl.openalex_lookup(["10.1/x", "10.1/gone", "10.1/bad"], cache, session_factory=factory,
                           workers=1, interval=0)
    assert (r["ok"], r["not_found"], r["failed"]) == (1, 1, 1)
    oa = zl.load_openalex(cache)
    assert oa["10.1/x"]["cites_vasp"] and oa["10.1/gone"]["status"] == 404 and "10.1/bad" not in oa
    r2 = zl.openalex_lookup(["10.1/x", "10.1/gone", "10.1/bad"], cache, session_factory=factory,
                            workers=1, interval=0)
    assert r2["todo"] == 1                                      # only the failed one is retried
    assert all("api_key" not in (p or {}) for s in sessions for _, p in s.calls)


class FakeSearchClient:
    """``search_page`` over a fixed id -> concept table (what an all-versions recid query returns)."""

    def __init__(self, table: dict[str, str]):
        self.table = table
        self.queries: list[tuple[str, dict]] = []

    def search_page(self, q: str, page: int = 1, size: int = 25, sort: str = "newest",
                    extra: dict | None = None) -> dict:
        self.queries.append((q, extra or {}))
        ids = q[len("recid:("):-1].split(" OR ")
        return {"hits": {"hits": [{"id": int(i), "conceptrecid": self.table[i]}
                                  for i in ids if i in self.table]}}


def test_resolve_versions_and_concept_aware_loaders(tmp_path):
    out = tmp_path / "versions.jsonl"
    client = FakeSearchClient({"1999": "2000", "3001": "3000"})
    r = zl.resolve_versions(["1999", "3001", "4444", "x"], out, client, batch=2)
    assert (r["todo"], r["resolved"], r["unknown"]) == (3, 2, 1)
    assert client.queries[0][1] == {"all_versions": "true"}
    assert zl.load_versions(out) == {"1999": "2000", "3001": "3000"}
    assert zl.resolve_versions(["1999", "4444"], out, client)["todo"] == 0   # resumable
    refs = _write_jsonl(tmp_path / "refs.jsonl", [{"zenodo_id": "1999", "citing_doi": "10.1000/p"}])
    assert zl.load_citing(refs, zl.load_versions(out)) == {"1999": {"10.1000/p"},
                                                           "2000": {"10.1000/p"}}
    census = _write_jsonl(tmp_path / "census.jsonl", [zc.slim_hit(zhit("2001", concept="2000"))])
    assert zc.census_keys(census) == {"2001", "2000"}


def test_score_reports_epmc_coverage(tmp_path):
    census = _write_jsonl(tmp_path / "census.jsonl", [
        zc.slim_hit(zhit("2001", {"a.zip": 5}, title="Data", concept="2000")),
        zc.slim_hit(zhit("5001", {"b.zip": 5}, title="Data", concept="5000"))])
    ex = zs.Exclusions()
    ex.add_dataset(_dataset_meta(tmp_path, [("100", "99")]))
    versions = {"1999": "2000"}
    epmc = zl._with_concepts({"1999": {"PMC1"}, "100": {"PMC2"}, "777": {"PMC3"}}, versions)
    rep = zs.score(census, tmp_path / "scored.jsonl", excl=ex, seeds=zs.build_seeds(census, ex),
                   epmc=epmc, versions=versions)
    rows = {r["recid"]: r for r in read_jsonl(tmp_path / "scored.jsonl")}
    assert rows["2001"]["tier"] == "T1" and rows["2001"]["signals"]["epmc"] == ["PMC1"]
    assert rep["epmc_coverage"] == {"census_T1": 1, "in_dataset": 1, "not_in_census": 1}


# --------------------------------------------------------------------------- #
# head-peek                                                                    #
# --------------------------------------------------------------------------- #

VASP_TREE = {"calc1/INCAR": b"ENCUT = 520\n", "calc1/POSCAR": b"Si\n1.0\n",
             "calc1/OUTCAR": b"vasp.6.3\n" * 50, "calc1/vasprun.xml": b"<modeling/>" * 50}


@pytest.mark.parametrize("kind,sniffed", [("gz", "gzip"), ("bz2", "bzip2"), ("xz", "xz"),
                                          ("zst", "zstd"), ("none", "tar")])
def test_head_peek_small_archive_is_a_complete_listing(kind, sniffed):
    blob = compress(make_tar(VASP_TREE), kind)
    s = FakeRangeSession({"u": blob})
    ev, status = hp.head_peek(s, "u", "calcs.tar.gz")
    assert status == "ok" and ev is not None and ev.kind == sniffed
    assert ev.names == list(VASP_TREE) and ev.total_size == len(blob)
    e = ev.evidence()
    assert e["n_primary"] == 2 and e["n_vasp_named"] == 4
    if kind != "zst":
        assert ev.complete
    assert s.requests[0][1]["Range"] == f"bytes=0-{hp.DEFAULT_HEAD_BYTES - 1}"


def test_head_peek_big_archive_lists_only_the_leading_members():
    big = os.urandom(3 << 20)               # incompressible: the head ends inside it
    blob = compress(make_tar({"calc/INCAR": b"x" * 100, "big.bin": big, "calc/OUTCAR": b"y"}),
                    "gz")
    ev, status = hp.head_peek(FakeRangeSession({"u": blob}), "u", head_bytes=1 << 20)
    assert status == "ok" and ev is not None and not ev.complete
    assert ev.names == ["calc/INCAR", "big.bin"]
    e = ev.evidence()
    assert e["n_primary"] == 0 and e["n_vasp_named"] == 1       # a VASP hint, not a primary


def test_head_peek_long_names_gnu_and_pax():
    long = "d" * 120 + "/OUTCAR"
    for fmt in (tarfile.GNU_FORMAT, tarfile.PAX_FORMAT):
        names, end, valid = hp.walk_tar(make_tar({long: b"1", "short/INCAR": b"2"}, fmt))
        assert valid and end and names == [long, "short/INCAR"]


def test_head_peek_single_file_and_other_containers():
    buf = io.BytesIO()
    with gzip.GzipFile(filename="OUTCAR", fileobj=buf, mode="wb") as g:
        g.write(b"not a tar " * 100)
    ev, status = hp.head_peek(FakeRangeSession({"u": buf.getvalue()}), "u", "results.gz")
    assert status == "ok" and ev is not None and ev.kind == "single" and ev.names == ["OUTCAR"]
    ev, status = hp.head_peek(FakeRangeSession({"u": gzip.compress(b"plain text")}), "u",
                              "notes.txt.gz")
    assert ev is not None and ev.names == ["notes.txt"]
    assert hp.head_peek(FakeRangeSession({"u": make_zip({"a": b"1"})}), "u")[1] == "is_zip"
    assert hp.head_peek(FakeRangeSession({"u": b"7z\xbc\xaf\x27\x1c" + b"0" * 64}), "u")[1] == "is_7z"
    assert hp.head_peek(FakeRangeSession({"u": b"hello world" * 100}), "u")[1] == "unknown_format"
    corrupt = b"\x1f\x8b\x08\x00" + b"\xff" * 200
    assert hp.head_peek(FakeRangeSession({"u": corrupt}), "u")[1].startswith("decompress_failed")
    assert hp.head_peek(FakeRangeSession({}), "u")[1] == "peek_failed: HTTP 404"


def test_head_peek_gzip_with_trailing_padding_still_reads():
    blob = gzip.compress(make_tar(VASP_TREE)) + b"\0" * 1024
    ev, status = hp.head_peek(FakeRangeSession({"u": blob}), "u")
    assert status == "ok" and ev is not None and ev.names == list(VASP_TREE) and ev.complete


# --------------------------------------------------------------------------- #
# triage                                                                       #
# --------------------------------------------------------------------------- #

def _f(key: str, size: int = 10) -> dict:
    return {"key": key, "size": size, "links": {"self": f"https://zenodo.org/x/{key}"}}


class _BrokenBodyResp(FakeResp):
    """Zenodo's reply to a suffix range longer than the file: 206, an underflowed start, and a
    Content-Length it never delivers (reading the body raises)."""

    @property
    def content(self) -> bytes:
        import requests
        raise requests.exceptions.ChunkedEncodingError("IncompleteRead(0 bytes read)")


class ZenodoLikeSession(FakeRangeSession):
    def get(self, u: str, headers: dict | None = None, **kw: Any) -> FakeResp:
        rng = (headers or {}).get("Range", "")
        body = self.blobs.get(u)
        if body is not None and rng.startswith("bytes=-") and int(rng[7:]) > len(body):
            self.requests.append((u, dict(headers or {})))
            n = len(body)
            return _BrokenBodyResp(206, b"", {"Content-Range": f"bytes {2**64 - int(rng[7:]) + n}"
                                                               f"-{n - 1}/{n}",
                                              "Content-Length": rng[7:]})
        return super().get(u, headers, **kw)


def test_zip_peek_survives_zenodo_suffix_range_bug():
    from materials_cloud_harvest.remote_zip import peek_archive
    small = make_zip({"calc/vasprun.xml": b"<modeling/>", "calc/INCAR": b"x"})   # << 1 MiB
    sess = ZenodoLikeSession({"https://zenodo.org/f/small.zip": small})
    f = {"key": "small.zip", "size": len(small), "links": {"self": "https://zenodo.org/f/small.zip"}}
    ev = zt.peek_file(sess, "zip", f)
    assert ev["status"] == "ok" and ev["n_primary"] == 1
    assert sess.requests[0][1]["Range"] == f"bytes=0-{len(small) - 1}"      # an ordinary range
    _, status = peek_archive(sess, "https://zenodo.org/f/small.zip")         # the old suffix read
    assert status.startswith("peek_failed")
    # a stale listing size is corrected from the server's Content-Range
    for wrong in (len(small) + 5000, len(small) // 2):
        ev2 = zt.peek_file(sess, "zip", {**f, "size": wrong})
        assert ev2["status"] == "ok" and ev2["n_primary"] == 1, wrong


def test_file_verdict_matrix():
    ok = {"status": "ok"}
    V = zt.file_verdict
    assert V(_f("a.zip"), {**ok, "mode": "zip", "n_primary": 2})[0] == "vasp"
    assert V(_f("a.zip"), {**ok, "mode": "zip", "n_primary": 0}) == ("empty", None)
    assert V(_f("a.zip"), {**ok, "mode": "zip", "n_nested": 1})[0] == "unresolved"
    assert V(_f("a.zip"), {"mode": "zip", "status": "peek_failed: x"})[0] == "unresolved"
    assert V(_f("a.tgz"), {**ok, "mode": "head", "complete": True, "n_primary": 0}) == (
        "empty", None)
    assert V(_f("a.tgz"), {**ok, "mode": "head", "complete": False, "n_vasp_named": 1})[0] == "vasp"
    assert V(_f("a.tgz"), {**ok, "mode": "head", "complete": False})[0] == "unresolved"
    assert V(_f("a.tgz"), {**ok, "mode": "head", "kind": "single", "names": ["OUTCAR"]}) == (
        "empty", None)
    assert V(_f("a.rar"), None)[0] == "unresolved"
    assert V(_f("band_data.json.gz"), None) == ("empty", None)      # a data file, not a tarball
    assert V(_f("README.md"), None)[0] == "loose"
    st, ff = V(_f("x.tar.gz"), {**ok, "mode": "zip", "n_primary": 1, "as_kind": "zip"})
    assert st == "vasp" and ff and ff["archive_kind"] == "zip"
    st, ff = V(_f("e.aiida"), {**ok, "mode": "zip", "n_primary": 1, "aiida_format": "legacy"})
    assert ff and ff["archive_kind"] == "zip"
    st, ff = V(_f("e.aiida"), {**ok, "mode": "zip", "aiida_format": "sqlite_zip",
                               "db_status": "db_too_large"})
    assert st == "unresolved" and ff and ff["archive_kind"] == "aiida"


def test_decide_policy():
    ev_empty = {"mode": "zip", "status": "ok", "n_primary": 0}
    ev_vasp = {"mode": "zip", "status": "ok", "n_primary": 1}
    files = [_f("a.zip", 5), _f("b.tar.gz", 7), _f("README.md", 1)]
    keep, why, fetch, nb = zt.decide("T2", files, {"a.zip": ev_vasp})
    assert keep and why == "vasp_evidence" and [f["key"] for f in fetch] == [
        "a.zip", "b.tar.gz", "README.md"] and nb == {"evidence": 5, "blind": 7}
    keep, why, _, _ = zt.decide("T2", files, {"a.zip": ev_empty})
    assert not keep and why == "unresolved_not_fetched"
    keep, why, fetch, _ = zt.decide("T1", files, {"a.zip": ev_empty})
    assert keep and why == "strong_unresolved" and [f["key"] for f in fetch] == [
        "b.tar.gz", "README.md"]                                   # the proven-empty zip pruned
    keep, why, _, _ = zt.decide("T1", [_f("a.zip")], {"a.zip": ev_empty})
    assert not keep and why == "proved_no_vasp"
    keep, why, fetch, _ = zt.decide("T3", [_f("OUTCAR.gz"), _f("x.zip")], {"x.zip": ev_empty})
    assert keep and why == "vasp_evidence" and [f["key"] for f in fetch] == ["OUTCAR.gz"]


def test_wilson_and_pacer(monkeypatch):
    p, lo, hi = zt.wilson(3, 3000)
    assert p == pytest.approx(0.001) and 0 < lo < p < hi < 0.004
    assert zt.wilson(0, 0) == (0.0, 0.0, 0.0)
    clock = {"t": 0.0}
    slept: list[float] = []
    monkeypatch.setattr(zt.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(zt.time, "sleep", lambda d: (slept.append(d), clock.__setitem__(
        "t", clock["t"] + d)))
    pacer = zt.Pacer(0.8)
    for _ in range(3):
        pacer.wait()
    assert slept == [pytest.approx(0.8), pytest.approx(0.8)]


def test_paced_session_sends_token_only_to_zenodo():
    import requests
    s = zt.PacedSession(zt.Pacer(0), token="SECRET")
    for host, expect in (("https://zenodo.org/api/records/1", True),
                         ("https://sandbox.zenodo.org/x", True),
                         ("https://api.openalex.org/works", False),
                         ("https://evil.example/zenodo.org", False)):
        req = s.prepare_request(requests.Request("GET", host))
        assert ("Authorization" in req.headers) is expect, host


def _triage_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    vasp_zip = make_zip({"run/vasprun.xml": b"<modeling/>", "run/INCAR": b"x"})
    empty_zip = make_zip({"figs/a.png": b"png"})
    z64 = make_zip64({"calc/OUTCAR": b"vasp", "calc/POSCAR": b"p"}, monkeypatch)
    head_tgz = gzip.compress(make_tar({"c/INCAR": b"1", "c/OUTCAR": b"2"}))
    blind_tgz = gzip.compress(make_tar({"big.bin": os.urandom(2 << 20), "c/OUTCAR": b"2"}))
    specs: list[tuple[str, dict[str, bytes], dict[str, Any]]] = [
        ("1001", {"figures.zip": empty_zip, "data.zip": vasp_zip},       # T2 -> evidence
         {"title": "Perovskite oxides and CdTe"}),
        ("1003", {"big.zip": z64}, {"title": "Point defects in LiNiO2"}),  # T2, ZIP64
        ("1005", {"calcs.tar.gz": head_tgz}, {"title": "Data", "owners": ["42"]}),  # T1 seed
        ("1007", {"stuff.tar.gz": blind_tgz},                            # T1 -> blind fetch
         {"title": "Ab initio DFT calculations of perovskites"}),
        ("1009", {"stuff.tar.gz": blind_tgz}, {"title": "Thin films of oxides"}),  # T2 -> drop
        ("1011", {"data.zip": vasp_zip},                                 # T1, ND -> review
         {"title": "Hybrid DFT study of perovskite halides", "license": "cc-by-nd-4.0"}),
        ("1013", {"x.zip": vasp_zip}, {"title": "Data"}),                 # T3 (sampled)
        ("1015", {"y.zip": empty_zip}, {"title": "Fish in lakes", "keywords": ["fish"]}),  # T0
    ]
    hits, blobs = [], {}
    for rid, files, kw in specs:
        hits.append(zhit(rid, dict(files), concept=str(int(rid) - 1), **kw))
        for k, b in files.items():
            blobs[url(rid, k)] = b
    census = _write_jsonl(tmp_path / "census.jsonl", [zc.slim_hit(h) for h in hits])
    seeds = sg.Seeds()
    seeds.owners = {"42"}
    zs.score(census, tmp_path / "scored.jsonl", excl=zs.Exclusions(), seeds=seeds)
    return census, blobs, hits


def test_triage_end_to_end_decisions_and_resume(tmp_path, monkeypatch):
    census, blobs, _ = _triage_fixture(tmp_path, monkeypatch)
    tiers = {r["recid"]: r["tier"] for r in read_jsonl(tmp_path / "scored.jsonl")}
    assert tiers == {"1001": "T2", "1003": "T2", "1005": "T1", "1007": "T1", "1009": "T2",
                     "1011": "T1", "1013": "T3", "1015": "T0"}
    sess = FakeRangeSession(blobs)
    out = tmp_path / "keep.jsonl"
    rep = zt.triage(tmp_path / "scored.jsonl", census, out, session_factory=lambda: sess,
                    residual_sample=5, negative_sample=5, interval=0, head_bytes=1 << 20)
    keep = {r["recid"]: r for r in read_jsonl(out)}
    assert set(keep) == {"1001", "1003", "1005", "1007", "1013"}
    assert [f["key"] for f in keep["1001"]["files"]] == ["data.zip"]      # empty zip pruned
    assert keep["1003"]["vasp_category"] == "vasp_direct"                # ZIP64 read
    assert keep["1007"]["census"]["triage_reason"] == "strong_unresolved"
    assert keep["1013"]["census"]["selected_as"] == "residual_sample"
    assert all(f.get("download", "").startswith(ZBASE) for r in keep.values() for f in r["files"])
    review = {r["recid"] for r in read_jsonl(tmp_path / "keep.licence_review.jsonl")}
    assert review == {"1011"}
    s = rep
    assert s["kept"]["records"] == 5 and s["licence_review_records"] == 1
    assert s["samples"]["residual_sample"]["n"] == 1
    assert s["samples"]["residual_sample"]["positive"] == 1
    assert s["samples"]["residual_sample"]["positive_primary"] == 1
    assert s["samples"]["negative_sample"]["positive"] == 0
    n_requests = len(sess.requests)
    zt.triage(tmp_path / "scored.jsonl", census, out, session_factory=lambda: sess,
              residual_sample=5, negative_sample=5, interval=0, head_bytes=1 << 20)
    assert len(sess.requests) == n_requests                              # every verdict cached
    # a later full-T3 run skips what the first run kept, reusing the shared peek cache
    rep2 = zt.triage(tmp_path / "scored.jsonl", census, tmp_path / "keep_t3.jsonl",
                     session_factory=lambda: sess, tiers=["T3"], residual_sample=0,
                     negative_sample=0, interval=0, exclude_keep=[out])
    assert rep2["kept"]["records"] == 0 and rep2["selected"] == {}
    assert len(sess.requests) == n_requests


# --------------------------------------------------------------------------- #
# full offline run: census -> score -> triage -> shared fetch -> parse -> verify
# --------------------------------------------------------------------------- #

def _fixture(name: str) -> bytes:
    ase = pytest.importorskip("ase")
    pytest.importorskip("pymatgen")
    p = Path(ase.__file__).parent / "test" / "testdata" / "vasp" / name
    if not p.is_file():
        pytest.skip("ASE VASP test fixtures not available")
    return p.read_bytes()


def test_end_to_end_census_keep_list_through_shared_fetch_and_parse(tmp_path):
    from zenodo_harvest import fetch as zfetch
    from zenodo_harvest.dataset_ops import verify_dataset
    from zenodo_harvest.parse import parse
    vz = make_zip({"relax/vasprun.xml": _fixture("vasprun_dfpt.xml"), "figs/a.png": b"png"})
    tg = gzip.compress(make_tar({"neb/OUTCAR": _fixture("OUTCAR_example_1")}))
    hit = zhit("2001", {"data.zip": vz, "more.tar.gz": tg, "notes.txt": b"hello"},
               title="Supplementary data", concept="2000", owners=["42"])
    census = _write_jsonl(tmp_path / "census.jsonl", [zc.slim_hit(hit)])
    seeds = sg.Seeds()
    seeds.owners = {"42"}
    zs.score(census, tmp_path / "scored.jsonl", excl=zs.Exclusions(), seeds=seeds)
    sess = FakeRangeSession({url("2001", "data.zip"): vz, url("2001", "more.tar.gz"): tg,
                             url("2001", "notes.txt"): b"hello"})
    keep = tmp_path / "keep.jsonl"
    zt.triage(tmp_path / "scored.jsonl", census, keep, session_factory=lambda: sess,
              residual_sample=0, negative_sample=0, interval=0)
    man = tmp_path / "manifests"
    f = zfetch.fetch(keep, out_path=man / "fetched.jsonl", raw_dir=tmp_path / "raw",
                     rejections_path=man / "rej.jsonl", max_bytes=None, workers=1,
                     session_factory=lambda: sess)
    assert f["calc_units"] == 2
    ds = tmp_path / "dataset"
    p = parse(man / "fetched.jsonl", dataset_dir=ds, raw_dir=tmp_path / "raw",
              rejections_path=ds / "rejections.jsonl")
    assert p["calcs_parsed"] == 2 and p["frames"] > 0
    assert verify_dataset(ds)["ok"]
    metas = list(read_jsonl(ds / "metadata.jsonl"))
    assert sorted(m["calc_id"] for m in metas) == [
        "zenodo:2001:data/relax/vasprun.xml", "zenodo:2001:more/neb/OUTCAR"]
    prov = metas[0]["provenance"]
    assert prov["source"] == "zenodo" and prov["record_id"] == "2001"
    assert prov["conceptrecid"] == "2000" and prov["doi"] == "10.5281/zenodo.2001"
    assert prov["license"] == "cc-by-4.0" and prov["resource_type"] == "dataset"


# --------------------------------------------------------------------------- #
# regression tests for the 2026-09-25 review findings                          #
# --------------------------------------------------------------------------- #

def test_hyphenated_and_dashed_terms_match():
    t = sg.clean_text("VASP-based screening; DFT-computed energies; first\u2010principles; "
                      "density\u2011functional; defect-driven; catalytic CO2 reduction")
    assert sg.VASP_RE.search(t)
    assert {sg.term_stem(m.group(0)) for m in sg.DFT_RE.finditer(t)} >= {
        "dft", "first-principle".replace("-", " "), "density-functional".replace("-", " ")}
    assert {m.group(0).lower() for m in sg.MATERIALS_RE.finditer(t)} >= {"defect", "catalytic"}
    assert sg.clean_text("soft\u00adhyphen") == "softhyphen"
    rec = zc.slim_hit(zhit("9", title="DFT study of catalytic CO2 reduction to mitigate climate "
                                      "change"))
    assert sg.tier_of({**sg.text_signals(rec)})[0] in ("T1", "T2")     # not T0 any more


def test_licence_other_closed_goes_to_review():
    assert sg.licence_class("other-closed") == "none"
    from zenodo_harvest.models import is_reusable_license
    assert not is_reusable_license("other-closed")
    assert is_reusable_license("other-open")


def test_formula_parser_is_linear_and_complete():
    import time as _t
    t0 = _t.monotonic()
    assert sg.formula_hits("Ox" * 60 + "_ and " + "Ox" * 60) == []
    assert _t.monotonic() - t0 < 0.5
    assert sg.formula_hits("TiOx MoSx InP PbS SrTiO3-δ") == ["TiOx", "MoSx", "InP", "PbS", "SrTiO3"]


def test_vasp_paper_dois_and_parentheses():
    meta = {"related_identifiers": [{"identifier": "10.1016/0927-0256(96)00008-0",
                                     "relation": "cites", "scheme": "doi"}],
            "references": ["G. Kresse (https://doi.org/10.1103/PhysRevB.54.11169)."]}
    dois = sg.paper_dois(meta)
    assert dois == ["10.1016/0927-0256(96)00008-0", "10.1103/physrevb.54.11169"]
    rec = zc.slim_hit(zhit("9", title="Data"))
    rec["metadata"].update(meta)
    sig = zs.record_signals(rec, sg.Seeds())
    assert sig["vasp_paper_linked"] and sg.tier_of(sig)[0] == "T1"
    assert sg.norm_doi("doi:10.1021/X.1?download=1#s") == "10.1021/x.1"
    assert sg.norm_doi("https://www.doi.org/10.1039/ABC") == "10.1039/abc"


def test_ambiguous_strong_cues_do_not_override_another_domain():
    ena = zc.slim_hit(zhit("9", title="Ena/VASP proteins in mouse fibroblasts",
                           keywords=["proteins", "mice"]))
    assert sg.tier_of(sg.text_signals(ena)) == ("T2", ["text_vasp"])
    eeg = zc.slim_hit(zhit("9", {"DFT_features.tar.gz": 5}, title="EEG recordings"))
    sig = sg.text_signals(eeg)
    assert not sig["file_vasp"] and sig["file_hint"] == ["DFT_features.tar.gz"]
    assert sg.tier_of({"dft": ["phonon", "phonons"]})[0] == "T2"        # one stem, ambiguous
    assert sg.tier_of({"mlip": ["mace"], "materials": ["oxide"]})[0] == "T2"
    assert sg.tier_of({"mlip": ["mace"], "dft": ["dft", "pbe"]})[0] == "T1"


def test_exclusions_recheck_what_the_old_triage_could_not_examine(tmp_path):
    cands = _write_jsonl(tmp_path / "candidates_full.jsonl", [
        {"recid": "1", "conceptrecid": "0", "vasp_rank": 1,
         "files": [{"key": "export.aiida", "size": 10}]},               # invisible archive
        {"recid": "3", "conceptrecid": "2", "vasp_rank": 3,
         "files": [{"key": "big.zip", "size": 300_000_000}]},           # 16-bit-count risk
        {"recid": "5", "conceptrecid": "4", "vasp_rank": 3,
         "files": [{"key": "small.zip", "size": 1000}]},                # properly peeked
        {"recid": "7", "conceptrecid": "6", "vasp_rank": 1,
         "files": [{"key": "data.txz", "size": 10}]}])                  # kept below
    keep = _write_jsonl(tmp_path / "keep.jsonl", [{"recid": "7", "conceptrecid": "6"}])
    ex = zs.Exclusions()
    ex.add_manifest(cands)
    ex.add_manifest(keep)
    assert sorted(ex.recheck) == ["1", "3"]
    assert ex.status({"id": 1, "conceptrecid": "0"}) is None
    assert ex.status({"id": 3, "conceptrecid": "2"}) is None
    assert ex.status({"id": 5, "conceptrecid": "4"}) == "evaluated"
    assert ex.status({"id": 7, "conceptrecid": "6"}) == "evaluated"


def test_cli_refuses_to_score_without_exclusion_inputs(tmp_path, monkeypatch):
    from zenodo_census import cli as zcli
    census = _write_jsonl(tmp_path / "census.jsonl", [zc.slim_hit(zhit("11"))])
    monkeypatch.setenv("ZENODO_CENSUS_DATA", str(tmp_path / "zc"))
    with pytest.raises(SystemExit):
        zcli.main(["score", "--census", str(census), "--dataset-metadata",
                   str(tmp_path / "missing.jsonl"), "--exclude-manifest",
                   str(tmp_path / "missing_keep.jsonl")])
    rc = zcli.main(["score", "--census", str(census), "--dataset-metadata",
                    str(tmp_path / "missing.jsonl"), "--exclude-manifest",
                    str(tmp_path / "missing_keep.jsonl"), "--allow-missing-exclusions",
                    "--out", str(tmp_path / "scored.jsonl")])
    assert rc == 0 and (tmp_path / "scored.jsonl").is_file()


def test_links_cli_runs_every_channel_even_if_one_fails(tmp_path, monkeypatch):
    from zenodo_census import cli as zcli
    monkeypatch.setenv("ZENODO_CENSUS_DATA", str(tmp_path / "zc"))
    called = []

    def boom(*a: Any, **k: Any) -> dict:
        raise RuntimeError("DataCite down")

    monkeypatch.setattr(zcli, "datacite_references", boom)
    monkeypatch.setattr(zcli, "epmc_mentions", lambda *a, **k: called.append(1) or {"ok": 1})
    assert zcli.main(["links"]) == 1 and called == [1]


def test_resolve_versions_batch_size_and_zero_hit_batches(tmp_path):
    client = FakeSearchClient({})
    client.token = None                                    # type: ignore[attr-defined]
    out = tmp_path / "v.jsonl"
    r = zl.resolve_versions([str(i) for i in range(100, 160)], out, client)
    assert all(len(q[len("recid:("):-1].split(" OR ")) <= 25 for q, _ in client.queries)
    assert r["batches_without_hits"] == 3 and not out.read_text().strip()   # nothing cached


def test_census_bounds_are_fixed_by_the_first_run(tmp_path):
    from datetime import date as _date
    seen: list[tuple[Any, Any]] = []

    class C(FakeCensusClient):
        def iter_records(self, query: str, start: Any = None, end: Any = None, size: int = 25,
                         **kw: Any):
            seen.append((start, end))
            yield from super().iter_records(query, start, end, size, **kw)

    out = tmp_path / "c.jsonl"
    zc.run_census(C(_windows()), out, end=_date(2024, 1, 2))
    zc.run_census(C(_windows()), out, end=_date(2030, 5, 5))            # a later day
    assert seen[0] == seen[1] == (_date(2013, 1, 1), _date(2024, 1, 2))
    with pytest.raises(ValueError):
        zc.run_census(C(_windows()), out, query="other")


def test_truncate_torn_tail_before_appends(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"a": 1}\n{"b": 2')
    assert zc.truncate_torn_tail(p) == len('{"b": 2') and p.read_text() == '{"a": 1}\n'
    assert zc.truncate_torn_tail(p) == 0
    epmc = tmp_path / "epmc.jsonl"
    epmc.write_text('{"source_id": "PMC1", "zenodo_ids": ["5"]}\n{"source_id": "PM')
    s = ScriptedSession([(lambda u, pp: u.endswith("/search"), FakeResp(200, json_obj={
        "resultList": {"result": [{"pmcid": "PMC2"}]}, "nextCursorMark": "*"})),
        (lambda u, pp: "PMC2/fullTextXML" in u, FakeResp(200, text="zenodo.org/records/6"))])
    zl.epmc_mentions(epmc, s, interval=0)
    assert zl.load_epmc(epmc) == {"5": {"PMC1"}, "6": {"PMC2"}}


def test_epmc_coverage_maps_every_id_to_its_concept(tmp_path):
    census = _write_jsonl(tmp_path / "census.jsonl", [
        zc.slim_hit(zhit("102", {"a.zip": 5}, title="Data", concept="100"))])
    ex = zs.Exclusions()
    ex.add_dataset(_dataset_meta(tmp_path, [("101", "100")]))     # an OLDER version is harvested
    epmc = {"102": {"PMC1"}, "100": {"PMC1"}}                       # newest version + concept
    rep = zs.score(census, tmp_path / "scored.jsonl", excl=ex, seeds=sg.Seeds(), epmc=epmc)
    assert rep["epmc_coverage"] == {"in_dataset": 1}


def _xz_multistream(a: bytes, b: bytes) -> bytes:
    return lzma.compress(a) + lzma.compress(b)


def test_head_peek_never_claims_a_cut_listing_complete():
    # multi-stream xz: tarfile reads both streams, so must we
    t1 = make_tar({"a/README": b"x"})[:1024]                   # header + data, NO end blocks
    t2 = make_tar({"calc/OUTCAR": b"vasp"})
    ev, st = hp.head_peek(FakeRangeSession({"u": _xz_multistream(t1, t2)}), "u")
    assert st == "ok" and ev is not None and ev.names == ["a/README", "calc/OUTCAR"]
    # a signed-checksum header (non-ASCII name) is a valid header, not a stop
    buf = bytearray(make_tar({"données/OUTCAR": b"1"}, tarfile.GNU_FORMAT))
    hdr = bytearray(buf[:512])
    hdr[148:156] = b" " * 8
    signed = sum(b - 256 if b > 127 else b for b in hdr)
    hdr[148:156] = f"{signed:06o}\0 ".encode()
    buf[:512] = hdr
    names, end, valid = hp.walk_tar(bytes(buf))
    assert valid and names and names[0].endswith("OUTCAR")
    # a 206 without Content-Range: the size is unknown -> never "complete"
    class NoCR(FakeRangeSession):
        def get(self, u: str, headers: dict | None = None, **kw: Any) -> FakeResp:
            r = super().get(u, headers, **kw)
            r.headers.pop("Content-Range", None)
            return r
    ev, st = hp.head_peek(NoCR({"u": make_tar({"x.txt": b"1"})}), "u")
    assert st == "ok" and ev is not None and not ev.complete
    # a head smaller than one bzip2 block decodes nothing: no verdict, not "a single file"
    big = bz2.compress(make_tar({"f.bin": os.urandom(400_000)}))
    assert hp.head_peek(FakeRangeSession({"u": big}), "u", head_bytes=4096)[1] == "head_too_small"


def test_head_peek_zstd_output_is_bounded():
    zstandard = pytest.importorskip("zstandard")
    bomb = zstandard.ZstdCompressor().compress(make_tar({"z.bin": b"\0" * (64 << 20)}))
    data, _ = hp._decompress("zstd", bomb, 1 << 20)
    assert len(data) <= 1 << 20


def test_walk_tar_stops_on_bad_size_fields():
    buf = bytearray(make_tar({"a/INCAR": b"1", "b/OUTCAR": b"2"}))
    second = 1024                                           # header 2 after 512 hdr + 512 data
    hdr = bytearray(buf[second:second + 512])
    hdr[124:136] = b"-0000001000\0"                        # a negative size
    hdr[148:156] = b" " * 8
    hdr[148:156] = f"{sum(hdr):06o}\0 ".encode()
    buf[second:second + 512] = hdr
    names, end, valid = hp.walk_tar(bytes(buf))             # returns (no infinite loop)
    assert names == ["a/INCAR"] and not end


def test_head_evidence_uses_strict_names():
    e = hp.names_evidence(["x/incarnation.txt", "models/chgnet_0.3.0.pt", "calc/CHGCAR",
                           "calc/INCAR", "calc/OUTCAR"])
    assert (e["n_vasp_named"], e["n_heavy"], e["n_primary"]) == (2, 1, 1)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        ti = tarfile.TarInfo("link_to_OUTCAR")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "OUTCAR"
        tf.addfile(ti)
    assert hp.walk_tar(buf.getvalue())[0] == []              # links are not listed


def test_triage_declares_real_extractors_and_skips_by_concept(tmp_path, monkeypatch):
    monkeypatch.setattr(hp.time, "sleep", lambda *_: None)     # the 503s below back off
    ok = {"status": "ok", "mode": "zip"}
    st, ff = zt.file_verdict(_f("export.zip"), {**ok, "n_primary": 2, "aiida_format": "sqlite_zip",
                                                 "db_status": "ok"})
    assert st == "vasp" and ff and ff["archive_kind"] == "aiida"
    st, ff = zt.file_verdict(_f("data.tar.gz"), {"mode": "head", "status": "is_7z",
                                                  "as_kind": "sevenzip"})
    assert st == "unresolved" and ff and ff["archive_kind"] == "sevenzip"
    assert zt._mode(_f("calc.tbz")) == "head"
    # a plain zip holding a stray db.sqlite3: judged by its member names, and cacheable
    z = make_zip({"calc/vasprun.xml": b"<modeling/>", "db.sqlite3": b"not a database",
                  "repo/abc": b"x"})
    f = {"key": "data.zip", "size": len(z), "links": {"self": "u"}}
    v = zt.peek_file(FakeRangeSession({"u": z}), "zip", f)
    assert v["status"] == "ok" and v["n_primary"] == 1 and v.get("aiida_format") is None
    # a transient failure in the zip->tar fallback is never cached as "not_zip"
    class Flaky(FakeRangeSession):
        def get(self, u: str, headers: dict | None = None, **kw: Any) -> FakeResp:
            if (headers or {}).get("Range", "").startswith("bytes=0-"):
                return FakeResp(503)
            return super().get(u, headers, **kw)
    tgz = make_tar({"c/OUTCAR": b"1"})
    v = zt.peek_file(Flaky({"u": tgz}), "zip", {**f, "size": len(tgz)})
    assert v["status"].startswith("peek_failed")
    # exclude_keep by concept: a newer version of a kept record is skipped
    census, blobs, _ = _triage_fixture(tmp_path, monkeypatch)
    prior = _write_jsonl(tmp_path / "old_keep.jsonl", [{"recid": "999", "conceptrecid": "1000"}])
    rep = zt.triage(tmp_path / "scored.jsonl", census, tmp_path / "k2.jsonl",
                    session_factory=lambda: FakeRangeSession(blobs), residual_sample=0,
                    negative_sample=0, interval=0, head_bytes=1 << 20, exclude_keep=[prior])
    assert "1001" not in {r["recid"] for r in read_jsonl(tmp_path / "k2.jsonl")}
    assert rep["kept"]["records"] >= 1


def test_remote_zip_recovers_from_a_stale_size_past_the_end():
    from materials_cloud_harvest.remote_zip import peek_archive
    z = make_zip({"calc/OUTCAR": b"1"})
    ev, st = peek_archive(FakeRangeSession({"u": z}), "u", size=len(z) + (3 << 20))
    assert st == "ok" and ev is not None and len(ev.primary) == 1
