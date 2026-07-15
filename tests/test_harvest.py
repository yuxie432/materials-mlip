"""Offline unit tests for the harvest pipeline's pure logic.

No network and no pymatgen/ase — these cover the file-signal classifier, the two
VASP filename matchers (triage's ``_VASP_RE`` and fetch's ``_PARSE_RE``), the
remote-ZIP central-directory peek parser (against an in-memory zip served by a
fake Range session, including the path where the central directory falls outside
the fetched tail), and discover's newest-version dedup.

Run: ``python -m pytest tests/ -q`` from the repo root.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from hashlib import md5
from pathlib import Path

import pytest
import requests

from zenodo_harvest import client as client_mod
from zenodo_harvest import discover as discover_mod
from zenodo_harvest import fetch as fetch_mod
from zenodo_harvest import triage as triage_mod
from zenodo_harvest.client import ZenodoClient, _parse_retry_after
from zenodo_harvest.fetch import _PARSE_RE, _find_calc_units, _unit_role, _unit_tag, download_file
from zenodo_harvest.manifest import read_jsonl
from zenodo_harvest.models import (
    CATEGORY_RANK,
    VASP_PRIMARY,
    _VASP_RE,
    _ext,
    classify_files,
)
from zenodo_harvest.triage import peek_zip_filenames


# --------------------------------------------------------------------------- #
# _ext — double compression extensions                                        #
# --------------------------------------------------------------------------- #

def test_ext_handles_double_and_single():
    assert _ext("archive.tar.gz") == ".tar.gz"
    assert _ext("archive.tar.bz2") == ".tar.bz2"
    assert _ext("data.zip") == ".zip"
    assert _ext("VASPRUN.XML") == ".xml"      # case-insensitive
    assert _ext("path/to/OUTCAR") == ""       # no extension


# --------------------------------------------------------------------------- #
# classify_files — category + rank + priority ordering                        #
# --------------------------------------------------------------------------- #

def _files(*keys):
    return [{"key": k, "size": 1} for k in keys]


def test_classify_primary_vasp_direct():
    fc = classify_files(_files("calc/vasprun.xml", "calc/INCAR"))
    assert fc["category"] == "vasp_direct"
    assert fc["primary_vasp_files"] == ["calc/vasprun.xml"]
    assert fc["rank"] == CATEGORY_RANK["vasp_direct"]


def test_classify_archive():
    fc = classify_files(_files("dataset.zip"))
    assert fc["category"] == "archive"
    assert fc["archives"] == ["dataset.zip"]


def test_classify_processed_atomistic():
    fc = classify_files(_files("train.extxyz", "readme.md"))
    assert fc["category"] == "processed_atomistic"
    assert fc["processed_files"] == ["train.extxyz"]


def test_classify_unlikely():
    fc = classify_files(_files("paper.pdf", "notes.txt"))
    assert fc["category"] == "unlikely"
    assert fc["rank"] == 0


def test_classify_primary_beats_archive():
    # A record exposing a raw vasprun.xml *and* a zip ranks as vasp_direct.
    fc = classify_files(_files("run/vasprun.xml", "extra.zip"))
    assert fc["category"] == "vasp_direct"
    assert "extra.zip" in fc["archives"]


# --------------------------------------------------------------------------- #
# _VASP_RE (triage/models) — canonical names + separator-led prefixes         #
# --------------------------------------------------------------------------- #

def test_vasp_re_matches_canonical_and_prefixed():
    for name in ["vasprun.xml", "OUTCAR", "run1/OUTCAR".rsplit("/", 1)[-1],
                 "Si.vasprun.xml", "POSCAR", "vaspout.h5", "OUTCAR.gz"]:
        assert _VASP_RE.search(name), name


def test_vasp_re_primary_group():
    m = _VASP_RE.search("Si.vasprun.xml")
    assert m and m.group(1).lower() in VASP_PRIMARY


def test_vasp_re_matches_suffixed_variants():
    # harvest-error-backlog #3 FIXED: a trailing suffix/number before the extension
    # now matches, so records whose only VASP files are numbered/suffixed variants
    # are no longer dropped at triage. (Previously these were a documented gap.)
    for name in ["vasprun_1.xml", "vasprun_2.xml", "OUTCAR_final",
                 "vasprun.relax.xml", "OUTCAR.1", "OUTCAR_final.gz"]:
        assert _VASP_RE.search(name), name
    # ...while still rejecting clearly-unrelated files.
    for name in ["paper.pdf", "notes.txt", "data.zip", "README", "energy.dat", "structure.cif"]:
        assert _VASP_RE.search(name) is None, name


# --------------------------------------------------------------------------- #
# _PARSE_RE (fetch) — files worth extracting                                  #
# --------------------------------------------------------------------------- #

def test_parse_re_matches_expected():
    for name in ["vasprun.xml", "OUTCAR", "OUTCAR.gz", "vaspout.h5",
                 "INCAR", "POSCAR", "CONTCAR", "vasprun_1.xml"]:
        assert _PARSE_RE.search(name), name


def test_parse_re_rejects_heavy_and_unrelated():
    for name in ["CHGCAR", "WAVECAR", "DOSCAR", "paper.pdf"]:
        assert _PARSE_RE.search(name) is None, name


def test_parse_re_covers_triage_confirmed_names():
    # fetch must recognise every VASP name triage's _VASP_RE confirms, or it would
    # drop a record triage already accepted (harvest-error-backlog #7). These are
    # the separator-led forms the old anchored _PARSE_RE missed.
    for name in ["site1_OUTCAR", "site2_OUTCAR", "Si.vasprun.xml", "run1/OUTCAR".rsplit("/", 1)[-1]]:
        assert _VASP_RE.search(name), f"precondition: triage confirms {name}"
        assert _PARSE_RE.search(name), f"fetch must also match {name}"


def test_unit_role_detection():
    assert _unit_role("vasprun.xml") == "vasprun"
    assert _unit_role("Si.vasprun.xml") == "vasprun"
    assert _unit_role("vaspout.h5") == "vaspout"
    assert _unit_role("site1_OUTCAR") == "outcar"      # separator-led prefix
    assert _unit_role("OUTCAR") == "outcar"
    assert _unit_role("CONTCAR") == "contcar"          # not misread as outcar
    assert _unit_role("INCAR") == "incar"
    assert _unit_role("CHGCAR") is None                # heavy output, not a unit file
    assert _unit_role("vasprun.json") is None          # extension pinned for vasprun


# --------------------------------------------------------------------------- #
# peek_zip_filenames — remote ZIP central-directory reader                    #
# --------------------------------------------------------------------------- #

class _FakeRangeSession:
    """Serves HTTP Range requests over a fixed byte blob (206 responses)."""

    def __init__(self, blob: bytes):
        self.blob = blob

    def get(self, url, headers=None, timeout=None):
        rng = (headers or {})["Range"].split("=", 1)[1]
        size = len(self.blob)
        if rng.startswith("-"):                      # suffix range: bytes=-N
            n = int(rng[1:])
            start, end = max(0, size - n), size - 1
        else:                                        # explicit: bytes=start-end
            s, e = rng.split("-")
            start, end = int(s), int(e)
        chunk = self.blob[start:end + 1]

        class _Resp:
            status_code = 206
            content = chunk
            headers = {"Content-Range": f"bytes {start}-{end}/{size}"}

        return _Resp()


def _make_zip(names):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for n in names:
            zf.writestr(n, b"x" * 32)
    return buf.getvalue()


def test_peek_zip_tail_contains_cd():
    names = ["calc/vasprun.xml", "calc/OUTCAR", "calc/README"]
    blob = _make_zip(names)
    got = peek_zip_filenames("http://x/a.zip", _FakeRangeSession(blob), tail=65_536)
    assert set(got) == set(names)


def test_peek_zip_cd_outside_tail_triggers_second_range():
    # tail=40 is smaller than the central directory, so the reader must issue a
    # second Range request for the CD (exercises the else-branch in the parser).
    names = ["a/vasprun.xml", "b/OUTCAR", "c/INCAR", "d/POSCAR"]
    blob = _make_zip(names)
    got = peek_zip_filenames("http://x/a.zip", _FakeRangeSession(blob), tail=40)
    assert got is not None and set(got) == set(names)


# --------------------------------------------------------------------------- #
# Item 1 — client + zip-peek resilience (429 Retry-After, connection retry)   #
# --------------------------------------------------------------------------- #

def test_parse_retry_after_defensive():
    assert _parse_retry_after("7") == 7
    assert _parse_retry_after(None) == 5                      # header absent -> default
    assert _parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT") == 5  # HTTP-date -> default
    assert _parse_retry_after("garbage", default=3) == 3


class _Rate429ThenRange:
    """Serves ``n_429`` throttling responses (Retry-After: 0) then Range like the
    plain fake session — models a peek that is transiently rate-limited."""

    def __init__(self, blob: bytes, n_429: int = 1):
        self.remaining_429 = n_429
        self._inner = _FakeRangeSession(blob)

    def get(self, url, headers=None, timeout=None):
        if self.remaining_429 > 0:
            self.remaining_429 -= 1

            class _R:
                status_code = 429
                headers = {"Retry-After": "0"}
                content = b""

            return _R()
        return self._inner.get(url, headers=headers, timeout=timeout)


def test_peek_retries_on_429_then_succeeds(monkeypatch):
    monkeypatch.setattr(triage_mod.time, "sleep", lambda _s: None)  # keep fast
    names = ["calc/vasprun.xml", "calc/OUTCAR"]
    blob = _make_zip(names)
    got = peek_zip_filenames("http://x/a.zip", _Rate429ThenRange(blob, n_429=1))
    assert got is not None and set(got) == set(names)


class _ConnErrThenOk:
    """Raises ConnectionError ``fail_times`` times, then returns a 200 JSON body."""

    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0
        self.headers: dict = {}

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise requests.ConnectionError("boom")

        class _R:
            status_code = 200

            def raise_for_status(self_inner):
                pass

            def json(self_inner):
                return {"hits": {"total": 0, "hits": []}}

        return _R()


def test_get_retries_on_connection_error(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: None)  # keep fast
    sess = _ConnErrThenOk(fail_times=2)
    client = ZenodoClient(session=sess, min_interval=0)
    out = client._get("/api/records")
    assert out == {"hits": {"total": 0, "hits": []}}
    assert sess.calls == 3  # two failures + one success


def test_get_reraises_after_budget_exhausted(monkeypatch):
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: None)
    sess = _ConnErrThenOk(fail_times=99)  # never recovers
    client = ZenodoClient(session=sess, min_interval=0)
    with pytest.raises(requests.ConnectionError):
        client._get("/api/records", max_retries=2)
    assert sess.calls == 3  # initial attempt + 2 retries, then re-raise


# --------------------------------------------------------------------------- #
# Item 2 — fetch download 429 retry-in-place                                  #
# --------------------------------------------------------------------------- #

class _FakeStreamResp:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self._content = content
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, n):
        for i in range(0, len(self._content), n):
            yield self._content[i:i + n]


class _Stream429ThenOk:
    """A streaming session: ``n_429`` throttles (Retry-After: 0) then a 200 body."""

    def __init__(self, content: bytes, n_429: int = 1):
        self.content = content
        self.remaining_429 = n_429
        self.calls = 0

    def get(self, url, stream=False, timeout=None):
        self.calls += 1
        if self.remaining_429 > 0:
            self.remaining_429 -= 1
            return _FakeStreamResp(429, headers={"Retry-After": "0"})
        return _FakeStreamResp(200, content=self.content,
                               headers={"Content-Length": str(len(self.content))})


def test_download_file_retries_on_429_then_lands(monkeypatch, tmp_path):
    monkeypatch.setattr(fetch_mod.time, "sleep", lambda _s: None)  # keep fast
    content = b"vasprun-content-bytes"
    expected = md5(content).hexdigest()
    sess = _Stream429ThenOk(content, n_429=1)
    dest = tmp_path / "OUTCAR"
    ok, why = download_file(sess, "http://x/OUTCAR", dest, expected)
    assert ok and why == "downloaded"
    assert dest.read_bytes() == content              # file landed
    assert sess.calls == 2                            # one 429 + one success


def test_download_file_falls_through_to_http_429(monkeypatch, tmp_path):
    monkeypatch.setattr(fetch_mod.time, "sleep", lambda _s: None)
    sess = _Stream429ThenOk(b"x", n_429=99)  # never stops throttling
    dest = tmp_path / "OUTCAR"
    ok, why = download_file(sess, "http://x/OUTCAR", dest, None)
    assert not ok and why == "http_429"
    assert sess.calls == 3  # 3 attempts, all 429


# --------------------------------------------------------------------------- #
# discover — newest-version-wins dedup by conceptrecid                        #
# --------------------------------------------------------------------------- #

def _record(recid, conceptrecid, title="t"):
    return {
        "id": recid,
        "conceptrecid": conceptrecid,
        "created": "2024-01-01T00:00:00+00:00",
        "metadata": {"title": title, "resource_type": {"type": "dataset"},
                     "creators": [{"name": "A"}], "keywords": []},
        "files": [{"key": "vasprun.xml", "size": 1, "links": {"self": "http://x"}}],
        "links": {"self_html": f"https://zenodo.org/records/{recid}"},
    }


class _FakeClient:
    def __init__(self, records):
        self._records = records

    def iter_window(self, query, extra=None):
        yield from self._records


def test_discover_keeps_newest_version(tmp_path):
    # Two versions of one concept (recids 100, 200) + a distinct concept (300).
    recs = [_record(100, "50"), _record(200, "50"), _record(300, "60")]
    out = tmp_path / "candidates.jsonl"
    summary = discover_mod.discover(
        _FakeClient(recs), queries=["VASP"], out_path=out, resource_types=("dataset",)
    )
    assert summary["unique_concepts"] == 2
    kept = {line.strip() for line in out.read_text().splitlines() if line.strip()}
    recids = sorted(json.loads(x)["recid"] for x in kept)
    assert recids == ["200", "300"]        # newest of concept 50 wins


def test_discover_skips_non_open_access_right(tmp_path):
    open_rec = _record(100, "50")                              # no access_right -> treated open
    restricted = _record(200, "60")
    restricted["metadata"]["access_right"] = "restricted"      # 403s at fetch -> drop early
    out = tmp_path / "candidates.jsonl"
    summary = discover_mod.discover(
        _FakeClient([open_rec, restricted]), queries=["VASP"], out_path=out,
        resource_types=("dataset",),
    )
    assert summary["skipped_access_right"] == 1
    recids = sorted(json.loads(line)["recid"]
                    for line in out.read_text().splitlines() if line.strip())
    assert recids == ["100"]                                   # only the open (missing) record kept


# --------------------------------------------------------------------------- #
# Item 3 — discover checkpoint/resume (sidecar + sentinels, crash-safe)       #
# --------------------------------------------------------------------------- #

def _manifest_records(path):
    """Manifest rows minus the volatile ``retrieved_at`` (differs per run)."""
    return [{k: v for k, v in json.loads(line).items() if k != "retrieved_at"}
            for line in Path(path).read_text().splitlines() if line.strip()]


def _windows_vasp():
    """Two leaf date-windows for query 'VASP', with fresh record dicts each call."""
    return {
        "VASP": [
            ((date(2020, 1, 1), date(2020, 6, 30)), [_record(100, "50"), _record(101, "51")]),
            ((date(2020, 7, 1), date(2020, 12, 31)), [_record(200, "52"), _record(300, "60")]),
        ],
    }


class _FakeExhaustiveClient:
    """Simulates ``iter_records`` over fixed leaf windows, honouring the checkpoint
    hooks and logging which windows it actually paged. ``fail_at=(query, w, r)``
    raises ConnectionError just before yielding record ``r`` of window ``w`` (a
    hard crash mid-window)."""

    def __init__(self, windows_by_query, fail_at=None):
        self.windows_by_query = windows_by_query
        self.fail_at = fail_at
        self.paged_windows: list[tuple[str, str, str]] = []

    def iter_records(self, query, extra=None, should_skip=None, on_window_done=None):
        for w_idx, ((start, end), records) in enumerate(self.windows_by_query.get(query, [])):
            if should_skip is not None and should_skip(start, end):
                continue
            self.paged_windows.append((query, start.isoformat(), end.isoformat()))
            for r_idx, rec in enumerate(records):
                if self.fail_at == (query, w_idx, r_idx):
                    raise requests.ConnectionError("boom mid-window")
                yield rec
            if on_window_done is not None:
                on_window_done(start, end)


def test_discover_exhaustive_writes_sentinels_and_manifest(tmp_path):
    out = tmp_path / "candidates.jsonl"
    client = _FakeExhaustiveClient(_windows_vasp())
    summary = discover_mod.discover(client, queries=["VASP"], out_path=out,
                                    resource_types=("dataset",), exhaustive=True)
    # concepts 50->100, 51->101, 52->200, 60->300 => 4 unique concepts.
    assert summary["unique_concepts"] == 4
    sidecar = Path(str(out) + ".hits.jsonl")
    rows = list(read_jsonl(sidecar))
    windows_done = [(r["query"], r["start"], r["end"]) for r in rows if r["kind"] == "window"]
    assert windows_done == [("VASP", "2020-01-01", "2020-06-30"),
                            ("VASP", "2020-07-01", "2020-12-31")]
    assert sum(1 for r in rows if r["kind"] == "hit") == 4  # every raw hit persisted


def test_discover_exhaustive_crash_then_resume(tmp_path):
    # Reference: an uninterrupted run (separate output) to compare the manifest to.
    ref_out = tmp_path / "ref.jsonl"
    discover_mod.discover(_FakeExhaustiveClient(_windows_vasp()), queries=["VASP"],
                          out_path=ref_out, resource_types=("dataset",), exhaustive=True)

    # Crash: fail in window 1 at record 1, AFTER window 0 has completed (sentinel).
    out = tmp_path / "candidates.jsonl"
    crash = _FakeExhaustiveClient(_windows_vasp(), fail_at=("VASP", 1, 1))
    with pytest.raises(requests.ConnectionError):
        discover_mod.discover(crash, queries=["VASP"], out_path=out,
                              resource_types=("dataset",), exhaustive=True)
    sidecar = Path(str(out) + ".hits.jsonl")
    rows = list(read_jsonl(sidecar))
    assert any(r["kind"] == "window" and r["start"] == "2020-01-01" for r in rows)      # w0 done
    assert not any(r["kind"] == "window" and r["start"] == "2020-07-01" for r in rows)  # w1 not
    assert sum(1 for r in rows if r["kind"] == "hit") == 3  # 100,101 (w0) + 200 (w1 partial)

    # Resume with a healthy client: must NOT re-page the completed window 0.
    resume = _FakeExhaustiveClient(_windows_vasp())
    summary = discover_mod.discover(resume, queries=["VASP"], out_path=out,
                                    resource_types=("dataset",), exhaustive=True)
    assert ("VASP", "2020-01-01", "2020-06-30") not in resume.paged_windows
    assert ("VASP", "2020-07-01", "2020-12-31") in resume.paged_windows
    assert summary["skipped_windows"] == 1
    assert summary["resumed_concepts"] == 3  # 100,101,200 loaded from sidecar
    # Final manifest is identical to the uninterrupted run's.
    assert _manifest_records(out) == _manifest_records(ref_out)


def test_discover_fresh_ignores_and_removes_sidecar(tmp_path):
    out = tmp_path / "candidates.jsonl"
    sidecar = Path(str(out) + ".hits.jsonl")
    sidecar.write_text(json.dumps({"kind": "query", "query": "VASP"}) + "\n")
    # --fresh must ignore that pre-existing sentinel and run the query anyway.
    client = _FakeClient([_record(100, "50")])
    summary = discover_mod.discover(client, queries=["VASP"], out_path=out,
                                    resource_types=("dataset",), fresh=True)
    assert summary["skipped_queries"] == 0
    assert summary["unique_concepts"] == 1


class _NoCallClient:
    def iter_window(self, query, extra=None):
        raise AssertionError("iter_window must not be called for a completed query")


def test_discover_windowed_resume_skips_completed_query(tmp_path):
    out = tmp_path / "c.jsonl"
    s1 = discover_mod.discover(_FakeClient([_record(100, "50"), _record(200, "60")]),
                               queries=["VASP"], out_path=out, resource_types=("dataset",))
    assert s1["skipped_queries"] == 0
    sidecar = Path(str(out) + ".hits.jsonl")
    assert any(r["kind"] == "query" and r["query"] == "VASP" for r in read_jsonl(sidecar))
    # Re-run: the completed query is skipped without touching the client.
    s2 = discover_mod.discover(_NoCallClient(), queries=["VASP"], out_path=out,
                               resource_types=("dataset",))
    assert s2["skipped_queries"] == 1
    assert s2["resumed_concepts"] == 2


# --------------------------------------------------------------------------- #
# Item 4 — relative manifest paths resolve to identical calc_ids              #
# (parse imports ase, so these helpers are imported lazily to keep the rest of #
#  this module ase-free per its docstring.)                                   #
# --------------------------------------------------------------------------- #

def test_resolve_absolute_path_passes_through():
    from zenodo_harvest.parse import _resolve
    # pathlib join semantics: joining an absolute path discards the base entirely,
    # so legacy absolute manifests keep working regardless of --raw-dir.
    assert _resolve(Path("data/raw"), "/abs/extracted/vasprun.xml") == Path("/abs/extracted/vasprun.xml")


def test_calc_id_identical_for_relative_and_absolute(tmp_path):
    from zenodo_harvest.parse import _calc_id, _resolve
    recid = "17930461"

    def calc_id_for(raw_dir, local_dir, unit):
        raw_dir = Path(raw_dir)
        base_meta = {"provenance": {"record_id": recid},
                     "_extracted_root": str(_resolve(raw_dir, local_dir) / "extracted")}
        resolved = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
        return _calc_id(resolved, base_meta)

    # Old absolute manifest (raw dir /old/raw) resolved against the DEFAULT raw dir.
    abs_local = "/old/raw/17930461"
    abs_unit = {"dir": "/old/raw/17930461/extracted/calc",
                "vasprun": "/old/raw/17930461/extracted/calc/vasprun.xml",
                "outcar": "/old/raw/17930461/extracted/calc/OUTCAR"}
    cid_abs = calc_id_for("data/raw", abs_local, abs_unit)

    # New relative manifest resolved against two DIFFERENT relocated raw dirs.
    rel_local = "17930461"
    rel_unit = {"dir": "17930461/extracted/calc",
                "vasprun": "17930461/extracted/calc/vasprun.xml",
                "outcar": "17930461/extracted/calc/OUTCAR"}
    cid_rel_a = calc_id_for(tmp_path / "scratchA", rel_local, rel_unit)
    cid_rel_b = calc_id_for(tmp_path / "scratchB" / "sub", rel_local, rel_unit)

    assert cid_abs == "zenodo:17930461:calc/vasprun.xml"
    assert cid_rel_a == cid_abs   # relative resolves to the same calc_id
    assert cid_rel_b == cid_abs   # ...and relocating raw_dir doesn't change it


# --------------------------------------------------------------------------- #
# Item 5 — per-frame electronic convergence (_step_scf, pure dict cases)      #
# --------------------------------------------------------------------------- #

def test_step_scf_converged_and_magnitude():
    from zenodo_harvest.parse import _step_scf
    step = {"electronic_steps": [{"e_0_energy": -10.0}, {"e_0_energy": -10.2},
                                 {"e_0_energy": -10.20001}]}
    dE, conv = _step_scf(step, nelm=60)
    assert conv is True                                   # 3 e-steps < NELM 60
    assert dE == pytest.approx(abs(-10.20001 - -10.2))    # last two e-steps


def test_step_scf_nelm_exhausted():
    from zenodo_harvest.parse import _step_scf
    step = {"electronic_steps": [{"e_0_energy": float(i)} for i in range(5)]}
    dE, conv = _step_scf(step, nelm=5)                    # hit the cap -> not converged
    assert conv is False
    assert dE == pytest.approx(1.0)


def test_step_scf_single_estep():
    from zenodo_harvest.parse import _step_scf
    dE, conv = _step_scf({"electronic_steps": [{"e_0_energy": -1.0}]}, nelm=60)
    assert dE is None and conv is True                    # <2 e-steps -> no dE; 1 < NELM


def test_step_scf_missing_keys_and_no_esteps():
    from zenodo_harvest.parse import _step_scf
    # missing e_0_energy -> dE None, but count still decides convergence
    dE, conv = _step_scf({"electronic_steps": [{"x": 1}, {"y": 2}]}, nelm=60)
    assert dE is None and conv is True
    # no electronic steps -> both unknown (don't fabricate a verdict)
    assert _step_scf({"electronic_steps": []}, nelm=60) == (None, None)
    # NELM unknown -> converged None even with e-steps present
    dE, conv = _step_scf({"electronic_steps": [{"e_0_energy": 1.0}, {"e_0_energy": 2.0}]}, nelm=None)
    assert conv is None and dE == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# Item 6 — no-energy frame drop policy (index-preserving, count logic)        #
# --------------------------------------------------------------------------- #

def test_select_frame_steps_drops_no_energy_keeps_indices():
    from zenodo_harvest.parse import _select_frame_steps
    steps = [
        {"e_0_energy": -5.0, "electronic_steps": [], "e_fr_energy": -5.0},  # keep
        {"electronic_steps": []},                                          # drop (no energy)
        {"e_0_energy": -6.0, "electronic_steps": [], "e_fr_energy": -6.0},  # keep
    ]
    kept, dropped = _select_frame_steps(steps)
    assert dropped == 1
    assert [i for i, _e in kept] == [0, 2]          # ORIGINAL indices preserved (no renumber)
    assert [e for _i, e in kept] == [-5.0, -6.0]


def test_select_frame_steps_all_dropped():
    from zenodo_harvest.parse import _select_frame_steps
    kept, dropped = _select_frame_steps([{"electronic_steps": []}, {"electronic_steps": []}])
    assert kept == [] and dropped == 2              # all-dropped -> caller hits `no_frames`


# --------------------------------------------------------------------------- #
# _find_calc_units — one calc unit per distinct primary (flat multi-calc split) #
# --------------------------------------------------------------------------- #

def test_find_calc_units_splits_flat_multicalc(tmp_path):
    # Regression for the site1/site2 collapse: a flat dir with two independent
    # calcs (distinguished by prefix) must yield TWO units, each paired with its
    # OWN CONTCAR, and neither primary OUTCAR may be dropped.
    d = tmp_path / "extracted"
    d.mkdir()
    for n in ["site1_OUTCAR", "site1_CONTCAR", "site2_OUTCAR", "site2_CONTCAR"]:
        (d / n).write_text("x")
    units = _find_calc_units(d)
    assert len(units) == 2
    assert sorted(Path(u["outcar"]).name for u in units) == ["site1_OUTCAR", "site2_OUTCAR"]
    pairing = {Path(u["outcar"]).name: Path(u["contcar"]).name for u in units}
    assert pairing == {"site1_OUTCAR": "site1_CONTCAR", "site2_OUTCAR": "site2_CONTCAR"}


def test_find_calc_units_single_primary_dir_is_one_unit(tmp_path):
    # The common per-calc-directory layout: one primary + inputs -> exactly one unit,
    # even when an input carries an incidental tag (POSCAR-final).
    d = tmp_path / "extracted" / "relax"
    d.mkdir(parents=True)
    for n in ["INCAR", "POSCAR-final", "KPOINTS", "OUTCAR", "vasprun.xml"]:
        (d / n).write_text("x")
    units = _find_calc_units(tmp_path / "extracted")
    assert len(units) == 1
    assert Path(units[0]["vasprun"]).name == "vasprun.xml"
    assert Path(units[0]["outcar"]).name == "OUTCAR"


def test_find_calc_units_inputs_only_yields_nothing(tmp_path):
    d = tmp_path / "extracted"
    d.mkdir()
    for n in ["INCAR", "POSCAR", "KPOINTS"]:  # no OUTCAR/vasprun -> no energies/forces
        (d / n).write_text("x")
    assert _find_calc_units(d) == []


def test_unit_tag():
    assert _unit_tag("site1_OUTCAR", "outcar") == "site1"
    assert _unit_tag("vasprun.xml", "vasprun") == ""
    assert _unit_tag("vasprun_1.xml", "vasprun") == "1"


# --------------------------------------------------------------------------- #
# read_jsonl — tolerate a truncated final line (crash-safe resume)            #
# --------------------------------------------------------------------------- #

def test_read_jsonl_tolerates_truncated_final_line(tmp_path):
    p = tmp_path / "m.jsonl"
    p.write_text('{"a": 1}\n{"b": 2}\n{"c": 3')  # crash mid-append: torn last line
    assert list(read_jsonl(p)) == [{"a": 1}, {"b": 2}]


def test_read_jsonl_raises_on_malformed_nonfinal_line(tmp_path):
    p = tmp_path / "m.jsonl"
    p.write_text('{"a": 1}\n{oops not json\n{"c": 3}\n')  # genuine mid-file corruption
    with pytest.raises(json.JSONDecodeError):
        list(read_jsonl(p))
