"""Offline unit tests for the harvest pipeline's pure logic.

No network and no pymatgen/ase — these cover the file-signal classifier, the two
VASP filename matchers (triage's ``_VASP_RE`` and fetch's ``_PARSE_RE``), the
remote-ZIP central-directory peek parser (against an in-memory zip served by a
fake Range session, including the path where the central directory falls outside
the fetched tail), and discover's newest-version dedup.

Run: ``python -m pytest tests/ -q`` from the repo root.
"""

from __future__ import annotations

import errno
import io
import json
import os
import shutil
import tarfile
import threading
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
from zenodo_harvest import zipstream as zipstream_mod
from zenodo_harvest.client import ZenodoClient, _parse_retry_after
from zenodo_harvest.fetch import (
    _MULTIPART_RE,
    _PARSE_RE,
    _archive_subdir,
    _extract_tar,
    _extract_tar_zst,
    _extract_zip,
    _fetch_zip_targeted,
    _find_calc_units,
    _is_archive,
    _unit_role,
    _unit_tag,
    download_file,
    fetch_record,
)
from zenodo_harvest.manifest import RejectionLogger, read_jsonl
from zenodo_harvest.models import (
    CATEGORY_RANK,
    VASP_PRIMARY,
    _VASP_RE,
    _ext,
    classify_files,
)
from zenodo_harvest.pipeline import run_pipeline
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


def test_classify_input_only_is_low_rank():
    # A record exposing ONLY VASP input files (no vasprun/OUTCAR/vaspout, no archive)
    # has no energies/forces to train on -> vasp_input_only (rank 1), dropped by the
    # default --min-rank 3 (mentor decision 2026-07-20). Regression for the old
    # behaviour where INCAR/POSCAR-only records over-ranked as vasp_direct.
    fc = classify_files(_files("relax/POSCAR", "relax/INCAR", "relax/KPOINTS", "relax/CONTCAR"))
    assert fc["category"] == "vasp_input_only"
    assert fc["rank"] == CATEGORY_RANK["vasp_input_only"] == 1
    assert fc["primary_vasp_files"] == []


def test_classify_input_plus_archive_stays_archive():
    # Inputs exposed directly BUT an archive present -> archive (the zip may hide the
    # outputs); archive is checked before the input-only branch.
    fc = classify_files(_files("INCAR", "POSCAR", "outputs.zip"))
    assert fc["category"] == "archive"
    assert fc["rank"] == CATEGORY_RANK["archive"]


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

    def get(self, url, headers=None, timeout=None, stream=None):
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


class _UnderflowSmallFileSession:
    """Models Zenodo's suffix-range bug for a file SMALLER than the requested tail:
    the ``bytes=-N`` request (N >= size) gets a 206 whose ``Content-Range`` start has
    underflowed (``size - N`` wrapped into a huge unsigned int) and whose body is
    undeliverable (``content`` raises like a broken chunked read). A whole-file GET
    (no Range) returns 200 with the full bytes. Verifies the small-file fallback."""

    def __init__(self, blob: bytes):
        self.blob = blob
        self.suffix_reads = 0
        self.whole_reads = 0

    def get(self, url, headers=None, timeout=None, stream=None):
        size = len(self.blob)
        rng = (headers or {}).get("Range") if headers else None
        blob = self.blob
        if rng and rng.startswith("bytes=-"):
            self.suffix_reads += 1
            n = int(rng[len("bytes=-"):])
            if n >= size:                                   # <- the underflow case
                bad_start = (1 << 64) - (n - size)

                class _Broken:
                    status_code = 206
                    headers = {"Content-Range": f"bytes {bad_start}-{size - 1}/{size}"}

                    @property
                    def content(self):
                        raise requests.exceptions.ChunkedEncodingError("IncompleteRead")

                    def close(self):
                        pass

                return _Broken()
            start = size - n

            class _R:
                status_code = 206
                content = blob[start:]
                headers = {"Content-Range": f"bytes {start}-{size - 1}/{size}"}

                def close(self):
                    pass

            return _R()

        self.whole_reads += 1                               # whole-file GET (no Range)

        class _Whole:
            status_code = 200
            content = blob
            headers: dict = {}

            def close(self):
                pass

        return _Whole()


def test_peek_zip_small_file_uses_whole_file_fallback():
    # A zip smaller than `tail` triggers Zenodo's suffix-range underflow; the peeker
    # must detect it from Content-Range and re-fetch the whole (small) file.
    names = ["run/OUTCAR", "run/POSCAR"]
    blob = _make_zip(names)
    assert len(blob) < 65_536                              # precondition: small file
    sess = _UnderflowSmallFileSession(blob)
    got = peek_zip_filenames("http://x/small.zip", sess, tail=65_536)
    assert got is not None and set(got) == set(names)
    assert sess.whole_reads == 1                           # took the whole-file path


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

    def get(self, url, headers=None, timeout=None, stream=None):
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


# --------------------------------------------------------------------------- #
# triage driver — peek ON by default + FAIL-SAFE drop of proven-no-VASP zips   #
# (peeking a zip's central directory costs ~KB; downloading it costs MB–GB, so #
#  peek is default and a positive "no VASP" peek drops the record before fetch) #
# --------------------------------------------------------------------------- #

def _cand(recid, files, category="archive", rank=3, primary=None):
    return {"recid": str(recid), "conceptrecid": str(recid), "vasp_category": category,
            "vasp_rank": rank, "primary_vasp_files": primary or [], "vasp_files": [],
            "archives": [f["key"] for f in files], "processed_files": [], "signals": [],
            "files": files}


def _zipfile(key, size=1000):
    return {"key": key, "ext": ".zip", "size": size, "download": f"http://x/{key}"}


def _write_manifest(tmp_path, cands):
    p = tmp_path / "cands.jsonl"
    p.write_text("".join(json.dumps(c) + "\n" for c in cands))
    return p


def _patch_peek(monkeypatch, mapping):
    """Patch triage.peek_zip_filenames to return mapping[url] (list, [], or None)."""
    monkeypatch.setattr(triage_mod, "peek_zip_filenames",
                        lambda url, session=None, tail=65_536: mapping.get(url))
    monkeypatch.setattr(triage_mod.time, "sleep", lambda _s: None)


def test_triage_peek_upgrades_and_keeps_vasp_zip(tmp_path, monkeypatch):
    _patch_peek(monkeypatch, {"http://x/d.zip": ["run/vasprun.xml", "run/OUTCAR"]})
    src = _write_manifest(tmp_path, [_cand(1, [_zipfile("d.zip")])])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, peek_interval=0)
    assert stats["kept"] == 1 and stats["peek_confirmed"] == 1 and stats["dropped_no_vasp"] == 0
    kept = json.loads(out.read_text())
    assert kept["vasp_category"] == "vasp_direct" and kept["primary_vasp_files"]


def test_triage_drops_zip_proven_no_vasp(tmp_path, monkeypatch):
    # zip peeked OK, contains no VASP file at all -> positive evidence -> DROP by default.
    _patch_peek(monkeypatch, {"http://x/d.zip": ["data/foo.csv", "readme.txt"]})
    src = _write_manifest(tmp_path, [_cand(1, [_zipfile("d.zip")])])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, peek_interval=0)
    assert stats["kept"] == 0 and stats["dropped_no_vasp"] == 1


def test_triage_failsafe_keeps_on_peek_failure(tmp_path, monkeypatch):
    # peek returns None (throttled/ZIP64/network) -> NOT positive evidence -> KEEP.
    _patch_peek(monkeypatch, {"http://x/d.zip": None})
    src = _write_manifest(tmp_path, [_cand(1, [_zipfile("d.zip")])])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, peek_interval=0)
    assert stats["kept"] == 1 and stats["dropped_no_vasp"] == 0


def test_triage_failsafe_keeps_when_unpeekable_archive_present(tmp_path, monkeypatch):
    # a .tar.gz can't be range-peeked; even if the sibling zip has no VASP, keep the
    # record (fetch confirms the tar at download) rather than dropping blind.
    _patch_peek(monkeypatch, {"http://x/d.zip": ["notes.txt"]})
    files = [_zipfile("d.zip"), {"key": "e.tar.gz", "ext": ".tar.gz", "size": 10,
                                 "download": "http://x/e.tar.gz"}]
    src = _write_manifest(tmp_path, [_cand(1, files)])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, peek_interval=0)
    assert stats["kept"] == 1 and stats["dropped_no_vasp"] == 0


def test_triage_keep_unconfirmed_disables_drop(tmp_path, monkeypatch):
    _patch_peek(monkeypatch, {"http://x/d.zip": ["data/foo.csv"]})
    src = _write_manifest(tmp_path, [_cand(1, [_zipfile("d.zip")])])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, require_confirmed=False, peek_interval=0)
    assert stats["kept"] == 1 and stats["dropped_no_vasp"] == 0


def test_triage_no_peek_keeps_all_without_network(tmp_path, monkeypatch):
    # peek=False must not call peek_zip_filenames at all, and keeps every rank>=min.
    def _boom(*a, **k):
        raise AssertionError("peek must not run when peek=False")
    monkeypatch.setattr(triage_mod, "peek_zip_filenames", _boom)
    src = _write_manifest(tmp_path, [_cand(1, [_zipfile("d.zip")])])
    out = tmp_path / "keep.jsonl"
    stats = triage_mod.triage(src, out, peek=False, peek_interval=0)
    assert stats["kept"] == 1 and stats["peeked"] == 0


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


def _parse_created_window(ranged: str):
    """Pull (start, end) datetimes out of a `... created:[A TO B}` query string."""
    import re
    from datetime import datetime
    m = re.search(r"created:\[(\S+) TO (\S+)\}", ranged)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(m.group(1), fmt), datetime.strptime(m.group(2), fmt)


def test_iter_records_bisects_below_one_day(monkeypatch):
    # A single dense day (>10k) must be split by datetime until each leaf is <=10k,
    # NOT truncated. Stub count(): any window wider than 1 hour reports 20000; a window
    # <=1 hour reports 100. Stub iter_window() to emit one record per leaf and record
    # the leaf window bounds. Assert: below-a-day leaves, disjoint+gapless coverage,
    # termination, and no truncation warning.
    from datetime import timedelta
    client = ZenodoClient(min_interval=0)
    leaves: list[tuple] = []

    def fake_count(ranged, extra=None):
        s, e = _parse_created_window(ranged)
        return 20000 if (e - s) > timedelta(hours=1) else 100

    def fake_iter_window(ranged, size=25, sort="newest", extra=None):
        s, e = _parse_created_window(ranged)
        leaves.append((s, e))
        yield {"id": f"{s.isoformat()}"}

    monkeypatch.setattr(client, "count", fake_count)
    monkeypatch.setattr(client, "iter_window", fake_iter_window)

    from datetime import date
    recs = list(client.iter_records("VASP", start=date(2017, 10, 23), end=date(2017, 10, 23)))
    # one full day split into <=1h leaves -> at least 24 leaves, all <= 1h wide
    assert len(recs) >= 24
    assert all((e - s) <= timedelta(hours=1) for s, e in leaves)
    # disjoint + gapless + sorted: each leaf's start == previous leaf's end
    leaves.sort()
    for (s0, e0), (s1, e1) in zip(leaves, leaves[1:]):
        assert e0 == s1, f"gap/overlap between {e0} and {s1}"


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
        self.break_after: int | None = None  # bytes to yield before dying mid-stream

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, n):
        sent = 0
        for i in range(0, len(self._content), n):
            chunk = self._content[i:i + n]
            if self.break_after is not None and sent + len(chunk) > self.break_after:
                yield chunk[: self.break_after - sent]
                raise requests.RequestException("connection dropped mid-stream")
            yield chunk
            sent += len(chunk)

    @property
    def content(self):
        return self._content

    def close(self):
        pass


class _Stream429ThenOk:
    """A streaming session: ``n_429`` throttles (Retry-After: 0) then a 200 body."""

    def __init__(self, content: bytes, n_429: int = 1):
        self.content = content
        self.remaining_429 = n_429
        self.calls = 0

    def get(self, url, stream=False, timeout=None, headers=None):
        self.calls += 1
        if self.remaining_429 > 0:
            self.remaining_429 -= 1
            return _FakeStreamResp(429, headers={"Retry-After": "0"})
        return _FakeStreamResp(200, content=self.content,
                               headers={"Content-Length": str(len(self.content))})


def _parse_range(headers):
    """Byte offset requested by a ``Range: bytes=N-`` header (0 if absent)."""
    rng = (headers or {}).get("Range")
    return int(rng.split("=", 1)[1].split("-", 1)[0]) if rng else 0


class _RangeSession:
    """Streaming session that honours ``Range: bytes=N-`` like Zenodo does.

    Optionally truncates the first response after ``break_after`` bytes (raising a
    ``RequestException`` mid-stream) to model a transfer killed part way — the case
    byte-range resume exists for. Records every requested offset in ``offsets``.
    """

    def __init__(self, content: bytes, break_after: int | None = None,
                 ignore_range: bool = False, unsatisfiable: bool = False):
        self.content = content
        self.break_after = break_after
        self.ignore_range = ignore_range
        self.unsatisfiable = unsatisfiable
        self.offsets: list[int] = []

    def get(self, url, stream=False, timeout=None, headers=None):
        start = _parse_range(headers)
        self.offsets.append(start)
        if start and self.unsatisfiable:
            return _FakeStreamResp(416)
        if start and not self.ignore_range:
            body = self.content[start:]
            return _FakeStreamResp(206, content=body,
                                   headers={"Content-Length": str(len(body))})
        body = self.content  # fresh download, or a server that ignored the Range
        resp = _FakeStreamResp(200, content=body,
                               headers={"Content-Length": str(len(body))})
        if self.break_after is not None:
            resp.break_after, self.break_after = self.break_after, None
        return resp


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
# download resume — a killed transfer must not restart from byte 0            #
# (a CSD3 job's 36h wallclock can expire mid-pull of a >100 GB archive)       #
# --------------------------------------------------------------------------- #

_RESUME_BLOB = bytes(range(256)) * 8_000          # ~2 MB, > the 1 MiB chunk size


def test_download_resumes_from_part_file(tmp_path):
    # First pass dies mid-stream leaving a .part; the second pass must Range-resume
    # from exactly those bytes and land the complete, checksum-correct file.
    expected = md5(_RESUME_BLOB).hexdigest()
    dest = tmp_path / "big.zip"
    sess = _RangeSession(_RESUME_BLOB, break_after=1 << 20)

    ok, why = download_file(sess, "http://x/big.zip", dest, expected)
    assert not ok and why.startswith("download_error")
    part = dest.with_suffix(dest.suffix + ".part")
    assert part.is_file() and 0 < part.stat().st_size < len(_RESUME_BLOB)   # partial kept
    resumed_from = part.stat().st_size

    ok, why = download_file(sess, "http://x/big.zip", dest, expected)
    assert ok and why == "resumed"
    assert dest.read_bytes() == _RESUME_BLOB                                # byte-exact
    assert not part.exists()
    assert sess.offsets == [0, resumed_from]                        # asked for the rest


def test_download_resume_restarts_when_server_ignores_range(tmp_path):
    # A server that answers 200 to a Range request must be handled by restarting from
    # scratch (truncate, not append — appending would corrupt the file).
    expected = md5(_RESUME_BLOB).hexdigest()
    dest = tmp_path / "big.zip"
    part = dest.with_suffix(dest.suffix + ".part")
    part.write_bytes(b"stale-prefix")                     # leftover from an older run

    sess = _RangeSession(_RESUME_BLOB, ignore_range=True)
    ok, why = download_file(sess, "http://x/big.zip", dest, expected)
    assert ok and why == "downloaded"
    assert dest.read_bytes() == _RESUME_BLOB                     # no stale prefix survived


def test_download_resume_416_means_part_already_complete(tmp_path):
    # 416 Range Not Satisfiable => the .part already holds the whole file; the checksum
    # confirms it and no bytes are re-transferred.
    expected = md5(_RESUME_BLOB).hexdigest()
    dest = tmp_path / "big.zip"
    dest.with_suffix(dest.suffix + ".part").write_bytes(_RESUME_BLOB)
    sess = _RangeSession(_RESUME_BLOB, unsatisfiable=True)

    ok, why = download_file(sess, "http://x/big.zip", dest, expected)
    assert ok and why == "resumed"
    assert dest.read_bytes() == _RESUME_BLOB


def test_download_resume_discards_wrong_bytes_via_checksum(tmp_path):
    # A .part from a DIFFERENT (superseded) file version must never survive: resuming
    # appends, the md5 fails, and the bad partial is deleted so the next run is clean.
    dest = tmp_path / "big.zip"
    part = dest.with_suffix(dest.suffix + ".part")
    part.write_bytes(b"bytes-of-some-other-file")
    sess = _RangeSession(_RESUME_BLOB)

    ok, why = download_file(sess, "http://x/big.zip", dest, md5(_RESUME_BLOB).hexdigest())
    assert not ok and why == "md5_mismatch"
    assert not part.exists() and not dest.exists()


def test_download_no_resume_without_checksum(tmp_path):
    # With no expected md5 a bad resume would be undetectable, so resume is disabled
    # and the partial is discarded rather than appended to.
    dest = tmp_path / "OUTCAR"
    part = dest.with_suffix(dest.suffix + ".part")
    part.write_bytes(b"partial")
    sess = _RangeSession(_RESUME_BLOB)

    ok, why = download_file(sess, "http://x/OUTCAR", dest, None)
    assert ok and why == "downloaded"
    assert sess.offsets == [0]                 # never asked for a range
    assert dest.read_bytes() == _RESUME_BLOB


def test_download_resume_size_cap_counts_bytes_already_have(tmp_path):
    # Under a 206 the Content-Length is only the REMAINDER, so the cap must be applied
    # to have+remaining or a resumed download could sail past --max-bytes.
    expected = md5(_RESUME_BLOB).hexdigest()
    dest = tmp_path / "big.zip"
    part = dest.with_suffix(dest.suffix + ".part")
    part.write_bytes(_RESUME_BLOB[: len(_RESUME_BLOB) // 2])
    sess = _RangeSession(_RESUME_BLOB)

    ok, why = download_file(sess, "http://x/big.zip", dest, expected,
                            max_bytes=len(_RESUME_BLOB) - 1)
    assert not ok and why == "over_size_cap"


# --------------------------------------------------------------------------- #
# discover — newest-version-wins dedup by conceptrecid                        #
# --------------------------------------------------------------------------- #

def _record(recid, conceptrecid, title="t", license="cc-by-4.0"):
    meta = {"title": title, "resource_type": {"type": "dataset"},
            "creators": [{"name": "A"}], "keywords": []}
    if license is not None:  # reusable by default so the license gate keeps these
        meta["license"] = {"id": license}
    return {
        "id": recid,
        "conceptrecid": conceptrecid,
        "created": "2024-01-01T00:00:00+00:00",
        "metadata": meta,
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


def test_is_reusable_license():
    from zenodo_harvest.models import is_reusable_license
    # collect: CC0/CC-BY/CC-BY-SA + Zenodo's other-open + permissive software licenses
    for lic in ["cc-by-4.0", "cc-by-3.0", "cc-by-sa-4.0", "cc-zero", "cc0-1.0",
                "other-open", "mit-license", "apache-2.0", "bsd-3-clause",
                "gpl-3.0-or-later", "apgl-v3"]:
        assert is_reusable_license(lic), lic
    # reject: NonCommercial, NoDerivatives, and no-usable-license
    for lic in ["cc-by-nc-4.0", "cc-by-nc-sa-4.0", "cc-by-nd-4.0", "cc-by-nc-nd-4.0",
                None, "notspecified", "all-rights-reserved", "closed"]:
        assert not is_reusable_license(lic), lic


def test_discover_license_gate(tmp_path):
    # Default gate ON: NC/ND/no-license records dropped, reusable ones kept.
    recs = [_record(100, "50", license="cc-by-4.0"),        # keep
            _record(200, "60", license="cc-by-nc-4.0"),     # drop (NonCommercial)
            _record(300, "70", license="cc-by-nd-4.0"),     # drop (NoDerivatives)
            _record(400, "80", license=None)]               # drop (no license)
    out = tmp_path / "candidates.jsonl"
    summary = discover_mod.discover(_FakeClient(recs), queries=["VASP"], out_path=out,
                                    resource_types=("dataset",))
    assert summary["unique_concepts"] == 1
    assert summary["skipped_license"] == 3
    recids = sorted(json.loads(line)["recid"]
                    for line in out.read_text().splitlines() if line.strip())
    assert recids == ["100"]
    # ...and --no-license-gate keeps them all.
    out2 = tmp_path / "c2.jsonl"
    s2 = discover_mod.discover(_FakeClient(recs), queries=["VASP"], out_path=out2,
                               resource_types=("dataset",), license_gate=False)
    assert s2["unique_concepts"] == 4 and s2["skipped_license"] == 0


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


def test_entropy_ts():
    from zenodo_harvest.parse import _entropy_ts
    # T*S = E - F = e_wo_entrp - e_fr_energy (>= 0; F is the lower, force-consistent one)
    assert _entropy_ts({"e_wo_entrp": -100.40, "e_fr_energy": -100.50}) == pytest.approx(0.10)
    assert _entropy_ts({"e_wo_entrp": -5.0, "e_fr_energy": -5.0}) == 0.0   # tetrahedron: no entropy
    assert _entropy_ts({"e_fr_energy": -5.0}) is None                      # OUTCAR path: no E
    assert _entropy_ts({"e_wo_entrp": -5.0}) is None
    assert _entropy_ts({}) is None


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


# --------------------------------------------------------------------------- #
# Archive extractors — zip-slip guard, member cap, per-archive subdirs        #
# (stdlib only; the pymatgen/ase parse of the extracted files is a later stage)#
# --------------------------------------------------------------------------- #

def _zip_bytes(items):
    """Build an in-memory zip from ``[(name, content_bytes), ...]``."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in items:
            zf.writestr(name, content)
    return buf.getvalue()


def _tar_bytes(items):
    """Build an in-memory tar from ``[(name, content_bytes), ...]``."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, content in items:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


# members shared by the zip and tar extractor tests: a good OUTCAR, an oversized
# member, and a zip-slip member that must never escape the destination dir.
_GOOD = b"outcar content"
_BIG = b"x" * 8192
_EXTRACT_ITEMS = [
    ("good/OUTCAR", _GOOD),           # small + VASP name -> extracted
    ("toobig/vasprun.xml", _BIG),     # VASP name but over the member cap -> skipped
    ("../evil_OUTCAR", b"pwned"),     # path traversal -> skipped, must not escape dest
]
_MEMBER_CAP = 4096


def test_extract_zip_skips_slip_and_oversize_keeps_good(tmp_path):
    arc = tmp_path / "a.zip"
    arc.write_bytes(_zip_bytes(_EXTRACT_ITEMS))
    dest = tmp_path / "extracted"
    names, extracted = _extract_zip(arc, dest, _MEMBER_CAP)
    assert extracted == ["good/OUTCAR"]                       # only the safe, in-cap file
    assert (dest / "good" / "OUTCAR").read_bytes() == _GOOD
    assert not (dest / "toobig" / "vasprun.xml").exists()     # oversized skipped
    assert not (tmp_path / "evil_OUTCAR").exists()            # traversal target never written
    assert not (dest.parent / "evil_OUTCAR").exists()


def test_extract_tar_skips_slip_and_oversize_keeps_good(tmp_path):
    arc = tmp_path / "a.tar"
    arc.write_bytes(_tar_bytes(_EXTRACT_ITEMS))
    dest = tmp_path / "extracted"
    names, extracted = _extract_tar(arc, dest, _MEMBER_CAP)
    assert extracted == ["good/OUTCAR"]
    assert (dest / "good" / "OUTCAR").read_bytes() == _GOOD
    assert not (dest / "toobig" / "vasprun.xml").exists()
    assert not (tmp_path / "evil_OUTCAR").exists()
    assert not (dest.parent / "evil_OUTCAR").exists()


def _tar_zst_bytes(items):
    """Build an in-memory zstd-compressed tar from ``[(name, content), ...]``."""
    import zstandard
    return zstandard.ZstdCompressor().compress(_tar_bytes(items))


def test_is_archive_recognizes_zstd_tarballs_and_bare_zst():
    assert _is_archive("training_data.tar.zst") == "tarzst"
    assert _is_archive("run.tzst") == "tarzst"
    assert _is_archive("Research_Data.zst") == "tarzst"      # bare .zst, non-VASP stem
    # a single compressed VASP file stays a direct download, NOT a tarball
    assert _is_archive("OUTCAR.zst") is None
    assert _is_archive("vasprun.xml.zst") is None
    # unchanged behaviour for the other formats
    assert _is_archive("a.zip") == "zip"
    assert _is_archive("a.tar.gz") == "tar"
    assert _is_archive("a.7z") == "sevenzip"
    assert _is_archive("plain.txt") is None


def test_extract_tar_zst_roundtrip_skips_slip_and_oversize(tmp_path):
    # Regression: `.tar.zst` archives were discovered as `archive` but fetch never
    # unpacked them (silent data miss of ~100+ GB of VASP data on Zenodo).
    arc = tmp_path / "a.tar.zst"
    arc.write_bytes(_tar_zst_bytes(_EXTRACT_ITEMS))
    dest = tmp_path / "extracted"
    names, extracted = _extract_tar_zst(arc, dest, _MEMBER_CAP)
    assert extracted == ["good/OUTCAR"]
    assert (dest / "good" / "OUTCAR").read_bytes() == _GOOD
    assert not (dest / "toobig" / "vasprun.xml").exists()     # oversized skipped
    assert not (tmp_path / "evil_OUTCAR").exists()            # traversal target never written


def test_archive_subdir_strips_zstd_suffixes():
    assert _archive_subdir("training_data.tar.zst") == "training_data"
    assert _archive_subdir("run.tzst") == "run"
    assert _archive_subdir("Research_Data.zst") == "Research_Data"


def test_multipart_regex_matches_split_archive_parts():
    for name in ["vasp_data.z01", "vasp_data.z09", "data.r01", "big.part1.rar"]:
        assert _MULTIPART_RE.search(name), name
    for name in ["data.zip", "data.7z", "OUTCAR", "notes.z1"]:  # z1 is not a 2-digit part
        assert _MULTIPART_RE.search(name) is None, name


def test_archive_subdir_strips_suffix_and_sanitizes():
    assert _archive_subdir("data.zip") == "data"
    assert _archive_subdir("run.tar.gz") == "run"
    assert _archive_subdir("archive.tar.bz2") == "archive"
    assert _archive_subdir("bundle.tgz") == "bundle"
    assert _archive_subdir("stuff.7z") == "stuff"
    assert _archive_subdir("weird name!.zip") == "weird_name_"   # non-safe chars -> _
    assert _archive_subdir(".zip") == "archive"                  # empty stem -> fallback


def test_two_archives_same_member_extract_to_distinct_subdirs(tmp_path):
    # Two archives sharing member path "calc/OUTCAR" must not clobber each other:
    # each extracts under extracted/<archive-stem>/.
    root = tmp_path / "extracted"
    (tmp_path / "a.zip").write_bytes(_zip_bytes([("calc/OUTCAR", b"first")]))
    (tmp_path / "b.zip").write_bytes(_zip_bytes([("calc/OUTCAR", b"second")]))
    _extract_zip(tmp_path / "a.zip", root / _archive_subdir("a.zip"), 1 << 20)
    _extract_zip(tmp_path / "b.zip", root / _archive_subdir("b.zip"), 1 << 20)
    assert (root / "a" / "calc" / "OUTCAR").read_bytes() == b"first"
    assert (root / "b" / "calc" / "OUTCAR").read_bytes() == b"second"


class _MultiFileStreamSession:
    """Streaming session serving a fixed ``{url: bytes}`` map (200 responses)."""

    def __init__(self, blobs):
        self.blobs = blobs

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, stream=False, timeout=None, headers=None):
        content = self.blobs[url]
        return _FakeStreamResp(200, content=content,
                               headers={"Content-Length": str(len(content))})


def test_fetch_record_two_zips_same_member_yield_two_units(tmp_path):
    # End-to-end at the record level: two archives with the same internal member
    # path extract into distinct subdirs and produce two independent calc units.
    blob_a = _zip_bytes([("calc/OUTCAR", b"first-outcar")])
    blob_b = _zip_bytes([("calc/OUTCAR", b"second-outcar")])
    rec = {
        "recid": "555",
        "files": [
            {"key": "a.zip", "download": "http://x/a.zip", "size": len(blob_a),
             "checksum": "md5:" + md5(blob_a).hexdigest()},
            {"key": "b.zip", "download": "http://x/b.zip", "size": len(blob_b),
             "checksum": "md5:" + md5(blob_b).hexdigest()},
        ],
    }
    session = _MultiFileStreamSession({"http://x/a.zip": blob_a, "http://x/b.zip": blob_b})
    raw_dir = tmp_path / "raw"
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, session, raw_dir, max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 2
    # the two OUTCARs live under distinct per-archive subdirs (relative to raw_dir)
    outcar_dirs = sorted(Path(u["outcar"]).parent.parent.name for u in entry["calc_units"])
    assert outcar_dirs == ["a", "b"]


class _DummySession:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass


def _write_recs(path, n, size):
    with path.open("w") as fh:
        for i in range(n):
            fh.write(json.dumps({"recid": str(i),
                                 "files": [{"key": "a.zip", "size": size, "download": "u"}]}) + "\n")


def _staging_fetch_record(per_record):
    def fake(rec, session, rd, max_bytes, rej, max_member_bytes=0, budget=None,
             zip_stream=True, zip_stream_max_files=128):
        d = Path(rd) / rec["recid"] / "extracted"
        d.mkdir(parents=True, exist_ok=True)
        (d / "OUTCAR").write_bytes(b"x" * per_record)
        rej.reject("fetch", rec["recid"] + ":note", "parallel_probe")  # exercise shared rej
        return {"recid": rec["recid"], "n_calc_units": 1, "calc_units": []}
    return fake


def test_dir_usage_survives_concurrent_deletion(tmp_path, monkeypatch):
    # In the overlapped `pipeline`, fetch baselines raw_dir for batch i+1 WHILE purge-raw
    # for batch i deletes trees under the same raw_dir. _dir_usage must not crash when an
    # entry vanishes between being listed and being stat()'d — otherwise a racing purge
    # aborts the foreground fetch. Simulate a file that disappears mid-walk.
    root = tmp_path / "raw"
    (root / "recA").mkdir(parents=True)
    (root / "recB").mkdir(parents=True)
    (root / "recA" / "OUTCAR").write_bytes(b"x" * 100)
    doomed = root / "recB" / "vasprun.xml"
    doomed.write_bytes(b"y" * 200)

    real_stat = os.stat

    def flaky_stat(path, *a, **k):
        if str(path) == str(doomed):
            doomed.unlink(missing_ok=True)           # vanish exactly as it is stat()'d
            return real_stat(path, *a, **k)          # -> raises FileNotFoundError
        return real_stat(path, *a, **k)

    monkeypatch.setattr(fetch_mod.os, "stat", flaky_stat)
    total, count = fetch_mod._dir_usage(root)         # must not raise
    assert total == 100                               # the survivor's bytes still counted
    # two record dirs counted as inodes; the vanished file is simply skipped
    assert count == 2 + 1                             # recA, recB, + surviving OUTCAR


def test_env_data_root_from_dotenv_is_honoured(tmp_path, monkeypatch):
    # A ZENODO_HARVEST_DATA that lives ONLY in .env (not exported) must still redirect the
    # data dirs. Before refresh_paths the constants stayed bound to their import-time
    # default `data/` — on CSD3 that is /home's 50 GB *backed-up* quota, not the 1 TB
    # scratch, so a full harvest would silently fill it and fail.
    from zenodo_harvest import config
    monkeypatch.delenv("ZENODO_HARVEST_DATA", raising=False)
    scratch = tmp_path / "rds" / "hpc-work" / "zenodo"
    (tmp_path / ".env").write_text(f"ZENODO_HARVEST_DATA={scratch}\n")
    saved = (config.DATA_ROOT, config.MANIFEST_DIR, config.RAW_DIR, config.DATASET_DIR)
    try:
        config.load_dotenv(tmp_path / ".env")
        assert os.environ.get("ZENODO_HARVEST_DATA") == str(scratch)  # .env populated env
        config.refresh_paths()
        assert config.RAW_DIR == scratch / "raw"
        assert config.DATASET_DIR == scratch / "dataset"
        assert config.MANIFEST_DIR == scratch / "manifests"
    finally:
        # config globals + the env var are process-wide: restore them so later tests that
        # fall back to config defaults are unaffected.
        os.environ.pop("ZENODO_HARVEST_DATA", None)
        config.DATA_ROOT, config.MANIFEST_DIR, config.RAW_DIR, config.DATASET_DIR = saved


def test_fetch_parallel_fetches_all(tmp_path, monkeypatch):
    # workers>1 must fetch every independent record exactly once, with thread-safe
    # writes to the output manifest and the (shared) rejection log.
    monkeypatch.setattr(fetch_mod, "_session", lambda _t: _DummySession())
    monkeypatch.setattr(fetch_mod, "fetch_record", _staging_fetch_record(1024))
    manifest = tmp_path / "keep.jsonl"
    _write_recs(manifest, 10, 1024)
    stats = fetch_mod.fetch(
        manifest, out_path=tmp_path / "fetched.jsonl", raw_dir=tmp_path / "raw",
        rejections_path=tmp_path / "rej.jsonl", max_bytes=None, workers=4,
    )
    assert stats["fetched"] == 10 and stats["stopped_disk_budget"] is False
    lines = list(read_jsonl(tmp_path / "fetched.jsonl"))
    assert len(lines) == 10 and {ln["recid"] for ln in lines} == {str(i) for i in range(10)}
    # rejection log stayed uncorrupted under concurrent appends (all 10 lines parse)
    assert len(list(read_jsonl(tmp_path / "rej.jsonl"))) == 10


def test_fetch_parallel_max_records_does_not_overshoot(tmp_path, monkeypatch):
    # max_records must bound what is SUBMITTED; gating on the completed count (which
    # lags behind by up to 2*workers futures) let it overshoot badly.
    monkeypatch.setattr(fetch_mod, "_session", lambda _t: _DummySession())
    monkeypatch.setattr(fetch_mod, "fetch_record", _staging_fetch_record(64))
    manifest = tmp_path / "keep.jsonl"
    _write_recs(manifest, 40, 64)
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "fetched.jsonl",
                            raw_dir=tmp_path / "raw", rejections_path=tmp_path / "rej.jsonl",
                            max_bytes=None, workers=4, max_records=5)
    assert stats["fetched"] == 5


def _write_7z(archive, members):
    """Build a .7z from ``{arcname: bytes}`` (helper for the 7z tests)."""
    py7zr = pytest.importorskip("py7zr")
    src = archive.parent / "_7zsrc"
    src.mkdir(exist_ok=True)
    with py7zr.SevenZipFile(archive, "w") as zf:
        for arcname, data in members.items():
            f = src / arcname.replace("/", "_")
            f.write_bytes(data)
            zf.write(f, arcname)


def test_extract_7z_extracts_vasp_members_to_disk(tmp_path):
    # Regression: py7zr 1.x removed the `read()` API this used, so EVERY .7z record
    # raised AttributeError and staged nothing. Selected members must land on disk
    # (streamed, so a huge member cannot blow up RAM) and non-VASP files stay out.
    archive = tmp_path / "calcs.7z"
    _write_7z(archive, {**{f"calc{i}/OUTCAR": b"o" * 1000 for i in range(4)},
                        "calc0/notes.txt": b"ignore me"})
    dest = tmp_path / "out"
    names, extracted = fetch_mod._extract_7z(archive, dest, 1 << 40)
    assert sorted(extracted) == [f"calc{i}/OUTCAR" for i in range(4)]
    assert all((dest / n).read_bytes() == b"o" * 1000 for n in extracted)
    assert not (dest / "calc0" / "notes.txt").exists()   # non-VASP not extracted
    assert "calc0/notes.txt" in names                    # but still listed


def test_extract_7z_skips_oversized_member(tmp_path):
    archive = tmp_path / "big.7z"
    _write_7z(archive, {"calc/OUTCAR": b"o" * 5000, "calc/INCAR": b"i" * 10})
    dest = tmp_path / "out"
    names, extracted = fetch_mod._extract_7z(archive, dest, 1000)  # member cap 1000 B
    assert extracted == ["calc/INCAR"]        # oversized member skipped
    assert not (dest / "calc" / "OUTCAR").exists()
    assert "calc/OUTCAR" in names             # still reported for availability scanning


def test_extract_7z_skips_zip_slip_members(tmp_path):
    archive = tmp_path / "evil.7z"
    _write_7z(archive, {"../escaped/OUTCAR": b"nope", "calc/OUTCAR": b"fine"})
    dest = tmp_path / "out"
    _names, extracted = fetch_mod._extract_7z(archive, dest, 1 << 40)
    assert extracted == ["calc/OUTCAR"]
    assert not (tmp_path / "escaped").exists()


def test_fetch_record_stages_a_7z_archive(tmp_path):
    # End-to-end: a .7z record must yield a calc unit (this silently yielded nothing,
    # or crashed the run, with py7zr 1.x).
    archive = tmp_path / "src.7z"
    _write_7z(archive, {"calc/OUTCAR": b"outcar-bytes", "calc/INCAR": b"incar"})
    blob = archive.read_bytes()
    rec = {"recid": "777",
           "files": [{"key": "src.7z", "download": "http://x/src.7z", "size": len(blob),
                      "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, _MultiFileStreamSession({"http://x/src.7z": blob}),
                         tmp_path / "raw", max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 1
    assert entry["calc_units"][0]["outcar"].endswith("calc/OUTCAR")


def test_rar_without_unrar_binary_is_a_rejection_not_a_crash(tmp_path, monkeypatch):
    # rarfile raises at open time when no unrar/bsdtar binary exists (likely on a bare
    # cluster node). That must reject ONE file, not abort the harvest.
    rarfile = pytest.importorskip("rarfile")
    blob = b"Rar!\x1a\x07\x00 not really a rar"

    def boom(*a, **k):
        raise rarfile.RarCannotExec("unrar not installed")

    monkeypatch.setattr(rarfile, "RarFile", boom)
    rec = {"recid": "888",
           "files": [{"key": "data.rar", "download": "http://x/data.rar",
                      "size": len(blob), "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej_path = tmp_path / "rej.jsonl"
    rej = RejectionLogger(rej_path)
    entry = fetch_record(rec, _MultiFileStreamSession({"http://x/data.rar": blob}),
                         tmp_path / "raw", max_bytes=None, rej=rej)   # must not raise
    rej.close()
    assert entry is None
    reasons = [r["reason"] for r in read_jsonl(rej_path)]
    assert "extract_error" in reasons and "no_vasp_files_fetched" in reasons


def test_extract_errors_cover_backend_api_drift():
    # An optional backend's API/version mismatch must reject ONE archive, not abort a
    # multi-hour harvest, so the caught-exception tuple includes AttributeError.
    assert AttributeError in fetch_mod._EXTRACT_ERRORS
    assert RuntimeError in fetch_mod._EXTRACT_ERRORS


# --------------------------------------------------------------------------- #
# pipeline — overlap fetch(i+1) with parse+purge(i)                           #
# --------------------------------------------------------------------------- #

def test_run_pipeline_overlaps_fetch_and_process():
    # Deterministic overlap proof (no sleeps): process(i) blocks until fetch(i+1) has
    # STARTED. If the orchestrator serialised the stages, process(i) would never be
    # unblocked and the wait would time out -> test fails. Overlap => it proceeds.
    parts = ["p0", "p1", "p2", "p3"]
    fetch_started = {p: threading.Event() for p in parts}
    order: list[tuple[str, str]] = []
    lock = threading.Lock()

    def fetch_fn(p):
        with lock:
            order.append(("fetch", p))
        fetch_started[p].set()

    def process_fn(p):
        i = parts.index(p)
        if i + 1 < len(parts):  # a next part exists -> its fetch must overlap this process
            assert fetch_started[parts[i + 1]].wait(timeout=10), f"no overlap after {p}"
        with lock:
            order.append(("process", p))
        return p

    done, errors = run_pipeline(parts, fetch_fn, process_fn, after_workers=1)
    assert not errors
    assert set(done) == set(parts)
    # every part was fetched, in order, and every part was processed
    fetches = [p for kind, p in order if kind == "fetch"]
    assert fetches == parts
    assert sorted(p for kind, p in order if kind == "process") == sorted(parts)


def test_run_pipeline_resumes_a_part_whose_fetch_hit_the_disk_budget():
    # THE disk-budget correctness property: a fetch that stops part way (returns False)
    # must NOT be carried on and forgotten — the orchestrator drains the outstanding
    # parse+purge (the only thing that frees staging) and re-fetches the SAME part.
    parts = ["p0", "p1"]
    passes: dict[str, int] = {"p0": 0, "p1": 0}
    processed: list[str] = []

    def fetch_fn(p):
        passes[p] += 1
        # p0 needs two passes: the first fills the budget, then p0's own... no — the
        # first pass of p1 is what stops, and only p0's parse+purge can free space.
        if p == "p1" and passes[p] == 1:
            return False
        return True

    def process_fn(p):
        processed.append(p)

    done, errors = run_pipeline(parts, fetch_fn, process_fn, after_workers=1)
    assert not errors
    assert passes == {"p0": 1, "p1": 2}          # p1 retried, not skipped
    assert set(done) == {"p0", "p1"}
    assert processed[0] == "p0"                   # p0's parse ran before p1 resumed
    assert set(processed) == {"p0", "p1"}         # both parts processed


def test_run_pipeline_paces_the_very_first_part_by_processing_its_own_batch():
    # The FIRST part has no older work to drain, so a budget trip there must be paced by
    # parsing+purging that part's own staged records and resuming it — the same
    # fetch->parse->purge->resume loop the disk valve is designed for. (Erroring out
    # here would fail the whole harvest on batch 0 whenever one batch exceeds the budget.)
    fetch_passes = {"n": 0}
    processed: list[str] = []

    def fetch_fn(p):
        fetch_passes["n"] += 1
        return fetch_passes["n"] >= 3          # p0 needs 3 passes to drain its batch

    def process_fn(p):
        processed.append(p)

    done, errors = run_pipeline(["p0"], fetch_fn, process_fn, after_workers=1)
    assert not errors
    assert fetch_passes["n"] == 3
    # processed twice mid-fetch to reclaim staging, then once more at the end
    assert processed == ["p0", "p0", "p0"]
    assert done == ["p0"]                      # deduped despite repeated processing


def test_run_pipeline_stops_a_part_that_never_completes():
    # A part that cannot be completed even with repeated parse+purge cycles means the
    # budget cannot hold a meaningful slice of one batch: report it and STOP, rather than
    # looping forever or under-fetching every later part.
    passes: dict[str, int] = {"p0": 0, "p1": 0, "p2": 0}

    def fetch_fn(p):
        passes[p] += 1
        return p != "p1"                       # p1 can never complete

    done, errors = run_pipeline(["p0", "p1", "p2"], fetch_fn, lambda p: None,
                                after_workers=1, max_fetch_passes=4)
    assert passes["p1"] == 4                   # bounded by max_fetch_passes, then gave up
    assert passes["p2"] == 0                   # stopped: later parts not under-fetched
    assert done == ["p0", "p1"]                # p1's partial batches were still parsed
    assert len(errors) == 1 and errors[0][0] == "p1"
    assert "disk budget" in str(errors[0][1])


def test_run_pipeline_records_process_errors_without_stopping():
    # A failing background process is recorded (not swallowed, not fatal); other parts
    # still complete.
    parts = ["a", "b", "c"]

    def fetch_fn(p):
        pass

    def process_fn(p):
        if p == "b":
            raise RuntimeError("parse boom")
        return p

    done, errors = run_pipeline(parts, fetch_fn, process_fn, after_workers=1)
    assert set(done) == {"a", "c"}
    assert len(errors) == 1 and errors[0][0] == "b"
    assert isinstance(errors[0][1], RuntimeError)


def test_cli_pipeline_wiring(tmp_path, monkeypatch):
    # End-to-end wiring of the `pipeline` CLI command (split -> fetch -> parse -> purge
    # -> verify) with the heavy stages faked, so a signature mismatch would fail here.
    import zenodo_harvest.cli as cli_mod
    keep = tmp_path / "keep.jsonl"
    with keep.open("w") as fh:
        for i in range(5):
            fh.write(json.dumps({"recid": str(i), "files": []}) + "\n")
    calls: list[str] = []

    def fake_fetch(in_path, out_path, raw_dir, **k):
        calls.append("fetch")
        Path(out_path).write_text(json.dumps({"recid": "x"}) + "\n")
        return {"fetched": 1, "stopped_disk_budget": False}

    def fake_parse(in_path, dataset_dir, **k):
        calls.append("parse")
        return {"ok": True}

    def fake_purge(raw_dir, dataset_dir, **k):
        calls.append("purge")
        return {"ok": True}

    monkeypatch.setattr(cli_mod, "fetch", fake_fetch)
    monkeypatch.setattr(cli_mod, "parse", fake_parse)
    monkeypatch.setattr(cli_mod, "purge_raw", fake_purge)
    monkeypatch.setattr(cli_mod, "verify_dataset", lambda d: {"ok": True})

    rc = cli_mod.main(["pipeline", "--in", str(keep), "--parts", "3",
                       "--raw-dir", str(tmp_path / "raw"), "--dataset-dir", str(tmp_path / "ds")])
    assert rc == 0
    assert calls.count("fetch") == 3 and calls.count("parse") == 3 and calls.count("purge") == 3


def test_cli_pipeline_resumes_a_batch_stopped_by_the_disk_budget(tmp_path, monkeypatch):
    # The end-to-end version of the disk-budget property: stage-2 fetch reporting
    # stopped_disk_budget must make the CLI's fetch_fn return False, so the batch is
    # re-fetched after the parse+purge frees space — never silently left half-fetched.
    import zenodo_harvest.cli as cli_mod
    keep = tmp_path / "keep.jsonl"
    with keep.open("w") as fh:
        for i in range(4):
            fh.write(json.dumps({"recid": str(i), "files": []}) + "\n")
    fetch_calls: list[str] = []
    stopped_once = {"done": False}

    def fake_fetch(in_path, out_path, raw_dir, **k):
        fetch_calls.append(Path(in_path).name)
        Path(out_path).write_text(json.dumps({"recid": "x"}) + "\n")
        # Batch 2 stops mid-way once: batch 1's parse+purge is outstanding, so draining
        # it frees staging and this batch can then be resumed to completion.
        if Path(in_path).name.endswith("part-001.jsonl") and not stopped_once["done"]:
            stopped_once["done"] = True
            return {"fetched": 1, "stopped_disk_budget": True}
        return {"fetched": 1, "stopped_disk_budget": False}

    monkeypatch.setattr(cli_mod, "fetch", fake_fetch)
    monkeypatch.setattr(cli_mod, "parse", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(cli_mod, "purge_raw", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(cli_mod, "verify_dataset", lambda d: {"ok": True})

    rc = cli_mod.main(["pipeline", "--in", str(keep), "--parts", "2",
                       "--raw-dir", str(tmp_path / "raw"), "--dataset-dir", str(tmp_path / "ds")])
    assert rc == 0
    # part-000 once, part-001 twice (stopped, then resumed) => 3 fetch calls total
    assert len(fetch_calls) == 3
    assert fetch_calls[1] == fetch_calls[2]      # the SAME part was retried, not skipped


def test_cli_pipeline_reports_a_hard_fetch_failure_and_still_verifies(tmp_path, monkeypatch):
    # A systemic fetch failure must not lose the run's report: it is recorded, the
    # dataset is still verified, and the exit code is non-zero.
    import zenodo_harvest.cli as cli_mod
    keep = tmp_path / "keep.jsonl"
    keep.write_text(json.dumps({"recid": "1", "files": []}) + "\n")
    verified = {"n": 0}

    def boom(*a, **k):
        raise requests.RequestException("DNS is down")

    def fake_verify(d):
        verified["n"] += 1
        return {"ok": True}

    monkeypatch.setattr(cli_mod, "fetch", boom)
    monkeypatch.setattr(cli_mod, "verify_dataset", fake_verify)
    rc = cli_mod.main(["pipeline", "--in", str(keep), "--parts", "1",
                       "--raw-dir", str(tmp_path / "raw"), "--dataset-dir", str(tmp_path / "ds")])
    assert rc == 1                      # reported as a failure...
    assert verified["n"] == 1           # ...but the dataset was still verified


def test_is_archive_treats_misnamed_compressed_tarballs_as_tar():
    # Bare .gz/.bz2/.xz are ambiguous. A compressed single VASP file stays a direct
    # download; anything else is treated as a (mis-named) compressed tarball rather than
    # being silently ignored — the same recall gap already fixed for `.zst`. Measured on
    # Zenodo: e.g. a 17 GB `…-vasp-raw.gz` that held a tar of VASP runs.
    assert fetch_mod._is_archive("OUTCAR.gz") is None        # single compressed VASP file
    assert fetch_mod._is_archive("vasprun.xml.gz") is None
    assert fetch_mod._is_archive("run1_OUTCAR.bz2") is None
    assert fetch_mod._is_archive("photoionization-vasp-raw.gz") == "tar"
    assert fetch_mod._is_archive("research_data.bz2") == "tar"
    assert fetch_mod._is_archive("dataset.xz") == "tar"
    assert fetch_mod._is_archive("archive.tar.gz") == "tar"  # explicit form unchanged


def test_extract_tar_reads_a_misnamed_gzipped_tarball(tmp_path):
    # tarfile auto-detects compression from CONTENT, so a gzipped tar named `.gz`
    # extracts normally...
    src = tmp_path / "OUTCAR"
    src.write_bytes(b"outcar-body")
    arc = tmp_path / "vasp-raw.gz"
    with tarfile.open(arc, "w:gz") as tf:
        tf.add(src, arcname="calc/OUTCAR")
    dest = tmp_path / "out"
    names, extracted = fetch_mod._extract_tar(arc, dest, 1 << 30)
    assert extracted == ["calc/OUTCAR"]
    assert (dest / "calc" / "OUTCAR").read_bytes() == b"outcar-body"


def test_misnamed_gz_that_is_not_a_tar_is_a_clean_rejection(tmp_path):
    # ...and a bare .gz that is NOT a tar raises TarError, which is already caught and
    # logged as one `extract_error` rejection rather than crashing the harvest.
    import gzip
    blob_path = tmp_path / "plain.gz"
    with gzip.open(blob_path, "wb") as fh:
        fh.write(b"not a tar at all")
    blob = blob_path.read_bytes()
    rec = {"recid": "999",
           "files": [{"key": "mystery_data.gz", "download": "http://x/m.gz",
                      "size": len(blob), "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej_path = tmp_path / "rej.jsonl"
    rej = RejectionLogger(rej_path)
    entry = fetch_record(rec, _MultiFileStreamSession({"http://x/m.gz": blob}),
                         tmp_path / "raw", max_bytes=None, rej=rej)   # must not raise
    rej.close()
    assert entry is None
    assert "extract_error" in [r["reason"] for r in read_jsonl(rej_path)]


# --------------------------------------------------------------------------- #
# per-record staging quota — the disk budget must bound REALITY, not just the  #
# pre-download estimate (compression ratios vary far more than the 6x booked)  #
# --------------------------------------------------------------------------- #

def _compressible_zip(n_members=400, member_size=1 << 20):
    """A zip of highly-compressible members: tiny compressed, huge extracted."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(n_members):
            zf.writestr(f"calc{i}/OUTCAR", b"\0" * member_size)
    return buf.getvalue()


def test_disk_valve_bounds_total_staging_end_to_end(tmp_path, monkeypatch):
    # The property that matters on the cluster: across MANY high-ratio records and
    # parallel workers, total staged bytes stay near the budget instead of running away.
    blob = _compressible_zip(n_members=200)
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    with manifest.open("w") as fh:
        for i in range(40):   # collectively want far more staging than the budget allows
            fh.write(json.dumps({"recid": str(i), "files": [
                {"key": "d.zip", "download": "http://x/d.zip", "size": len(blob),
                 "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")
    budget = 16 << 20
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl",
                            raw_dir=tmp_path / "raw", rejections_path=tmp_path / "r.jsonl",
                            max_bytes=None, max_disk_bytes=budget, workers=3)
    staged = sum(p.stat().st_size for p in (tmp_path / "raw").rglob("*") if p.is_file())
    # Without the per-record quota a single one of these records staged ~200 MB.
    assert staged <= budget * 2, f"staged {staged} vs budget {budget}"
    assert stats["stopped_disk_budget"] is True     # stopped cleanly, resumably
    assert stats["fetched"] < 40                    # paced, not run to completion


def test_quota_exhaustion_on_disk_is_transient_and_retryable(tmp_path, monkeypatch):
    """Last line of defence: if the real filesystem quota is hit anyway (budget set too
    high, or something else filled the volume), writes fail with ENOSPC/EDQUOT.

    That must stay a TRANSIENT failure so a later run retries the record. Regression: the
    per-file error was already transient, but the record then fell through to
    ``no_vasp_files_fetched`` — a TERMINAL reason — so every affected record was
    permanently skipped by all later runs. A disk-full episode silently shrank the harvest.
    """
    blob = b"outcar-bytes"
    rec = {"recid": "1", "files": [{"key": "OUTCAR", "download": "http://x/OUTCAR",
                                    "size": len(blob),
                                    "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej_path = tmp_path / "rej.jsonl"
    rej = RejectionLogger(rej_path)          # opened BEFORE the failure is injected

    def edquot(self, *a, **k):
        raise OSError(errno.EDQUOT, "Disk quota exceeded")

    monkeypatch.setattr(fetch_mod.Path, "open", edquot)
    entry = fetch_record(rec, _MultiFileStreamSession({"http://x/OUTCAR": blob}),
                         tmp_path / "raw", max_bytes=None, rej=rej)   # must not raise
    monkeypatch.undo()
    rej.close()

    rows = list(read_jsonl(rej_path))
    reasons = [r["reason"] for r in rows]
    assert entry is None
    assert any(r.startswith("write_error") for r in reasons), reasons
    # the RECORD-level verdict must be the retryable one, never the terminal one
    assert fetch_mod._TRANSIENT_RECORD_REASON in reasons, reasons
    assert "no_vasp_files_fetched" not in reasons
    # ...so a later run does NOT skip this recid
    assert fetch_mod._terminal_reject_recids(rej_path) == set()


def test_record_with_genuinely_no_vasp_stays_terminal(tmp_path):
    # The flip side: a record that really holds nothing usable must STILL be terminal, so
    # its (often large) archive is not re-downloaded on every resume.
    blob = _zip_bytes([("notes/readme.txt", b"no vasp here")])
    rec = {"recid": "2", "files": [{"key": "d.zip", "download": "http://x/d.zip",
                                    "size": len(blob),
                                    "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej_path = tmp_path / "rej.jsonl"
    rej = RejectionLogger(rej_path)
    assert fetch_record(rec, _MultiFileStreamSession({"http://x/d.zip": blob}),
                        tmp_path / "raw", max_bytes=None, rej=rej) is None
    rej.close()
    assert "no_vasp_files_fetched" in [r["reason"] for r in read_jsonl(rej_path)]
    assert fetch_mod._terminal_reject_recids(rej_path) == {"2"}




# --------------------------------------------------------------------------- #
# StagingBudget — the disk/inode valve. Enforcement is on ACTUAL bytes and     #
# files as they are written, so these tests deliberately sweep the compression #
# ratio (measured on real Zenodo data: ~1x to 4.1x; synthetic: 880x) instead   #
# of trusting any single factor.                                              #
# --------------------------------------------------------------------------- #

def _zip_with_ratio(member_bytes, n_members=20, kind="text"):
    """A zip whose members compress predictably.

    ``kind``: "incompressible" (random ~1x) | "text" (hex ~2x) | "bomb" (zeros ~1000x).
    Returns ``(blob, uncompressed_total, ratio)``.
    """
    if kind == "incompressible":
        body = os.urandom(member_bytes)
    elif kind == "text":
        body = os.urandom(member_bytes // 2).hex().encode()[:member_bytes]
    else:
        body = b"\0" * member_bytes
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(n_members):
            zf.writestr(f"calc{i}/OUTCAR", body)
    blob = buf.getvalue()
    total = len(body) * n_members
    return blob, total, total / len(blob)


def _one_record_manifest(path, blob, n_records=1, url="http://x/d.zip"):
    with path.open("w") as fh:
        for i in range(n_records):
            fh.write(json.dumps({"recid": str(i), "files": [
                {"key": "d.zip", "download": url, "size": len(blob),
                 "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")


def test_staging_budget_charges_refunds_and_reports_peak():
    b = fetch_mod.StagingBudget(max_bytes=1000, max_files=10)
    b.begin_record()                                  # record A
    assert b.charge(600, 3) and b.used_bytes == 600 and b.used_files == 3
    b.begin_record()                                  # record B starts with A's 600 staged
    with pytest.raises(fetch_mod.BudgetExceeded):     # reclaimable -> abort + roll back
        b.charge(500, 1)
    assert b.hit_limit == "bytes" and b.full()        # ...and the run should pause
    b.refund(600, 3)                             # e.g. an archive deleted after extraction
    assert b.used_bytes == 0 and b.used_files == 0
    assert b.peak_bytes == 600 and b.peak_files == 3      # high-water mark retained
    b.begin_record()
    assert b.charge(900, 1)                      # space is available again
    f = fetch_mod.StagingBudget(max_files=2)
    f.begin_record()
    assert f.charge(10**9, 1)
    f.begin_record()                             # a second record, one file already staged
    assert f.charge(10**9, 1)
    f.begin_record()
    with pytest.raises(fetch_mod.BudgetExceeded):
        f.charge(1, 1)
    assert f.hit_limit == "files"
    off = fetch_mod.StagingBudget()
    assert off.charge(10**12, 10**6) and not off.enabled  # opt-in: no limits, no accounting


def test_staging_budget_flags_an_item_too_big_for_the_whole_budget():
    # A refusal is classified by the record's OWN footprint, never by what happens to be
    # staged beside it. This record needs more than the entire budget, so no purge can help:
    # that must NOT look like "pause and reclaim" (the pacing loop would spin on it for
    # ever) — charge returns False so the caller keeps what landed and moves on.
    b = fetch_mod.StagingBudget(max_bytes=100)
    b.begin_record()
    assert b.charge(500, 1) is False             # -> it can never fit; truncate, don't retry
    assert b.unfittable == 1 and b.record_was_truncated()
    assert not b.full()                          # -> reported per item, run continues
    b.begin_record()
    assert b.charge(50, 1) and not b.full()      # a stale hit_limit must not trip pause
    assert not b.record_was_truncated()          # per-record flag reset by begin_record
    b.begin_record()                             # a NEW record; 50 staged by the previous one
    with pytest.raises(fetch_mod.BudgetExceeded):
        b.charge(60, 1)                          # fits alone (60<100) -> reclaimable, retry
    assert b.full() and b.pause == "bytes"


def test_a_refusal_is_classified_by_the_records_own_footprint_not_by_its_neighbours():
    """The verdict must not depend on which records happen to be in flight.

    The earlier rule was "was the budget empty when this record started?". Under
    ``--workers N`` a record almost never starts against an empty budget, so a record too
    big for the budget was classified as merely deferrable — rolled back, re-downloaded and
    re-refused every pass. In a randomised sweep this stalled 8 of 9 records permanently.
    """
    b = fetch_mod.StagingBudget(max_bytes=100)
    b.begin_record()
    assert b.charge(30, 1)                       # another worker's record, mid-flight
    own_a = b.own_handle()
    b.begin_record()                             # our record (same thread, new tally)
    assert b.charge(40, 1)                       # 70 used; ours accounts for 40
    # 90 more would exceed the limit, and ours alone (40+90) exceeds it too -> unfittable,
    # even though 30 bytes of somebody else's data are sitting in the budget.
    assert b.charge(90, 1) is False
    assert b.record_was_truncated()
    b.refund(40, 1)                              # that record is rolled back: 30 left staged
    # ...whereas a record that would fit alone (40+50 < 100) but not beside the neighbour's
    # 30 is deferrable: roll it back, purge, retry.
    b.begin_record()
    assert b.charge(40, 1)
    with pytest.raises(fetch_mod.BudgetExceeded):
        b.charge(50, 1)
    assert own_a == [30, 1]                      # per-record tallies stay independent


@pytest.mark.parametrize("kind", ["incompressible", "text", "bomb"])
@pytest.mark.parametrize("workers", [1, 3])
def test_byte_limit_holds_at_any_compression_ratio(tmp_path, monkeypatch, kind, workers):
    """THE invariant, swept across compression ratios and both code paths.

    A fixed "expansion factor" cannot bound staging — the ratio is whatever the uploader's
    compression achieved (measured 1x-4.1x on real records, 880x on a bomb). Because every
    byte is charged as it lands, peak staged bytes must never exceed the limit for ANY of
    these, with no tuning.
    """
    blob, uncompressed, ratio = _zip_with_ratio(1 << 20, n_members=40, kind=kind)
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=6)
    budget = 12 << 20                    # far less than 6 records x 40 MiB uncompressed
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                            max_disk_bytes=budget, workers=workers)
    staged = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert staged <= budget, f"{kind} (ratio {ratio:.0f}x, workers={workers}): {staged} > {budget}"
    assert stats["peak_staged_bytes"] <= budget          # the run's own report agrees
    assert stats["peak_staged_bytes"] >= staged


@pytest.mark.parametrize("workers", [1, 3])
def test_file_limit_holds_exactly(tmp_path, monkeypatch, workers):
    # Inodes are charged per file as it lands, so the file limit is exact too — no
    # in-flight overshoot window, unlike the old reservation-based scheme.
    blob, _u, _r = _zip_with_ratio(4096, n_members=60, kind="text")
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=5)
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                            max_disk_files=45, workers=workers)
    # The quota being modelled is an INODE quota (CSD3 scratch is Lustre), so the count
    # that must stay under the limit includes directories, not just regular files.
    staged = sum(1 for p in raw.rglob("*") if p.is_file() or p.is_dir())
    assert staged <= 45, f"workers={workers}: staged {staged} inodes > 45"
    assert sum(1 for p in raw.rglob("*") if p.is_dir()) > 0   # dirs really are in play
    assert stats["peak_staged_files"] <= 45
    assert stats["stopped_on"] == "files" and stats["stopped_disk_budget"] is True


def test_directories_are_charged_so_a_dir_heavy_record_cannot_blow_the_inode_quota(
        tmp_path, monkeypatch):
    """Each member of this archive sits in its own directory, so the real inode cost is
    ~2x the file count. Counting only files (the earlier behaviour) let a record use about
    twice the quota it was charged for — on CSD3 that is the silent stall the limit exists
    to prevent, because exceeding the 1M-file quota only shows up as write errors."""
    blob, _u, _r = _zip_with_ratio(256, n_members=40, kind="text")   # calc0..39/OUTCAR
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=1)
    raw = tmp_path / "raw"
    limit = 30
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                            max_disk_files=limit, workers=1)
    files = sum(1 for p in raw.rglob("*") if p.is_file())
    dirs = sum(1 for p in raw.rglob("*") if p.is_dir())
    assert dirs >= files                       # one directory per extracted member
    assert files + dirs <= limit, f"{files} files + {dirs} dirs > {limit} inodes"
    assert stats["peak_staged_files"] <= limit
    assert stats["items_over_whole_budget"] >= 1   # reported, not silently truncated


def _understated_manifest(path, blob, declared, key="OUTCAR", url="http://x/OUTCAR"):
    """A manifest that declares ``declared`` bytes for a file that is really ``len(blob)``.

    Models the case the budget must survive without trusting metadata: a stale or wrong
    Zenodo ``size`` (or a server that simply sends more than it advertised). It matters in
    production because the real harvest runs with ``--max-bytes 0`` — no per-file cap at
    all — so the staging budget is the *only* thing bounding what lands on disk.
    """
    with path.open("w") as fh:
        fh.write(json.dumps({"recid": "0", "files": [
            {"key": key, "download": url, "size": declared,
             "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")


def test_budget_is_charged_from_bytes_that_land_not_from_the_declared_size(
        tmp_path, monkeypatch):
    # A file whose declared size understates reality by 30000x, small enough to fit.
    # The tally must equal what is really on disk — if it recorded the declared size, the
    # budget would think it had almost all its room left and overrun on the next record.
    blob = os.urandom(150_000)
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/OUTCAR": blob}))
    manifest = tmp_path / "keep.jsonl"
    _understated_manifest(manifest, blob, declared=5)
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=0,
                            max_disk_bytes=1 << 20, workers=1)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert on_disk == len(blob) and stats["fetched"] == 1
    assert stats["staged_bytes_now"] == on_disk        # reality, not the declared 5 bytes
    assert stats["peak_staged_bytes"] == on_disk


def test_byte_limit_holds_when_a_file_is_far_bigger_than_it_declared(tmp_path, monkeypatch):
    """The limit must bound what LANDS, not what was promised.

    Charging the declared size up front and then writing without re-checking meant a file
    that arrived larger than advertised wrote past the ceiling unnoticed — and with
    ``--max-bytes 0`` (the production setting) there was no second cap to catch it. The
    transfer must now run out of allowance part way and be discarded.
    """
    blob = os.urandom(4 << 20)                      # 4 MiB actually served
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/OUTCAR": blob}))
    manifest = tmp_path / "keep.jsonl"
    _understated_manifest(manifest, blob, declared=1000)   # ~1 KB advertised
    raw = tmp_path / "raw"
    limit = 1 << 20                                 # 1 MiB budget: the 4 MiB cannot fit
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=0,
                            max_disk_bytes=limit, workers=1)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert on_disk <= limit, f"{on_disk} B staged against a {limit} B limit"
    assert stats["peak_staged_bytes"] <= limit
    assert not any(p.name.endswith(".part") for p in raw.rglob("*"))  # partial discarded
    # ...and the record stays retryable: it was refused for space, not for bad data.
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "r.jsonl")]
    assert "disk_budget_reached" in reasons
    assert "no_vasp_files_fetched" not in reasons     # would be terminal = permanent loss


def test_7z_extraction_is_charged_as_it_writes_not_from_its_header(tmp_path):
    """``.7z`` is the one backend that cannot be paced from the outside: py7zr decompresses
    the whole selection in a single call, on its own worker threads. The charge therefore
    happens inside the writer it is handed, per block, so the same invariant holds."""
    archive = tmp_path / "calcs.7z"
    _write_7z(archive, {f"calc{i}/OUTCAR": os.urandom(50_000) for i in range(8)})
    dest = tmp_path / "out"
    limit = 120_000                                  # room for ~2 of the 8 members
    b = fetch_mod.StagingBudget(max_bytes=limit)
    b.begin_record()
    names, extracted = fetch_mod._extract_7z(archive, dest, 1 << 40, b)
    on_disk = sum(p.stat().st_size for p in dest.rglob("*") if p.is_file())
    assert len(names) == 8 and 0 < len(extracted) < 8      # partial, and it says so
    assert on_disk <= limit, f"{on_disk} B extracted against a {limit} B limit"
    assert b.used_bytes == on_disk                        # tally == reality
    assert b.record_was_truncated()                       # reported, not silent
    # whatever did land is intact (no half-written member kept)
    assert all((dest / n).stat().st_size == 50_000 for n in extracted)


def test_7z_extraction_defers_the_record_when_a_purge_could_make_room(tmp_path):
    # Same archive, but the budget is full of ANOTHER record's data: the right response is
    # to abort this record for a clean rollback and retry after parse+purge, not to keep a
    # partial stage. py7zr's threads cannot raise that decision, so the factory records it
    # and _extract_7z re-raises in the calling thread.
    archive = tmp_path / "calcs.7z"
    _write_7z(archive, {f"calc{i}/OUTCAR": os.urandom(50_000) for i in range(8)})
    b = fetch_mod.StagingBudget(max_bytes=120_000)
    b.begin_record()
    assert b.charge(100_000, 1)          # an earlier record's staged data
    b.begin_record()                     # this record starts with a non-zero floor
    with pytest.raises(fetch_mod.BudgetExceeded):
        fetch_mod._extract_7z(archive, tmp_path / "out", 1 << 40, b)


@pytest.mark.parametrize("workers", [1, 4])
def test_limits_hold_with_concurrent_workers_on_one_staging_dir(tmp_path, monkeypatch,
                                                                workers):
    """The real harvest runs ``--workers 4`` against one CSD3 staging dir, so the budget is
    shared: several records are downloading and extracting at the same moment, each
    charging the same tally. Both limits must hold at once, with every declared size wrong.
    """
    blob, _u, _r = _zip_with_ratio(8192, n_members=12, kind="bomb")   # 1000x expansion
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    with manifest.open("w") as fh:
        for i in range(10):
            fh.write(json.dumps({"recid": str(i), "files": [
                {"key": "d.zip", "download": "http://x/d.zip", "size": 7,  # nonsense size
                 "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")
    raw = tmp_path / "raw"
    max_b, max_f = 300_000, 60
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=0,
                            max_disk_bytes=max_b, max_disk_files=max_f, workers=workers)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    inodes = sum(1 for p in raw.rglob("*") if p.is_file() or p.is_dir())
    assert on_disk <= max_b, f"workers={workers}: {on_disk} B > {max_b} B"
    assert inodes <= max_f, f"workers={workers}: {inodes} inodes > {max_f}"
    assert stats["peak_staged_bytes"] <= max_b and stats["peak_staged_files"] <= max_f
    # and the tally still agrees with the filesystem afterwards (no drift under threads)
    assert (stats["staged_bytes_now"], stats["staged_files_now"]) == (on_disk, inodes)


def test_a_record_that_cannot_fit_leaves_nothing_charged_behind(tmp_path, monkeypatch):
    """A record refused as too big must not leave staging behind that nothing can reclaim.

    ``purge-raw`` only frees trees whose calc_ids reached the dataset, so a directory left
    by an unrecorded record holds budget for the rest of the harvest. It used to: the empty
    directories of a refused record stayed charged, the budget was therefore never empty
    again, and every LATER record was deferred and retried against space no purge could
    free — 8 of 9 records permanently uncollected in a randomised sweep.
    """
    blob, _u, _r = _zip_with_ratio(400_000, n_members=4, kind="text")
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=1)
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=0,
                            max_disk_bytes=200_000, workers=1)   # far too small for it
    assert stats["items_over_whole_budget"] >= 1
    assert list(raw.iterdir()) == []                     # tree removed...
    assert stats["staged_bytes_now"] == 0                # ...and un-charged
    assert stats["staged_files_now"] == 0
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "r.jsonl")]
    assert "record_exceeds_disk_budget" in reasons
    # NOT terminal: raising the budget must be enough to collect it on a later run.
    assert not (fetch_mod._TERMINAL_REJECT_REASONS & set(reasons))
    assert fetch_mod._terminal_reject_recids(tmp_path / "r.jsonl") == set()


def test_a_parallel_pass_that_stages_nothing_falls_back_to_serial(tmp_path, monkeypatch):
    """Forward progress must be guaranteed, not likely.

    With several workers, records can each fit individually yet fill the budget between
    them — then every one is rolled back and the pass stages nothing. Handing that batch
    back to the pacing loop repeats it for ever, because there is nothing new to purge. The
    run must notice and retry serially, where a record meets a budget the rollbacks just
    emptied.
    """
    blob, _u, _r = _zip_with_ratio(300_000, n_members=3, kind="text")   # ~0.9 MB staged
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=4)
    raw = tmp_path / "raw"
    limit = 1_300_000                     # room for one record, not for four at once
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=0,
                            max_disk_bytes=limit, workers=4)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert on_disk <= limit and stats["peak_staged_bytes"] <= limit
    assert stats["fetched"] >= 1, "the pass must not stage nothing and hand back a stall"
    if stats.get("serial_fallback"):
        assert stats["fetched"] >= 1      # the fallback is what rescued it


def test_refused_member_is_refunded_so_the_budget_does_not_leak(tmp_path):
    # A member abandoned part way (here: over --max-member-bytes) must give back both its
    # bytes and its inode. A leak would shrink the budget on every archive until the
    # harvest paced itself to a standstill with disk to spare.
    b = fetch_mod.StagingBudget(max_bytes=1 << 20, max_files=100)
    b.begin_record()
    src = io.BytesIO(b"x" * 5000)
    out = tmp_path / "sub" / "OUTCAR"
    assert fetch_mod._copy_capped(src, out, 1000, b) == fetch_mod._COPY_OVER_CAP
    assert not out.exists()
    assert b.used_bytes == 0
    assert b.used_files == 1          # only the directory it created remains charged
    assert (tmp_path / "sub").is_dir()


def test_archive_bytes_are_refunded_so_they_are_not_double_counted(tmp_path, monkeypatch):
    # An archive is deleted as soon as its VASP members are extracted, so its bytes are
    # transient. If they were never refunded, the budget would think the footprint is
    # (archive + extracted) and pace ~2x too conservatively.
    blob, _u, _r = _zip_with_ratio(64, n_members=2, kind="text")
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=1)
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                            max_disk_bytes=1 << 30, workers=1)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    # Inodes, not just regular files: on Lustre (CSD3 scratch) a directory consumes one
    # from the same "1 million files" quota, so the tally has to include them.
    n_inodes_on_disk = sum(1 for p in raw.rglob("*") if p.is_file() or p.is_dir())
    assert sum(1 for p in raw.rglob("*") if p.is_dir()) > 0, "expected staged directories"
    # the running total matches what is really there (archive refunded, members kept)
    assert stats["staged_bytes_now"] == on_disk
    assert stats["staged_files_now"] == n_inodes_on_disk
    assert not (raw / "0" / "d.zip").exists()


def test_member_too_big_for_the_budget_is_rejected_not_stalled(tmp_path, monkeypatch):
    # A single member larger than the WHOLE budget can never be staged. It must be
    # reported per item and the run continue — otherwise the pacing loop retries forever.
    big, _u, _r = _zip_with_ratio(4 << 20, n_members=1, kind="text")
    small, _u2, _r2 = _zip_with_ratio(1024, n_members=1, kind="text")
    monkeypatch.setattr(fetch_mod, "_session", lambda _t: _MultiFileStreamSession(
        {"http://x/big.zip": big, "http://x/small.zip": small}))
    manifest = tmp_path / "keep.jsonl"
    with manifest.open("w") as fh:
        for recid, blob, url in (("1", big, "http://x/big.zip"),
                                 ("2", small, "http://x/small.zip")):
            fh.write(json.dumps({"recid": recid, "files": [
                {"key": url.rsplit("/", 1)[-1], "download": url, "size": len(blob),
                 "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl",
                            raw_dir=tmp_path / "raw", rejections_path=tmp_path / "r.jsonl",
                            max_bytes=None, max_disk_bytes=1 << 20, workers=1)
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "r.jsonl")]
    assert "disk_budget_reached" in reasons          # the oversized item, reported
    assert stats["items_over_whole_budget"] >= 1
    assert stats["fetched"] == 1                     # ...and the small record still landed


def test_budget_deferred_record_is_rolled_back_not_left_partial(tmp_path, monkeypatch):
    """A record cut short by the budget must leave NO trace and stay out of the manifest.

    Otherwise the harvest keeps permanently-partial data: resumes skip recids already in
    ``fetched.jsonl``, and partial files that no fetched record owns can never be reclaimed
    by ``purge-raw`` — they would occupy the budget forever.
    """
    # Sized so the second record fits on its own but not beside the first: that is what
    # makes it DEFERRABLE (a purge can make room) rather than too big for the budget.
    filler, _u, _r = _zip_with_ratio(1 << 20, n_members=6, kind="text")     # ~6 MiB staged
    big, _u2, _r2 = _zip_with_ratio(1 << 20, n_members=5, kind="text")      # ~5 MiB staged
    monkeypatch.setattr(fetch_mod, "_session", lambda _t: _MultiFileStreamSession(
        {"http://x/filler.zip": filler, "http://x/big.zip": big}))
    manifest = tmp_path / "keep.jsonl"
    with manifest.open("w") as fh:
        for recid, blob, url in (("filler", filler, "http://x/filler.zip"),
                                 ("big", big, "http://x/big.zip")):
            fh.write(json.dumps({"recid": recid, "files": [
                {"key": url.rsplit("/", 1)[-1], "download": url, "size": len(blob),
                 "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")
    raw = tmp_path / "raw"
    stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                            rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                            max_disk_bytes=12 << 20, workers=1)
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "r.jsonl")]
    assert "disk_budget_deferred" in reasons              # deferred, with a reason
    assert not (raw / "big").exists()                     # rolled back completely
    fetched = [r["recid"] for r in read_jsonl(tmp_path / "f.jsonl")]
    assert "big" not in fetched                           # ...and NOT marked as done
    assert "filler" in fetched                            # the record that did fit is kept
    assert stats["stopped_disk_budget"] is True           # -> pipeline reclaims + resumes
    # the budget's running total matches reality after the rollback (no leaked charge)
    on_disk = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert stats["staged_bytes_now"] == on_disk


def test_closed_loop_harvests_everything_without_exceeding_the_limits(tmp_path, monkeypatch):
    """The end-to-end property, with REAL archives at a high compression ratio.

    A manifest whose uncompressed content is many times the budget must still be harvested
    COMPLETELY, with peak staging never breaching either limit. Exercises the full loop:
    charge-as-written -> stop cleanly -> reclaim (parse+purge stand-in) -> resume the same
    batch -> repeat. A regression in any link shows up as lost records or a blown budget.
    """
    from zenodo_harvest.pipeline import run_pipeline

    blob, uncompressed, ratio = _zip_with_ratio(1 << 20, n_members=8, kind="text")
    assert ratio > 1.5                                    # genuinely compressed
    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _MultiFileStreamSession({"http://x/d.zip": blob}))
    budget_bytes, budget_files = 20 << 20, 40
    n_parts, per_part = 4, 5
    parts = []
    for pi in range(n_parts):
        part = tmp_path / f"part-{pi}.jsonl"
        with part.open("w") as fh:
            for i in range(per_part):
                fh.write(json.dumps({"recid": f"{pi}-{i}", "files": [
                    {"key": "d.zip", "download": "http://x/d.zip", "size": len(blob),
                     "checksum": "md5:" + md5(blob).hexdigest()}]}) + "\n")
        parts.append(part)
    raw = tmp_path / "raw"
    peaks = {"bytes": 0, "files": 0}

    def fetch_fn(part):
        stats = fetch_mod.fetch(part, out_path=part.with_suffix(".fetched.jsonl"),
                                raw_dir=raw, rejections_path=tmp_path / "rej.jsonl",
                                max_bytes=None, max_disk_bytes=budget_bytes,
                                max_disk_files=budget_files, workers=1)
        peaks["bytes"] = max(peaks["bytes"], stats["peak_staged_bytes"])
        peaks["files"] = max(peaks["files"], stats["peak_staged_files"])
        return not stats["stopped_disk_budget"]

    def process_fn(part):       # stands in for parse + purge-raw
        for rec in read_jsonl(part.with_suffix(".fetched.jsonl")):
            shutil.rmtree(raw / rec["recid"], ignore_errors=True)

    done, errors = run_pipeline(parts, fetch_fn, process_fn, after_workers=1)

    assert not errors, errors
    assert len(done) == n_parts
    # NOTHING lost: every record of every batch landed exactly once...
    fetched = [r["recid"] for p in parts for r in read_jsonl(p.with_suffix(".fetched.jsonl"))]
    assert len(fetched) == n_parts * per_part == len(set(fetched))
    # ...while total uncompressed content far exceeded the byte budget...
    assert uncompressed * n_parts * per_part > budget_bytes * 4
    # ...and neither limit was ever breached.
    assert peaks["bytes"] <= budget_bytes, f"peak {peaks['bytes']} > {budget_bytes}"
    assert peaks["files"] <= budget_files, f"peak {peaks['files']} > {budget_files}"


def test_record_bigger_than_the_whole_budget_does_not_livelock(tmp_path, monkeypatch):
    """Regression for an observed livelock (live run: 48 deferral cycles, never finished).

    A record whose own staging exceeds the entire budget used to fail mid-extraction with
    "space could be freed" (the space in question being its OWN earlier members), get rolled
    back, and be retried — re-downloading every time. It must instead be recognised as
    unfittable: staged as far as the budget allows, kept, reported, and NOT retried.
    """
    # A tiny archive (so the download itself fits) whose CONTENTS far exceed the budget.
    blob, uncompressed, _r = _zip_with_ratio(1 << 20, n_members=20, kind="bomb")
    assert len(blob) < (1 << 20) < uncompressed
    calls = {"n": 0}

    class _CountingSession(_MultiFileStreamSession):
        def get(self, url, **k):
            calls["n"] += 1
            return super().get(url, **k)

    monkeypatch.setattr(fetch_mod, "_session",
                        lambda _t: _CountingSession({"http://x/d.zip": blob}))
    manifest = tmp_path / "keep.jsonl"
    _one_record_manifest(manifest, blob, n_records=1)
    budget = 6 << 20                       # only ~6 MiB: far less than the record needs
    raw = tmp_path / "raw"

    # Emulate the pacing loop: fetch, "purge" nothing (the record is the only thing), retry.
    for attempt in range(3):
        stats = fetch_mod.fetch(manifest, out_path=tmp_path / "f.jsonl", raw_dir=raw,
                                rejections_path=tmp_path / "r.jsonl", max_bytes=None,
                                max_disk_bytes=budget, workers=1)
        if attempt == 0:
            assert stats["fetched"] == 1               # partial stage KEPT, not rolled back
            assert stats["items_over_whole_budget"] >= 1
            assert stats["stopped_disk_budget"] is False   # no pointless pause/retry
            downloads_after_first = calls["n"]
        else:
            # already in the manifest -> skipped entirely, nothing re-downloaded
            assert stats["skipped_existing"] == 1
            assert calls["n"] == downloads_after_first

    staged = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file())
    assert staged <= budget                            # limit still respected
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "r.jsonl")]
    assert "record_exceeds_disk_budget" in reasons     # and it is reported, not silent


# --------------------------------------------------------------------------- #
# Targeted ZIP member fetch (zipstream) — pull only the VASP files out of a    #
# .zip over HTTP Range, instead of downloading the whole archive to extract a  #
# few files and delete the rest (mentor/user 2026-07-29). Standard 32-bit ZIP; #
# falls back to whole-archive download for anything not addressable this way.  #
# --------------------------------------------------------------------------- #

class _ZipRangeSession:
    """Serves a fixed ``{url: blob}`` over HTTP Range like Zenodo: 206 + Content-Range for
    ``bytes=-N`` (suffix) and ``bytes=start-end`` (explicit); a Range-less GET returns the
    whole file (200), which is what the whole-archive fallback + the small-file suffix
    underflow use. Records every ``(url, range_header)`` and the total bytes served, so a
    test can prove targeted fetch transferred far less than the whole archive.

    ``honor_range=False`` models a server that ignores Range (answers 200) — the enumeration
    then fails and the caller must fall back to a whole download.
    """

    def __init__(self, blobs, honor_range=True):
        self.blobs = blobs
        self.honor_range = honor_range
        self.requests: list[tuple] = []
        self.bytes_served = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    def get(self, url, headers=None, timeout=None, stream=None):
        blob = self.blobs[url]
        size = len(blob)
        rng = (headers or {}).get("Range")
        self.requests.append((url, rng))
        if rng and self.honor_range:
            spec = rng.split("=", 1)[1]
            if spec.startswith("-"):
                n = int(spec[1:])
                start, end = max(0, size - n), size - 1
            else:
                s, e = spec.split("-")
                start = int(s)
                end = min(int(e), size - 1) if e else size - 1
            body = blob[start:end + 1]
            self.bytes_served += len(body)
            return _FakeStreamResp(206, content=body,
                                   headers={"Content-Range": f"bytes {start}-{end}/{size}",
                                            "Content-Length": str(len(body))})
        self.bytes_served += size                        # whole file (fresh / Range ignored)
        return _FakeStreamResp(200, content=blob, headers={"Content-Length": str(size)})


def _zip_with(members, stored=()):
    """Build an in-memory zip from ``[(name, bytes), ...]`` (stored members uncompressed)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in members:
            ct = zipfile.ZIP_STORED if name in stored else zipfile.ZIP_DEFLATED
            zf.writestr(name, content, compress_type=ct)
    return buf.getvalue()


def _read_member(session, url, entry):
    reader = zipstream_mod.open_member_reader(session, url, entry)
    assert reader is not None
    out = bytearray()
    while True:
        b = reader.read(1 << 16)
        if not b:
            break
        out.extend(b)
    reader.close()
    return bytes(out), reader


def test_remote_central_directory_matches_stdlib_offsets():
    # The enumerated members (name/method/sizes/crc/offset) must agree with stdlib's own
    # central-directory reading, or a targeted fetch would seek to the wrong byte.
    members = [("calc/vasprun.xml", b"<xml>" + b"A" * 4000), ("calc/OUTCAR", b"o" * 500),
               ("calc/CHGCAR", os.urandom(20000)), ("stored/POSCAR", b"poscar")]
    blob = _zip_with(members, stored={"stored/POSCAR"})
    entries = zipstream_mod.remote_central_directory("http://x/a.zip", _ZipRangeSession({"http://x/a.zip": blob}))
    assert entries is not None
    ref = {i.filename: i for i in zipfile.ZipFile(io.BytesIO(blob)).infolist()}
    assert {e.name for e in entries} == set(ref)
    for e in entries:
        zi = ref[e.name]
        assert e.local_header_offset == zi.header_offset
        assert e.compressed_size == zi.compress_size
        assert e.uncompressed_size == zi.file_size
        assert e.crc == zi.CRC
        assert e.method == zi.compress_type


def test_remote_central_directory_none_when_range_ignored():
    blob = _zip_with([("calc/OUTCAR", b"o" * 100)])
    sess = _ZipRangeSession({"http://x/a.zip": blob}, honor_range=False)
    assert zipstream_mod.remote_central_directory("http://x/a.zip", sess) is None


def test_open_member_reader_extracts_bytewise_and_verifies_crc():
    members = [("calc/vasprun.xml", b"<xml>" + b"B" * 6000 + b"</xml>"),
               ("calc/OUTCAR", b"outcar " * 300), ("empty/POSCAR", b""),
               ("stored/INCAR", b"ENCUT = 520\nISMEAR = 0\n")]
    blob = _zip_with(members, stored={"stored/INCAR"})
    ref = zipfile.ZipFile(io.BytesIO(blob))
    entries = zipstream_mod.remote_central_directory("http://x/a.zip", _ZipRangeSession({"http://x/a.zip": blob}))
    for e in entries:
        got, reader = _read_member(_ZipRangeSession({"http://x/a.zip": blob}), "http://x/a.zip", e)
        assert got == ref.read(e.name), e.name
        assert reader.complete and reader.crc_ok


def test_open_member_reader_refetch_path_when_local_extra_exceeds_margin():
    # A member whose LOCAL header carries a bigger extra field than the central one
    # (force_zip64 does this) must still extract correctly — with margin=0 the reader
    # cannot grab header+data in one shot and must issue a precise second Range read.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        with zf.open(zipfile.ZipInfo("big/vasprun.xml"), "w", force_zip64=True) as fh:
            fh.write(b"Z" * 5000)
    blob = buf.getvalue()
    entries = zipstream_mod.remote_central_directory("http://x/z.zip", _ZipRangeSession({"http://x/z.zip": blob}))
    entry = entries[0]
    assert entry.targetable                         # small actual size -> not ZIP64 in the CD
    sess = _ZipRangeSession({"http://x/z.zip": blob})
    reader = zipstream_mod.open_member_reader(sess, "http://x/z.zip", entry, margin=0)
    out = bytearray()
    while True:
        c = reader.read(1 << 16)
        if not c:
            break
        out.extend(c)
    reader.close()
    assert bytes(out) == zipfile.ZipFile(io.BytesIO(blob)).read("big/vasprun.xml")
    assert reader.complete
    assert sum(1 for _u, r in sess.requests if r and not r.startswith("bytes=-")) == 2  # 2 reads


def test_targeted_fetch_pulls_only_vasp_and_skips_heavy(tmp_path, monkeypatch):
    # The core win: a zip whose bulk is a heavy CHGCAR yields its VASP files while the
    # CHGCAR is never fetched (recorded as availability only), transferring far less than
    # the whole archive.
    monkeypatch.setattr(fetch_mod, "ZIP_STREAM_MIN_SKIP_BYTES", 100_000)
    members = [("run/vasprun.xml", b"<modeling>" + b"V" * 3000 + b"</modeling>"),
               ("run/OUTCAR", b"outcar " * 400), ("run/INCAR", b"ENCUT=520"),
               ("run/CHGCAR", os.urandom(1 << 20))]        # ~1 MB incompressible heavy file
    blob = _zip_with(members)
    dest = tmp_path / "extracted"
    sess = _ZipRangeSession({"http://x/a.zip": blob})
    result = _fetch_zip_targeted("http://x/a.zip", dest, sess, None, 128)
    assert result is not None
    names, extracted = result
    assert set(extracted) == {"run/vasprun.xml", "run/OUTCAR", "run/INCAR"}   # CHGCAR skipped
    ref = zipfile.ZipFile(io.BytesIO(blob))
    for n in extracted:
        assert (dest / n).read_bytes() == ref.read(n)
    assert not (dest / "run" / "CHGCAR").exists()          # heavy file never written
    assert "run/CHGCAR" in names                           # ...but still enumerated
    assert sess.bytes_served < len(blob)                   # transferred far less than whole


def test_targeted_fetch_proven_no_vasp_downloads_nothing(tmp_path):
    # Enumeration alone proves a zip holds no VASP -> return (names, []) and never pull a
    # single member (no explicit-range request), so a non-VASP zip costs one tail read.
    blob = _zip_with([("data/results.csv", b"a,b,c\n1,2,3\n"), ("README.txt", b"hi")])
    dest = tmp_path / "extracted"
    sess = _ZipRangeSession({"http://x/a.zip": blob})
    names, extracted = _fetch_zip_targeted("http://x/a.zip", dest, sess, None, 128)
    assert extracted == []
    assert set(names) == {"data/results.csv", "README.txt"}
    assert not dest.exists() or not any(dest.rglob("*"))
    assert all(r is None or r.startswith("bytes=-") for _u, r in sess.requests)  # no member fetch


def test_targeted_fetch_falls_back_when_too_many_members(tmp_path):
    # More target members than the budget -> None (one whole download is cheaper).
    blob = _zip_with([(f"c{i}/vasprun.xml", b"<x/>") for i in range(5)])
    sess = _ZipRangeSession({"http://x/a.zip": blob})
    assert _fetch_zip_targeted("http://x/a.zip", tmp_path / "e", sess, None, 3) is None


def test_targeted_fetch_falls_back_for_small_all_vasp_zip(tmp_path):
    # A small, ~all-VASP zip has nothing worth skipping -> fall back (one request beats N).
    blob = _zip_with([("run/vasprun.xml", b"<x/>"), ("run/OUTCAR", b"o")])
    sess = _ZipRangeSession({"http://x/a.zip": blob})
    assert _fetch_zip_targeted("http://x/a.zip", tmp_path / "e", sess, None, 128) is None


def test_targeted_fetch_zip_slip_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_mod, "ZIP_STREAM_MIN_SKIP_BYTES", 100_000)
    members = [("good/OUTCAR", b"good outcar"), ("../evil_OUTCAR", b"pwned"),
               ("heavy/CHGCAR", os.urandom(1 << 20))]
    blob = _zip_with(members)
    dest = tmp_path / "extracted"
    result = _fetch_zip_targeted("http://x/a.zip", dest, _ZipRangeSession({"http://x/a.zip": blob}),
                                 None, 128)
    assert result is not None
    _names, extracted = result
    assert extracted == ["good/OUTCAR"]                    # traversal member refused
    assert (dest / "good" / "OUTCAR").read_bytes() == b"good outcar"
    assert not (tmp_path / "evil_OUTCAR").exists()
    assert not (dest.parent / "evil_OUTCAR").exists()


def test_fetch_record_targeted_zip_end_to_end(tmp_path, monkeypatch):
    # Record-level: targeted fetch stages the VASP unit, records the CHGCAR as availability
    # only (never written), and transfers far less than the whole archive.
    monkeypatch.setattr(fetch_mod, "ZIP_STREAM_MIN_SKIP_BYTES", 100_000)
    members = [("run/vasprun.xml", b"<modeling>" + b"V" * 2000 + b"</modeling>"),
               ("run/OUTCAR", b"outcar " * 300), ("run/CHGCAR", os.urandom(1 << 20))]
    blob = _zip_with(members)
    rec = {"recid": "4242", "files": [{"key": "data.zip", "download": "http://x/data.zip",
                                       "size": len(blob),
                                       "checksum": "md5:" + md5(blob).hexdigest()}]}
    sess = _ZipRangeSession({"http://x/data.zip": blob})
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, sess, tmp_path / "raw", max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 1
    assert entry["availability"]["charge_density"] is True         # CHGCAR recorded...
    raw_files = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "CHGCAR" not in raw_files                               # ...but not on disk
    assert "vasprun.xml" in raw_files and "OUTCAR" in raw_files
    assert sess.bytes_served < len(blob)                          # much less than whole archive


def test_fetch_record_falls_back_to_whole_download_when_range_ignored(tmp_path):
    # A server that ignores Range: enumeration fails, so fetch_record must fall back to the
    # whole-archive download+extract and still produce the calc unit.
    members = [("run/vasprun.xml", b"<modeling>" + b"V" * 2000 + b"</modeling>"),
               ("run/OUTCAR", b"outcar " * 300)]
    blob = _zip_with(members)
    rec = {"recid": "4243", "files": [{"key": "data.zip", "download": "http://x/data.zip",
                                       "size": len(blob),
                                       "checksum": "md5:" + md5(blob).hexdigest()}]}
    sess = _ZipRangeSession({"http://x/data.zip": blob}, honor_range=False)
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, sess, tmp_path / "raw", max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 1
    raw_files = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "vasprun.xml" in raw_files and "OUTCAR" in raw_files


def test_fetch_record_zip_stream_disabled_never_range_peeks(tmp_path):
    # --no-zip-stream: targeted fetch is not attempted at all (no suffix-range peek), the
    # archive is whole-downloaded as before.
    members = [("run/vasprun.xml", b"<modeling>V</modeling>"), ("run/OUTCAR", b"o" * 50),
               ("run/CHGCAR", os.urandom(1 << 20))]
    blob = _zip_with(members)
    rec = {"recid": "4244", "files": [{"key": "data.zip", "download": "http://x/data.zip",
                                       "size": len(blob),
                                       "checksum": "md5:" + md5(blob).hexdigest()}]}
    sess = _ZipRangeSession({"http://x/data.zip": blob})
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, sess, tmp_path / "raw", max_bytes=None, rej=rej, zip_stream=False)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 1
    assert all(r is None or not r.startswith("bytes=-") for _u, r in sess.requests)  # no peek


def test_cli_fetch_wires_zip_stream_flags(tmp_path, monkeypatch):
    # --no-zip-stream / --zip-stream-max-files reach the fetch() call.
    import zenodo_harvest.cli as cli_mod
    captured: dict = {}

    def fake_fetch(in_path, **k):
        captured.update(k)
        return {"ok": True, "fetched": 0}

    monkeypatch.setattr(cli_mod, "fetch", fake_fetch)
    keep = tmp_path / "keep.jsonl"
    keep.write_text("")
    cli_mod.main(["fetch", "--in", str(keep)])                     # default
    assert captured["zip_stream"] is True
    cli_mod.main(["fetch", "--in", str(keep), "--no-zip-stream",
                  "--zip-stream-max-files", "7"])
    assert captured["zip_stream"] is False and captured["zip_stream_max_files"] == 7


# --------------------------------------------------------------------------- #
# Nested-archive recursion — extract VASP files from sub-archives (a .zip of   #
# per-run .tar.gz's, a .tar of .zip's, …), for ALL archive types, after        #
# download. The extractors pull out nested-archive members too; a recursion    #
# driver then unpacks each one, deleting it afterwards (mentor/user request).   #
# --------------------------------------------------------------------------- #

def _tar_gz_bytes(items):
    """Build an in-memory gzip-compressed tar from ``[(name, bytes), ...]``."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in items:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _fetch_whole(rec, session, raw_dir):
    rej = RejectionLogger(Path(raw_dir).parent / "rej.jsonl")
    entry = fetch_record(rec, session, raw_dir, max_bytes=None, rej=rej)
    rej.close()
    return entry


def _zip_rec(recid, key, blob):
    return {"recid": recid, "files": [{"key": key, "download": f"http://x/{key}",
            "size": len(blob), "checksum": "md5:" + md5(blob).hexdigest()}]}


def test_extract_zip_also_extracts_nested_archive_member(tmp_path):
    # The broadened extractor pulls out a nested archive member (so recursion can reach it),
    # alongside the VASP files — without touching the existing VASP-only behaviour.
    inner = _zip_with([("calc/OUTCAR", b"o")])
    arc = tmp_path / "a.zip"
    arc.write_bytes(_zip_with([("run/vasprun.xml", b"<x/>"), ("bundle.zip", inner)]))
    names, extracted = _extract_zip(arc, tmp_path / "extracted", 1 << 30)
    assert "bundle.zip" in extracted and "run/vasprun.xml" in extracted
    assert (tmp_path / "extracted" / "bundle.zip").exists()


def test_fetch_record_recurses_nested_targz_in_zip(tmp_path):
    # The headline case: a .zip whose VASP data lives inside a nested .tar.gz. Recursion
    # must extract the vasprun/OUTCAR, record a CHGCAR that lives inside the sub-archive as
    # availability, and delete the sub-archive afterwards.
    inner = _tar_gz_bytes([("calc/vasprun.xml", b"<modeling>V</modeling>"),
                           ("calc/OUTCAR", b"outcar"), ("calc/CHGCAR", b"heavy" * 100)])
    outer = _zip_with([("data/inner.tar.gz", inner), ("top/README", b"hi")])
    entry = _fetch_whole(_zip_rec("7001", "outer.zip", outer),
                         _MultiFileStreamSession({"http://x/outer.zip": outer}), tmp_path / "raw")
    assert entry is not None and entry["n_calc_units"] == 1
    assert entry["availability"]["charge_density"] is True          # CHGCAR inside sub-archive
    raw = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "vasprun.xml" in raw and "OUTCAR" in raw
    assert "CHGCAR" not in raw                                       # heavy -> availability only
    assert not any(p.name == "inner.tar.gz" for p in (tmp_path / "raw").rglob("*"))  # deleted


def test_fetch_record_recurses_two_levels(tmp_path):
    # zip -> a.zip -> b.tar -> vasprun/OUTCAR: recursion must descend more than one level.
    b = _tar_bytes([("calc/vasprun.xml", b"<x/>"), ("calc/OUTCAR", b"o")])
    a = _zip_with([("sub/b.tar", b)])
    outer = _zip_with([("deep/a.zip", a)])
    entry = _fetch_whole(_zip_rec("7002", "outer.zip", outer),
                         _MultiFileStreamSession({"http://x/outer.zip": outer}), tmp_path / "raw")
    assert entry is not None and entry["n_calc_units"] == 1
    raw = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "vasprun.xml" in raw and "OUTCAR" in raw
    assert not any(p.suffix in {".zip", ".tar"} for p in (tmp_path / "raw").rglob("*"))


def test_fetch_record_recurses_zip_inside_tar(tmp_path):
    # Cross-type: the TOP archive is a .tar containing a nested .zip. Recursion is
    # archive-type-agnostic, so the zip's VASP files are still extracted.
    inner = _zip_with([("calc/vasprun.xml", b"<x/>"), ("calc/OUTCAR", b"o")])
    outer = _tar_bytes([("nested/inner.zip", inner)])
    entry = _fetch_whole(_zip_rec("7003", "outer.tar", outer),
                         _MultiFileStreamSession({"http://x/outer.tar": outer}), tmp_path / "raw")
    assert entry is not None and entry["n_calc_units"] == 1
    raw = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "vasprun.xml" in raw and "OUTCAR" in raw


def test_nested_recursion_depth_cap(tmp_path, monkeypatch):
    # A depth cap guards an archive-quine: with the cap at 1, only the first nested level
    # is unpacked and the deeper one is logged, not descended into forever.
    monkeypatch.setattr(fetch_mod, "_MAX_NEST_DEPTH", 1)
    b = _tar_bytes([("calc/vasprun.xml", b"<x/>"), ("calc/OUTCAR", b"o")])
    a = _zip_with([("sub/b.tar", b)])
    outer = _zip_with([("deep/a.zip", a)])
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(_zip_rec("7004", "outer.zip", outer),
                         _MultiFileStreamSession({"http://x/outer.zip": outer}),
                         tmp_path / "raw", max_bytes=None, rej=rej)
    rej.close()
    reasons = [r["reason"] for r in read_jsonl(tmp_path / "rej.jsonl")]
    assert "archive_nesting_too_deep" in reasons
    # b.tar was reached (depth 1) but not descended into -> no vasprun surfaced -> no unit
    assert entry is None


def test_targeted_fetch_falls_back_on_nested_archive(tmp_path):
    # A zip containing a nested archive can't be targeted (the sub-archive's members are
    # invisible to the central-directory enumeration) -> None, so the caller whole-downloads
    # and the recursion path unpacks it.
    inner = _tar_bytes([("calc/vasprun.xml", b"<x/>"), ("calc/OUTCAR", b"o")])
    outer = _zip_with([("data/inner.tar", inner), ("run/CHGCAR", os.urandom(1 << 20))])
    sess = _ZipRangeSession({"http://x/o.zip": outer})
    assert _fetch_zip_targeted("http://x/o.zip", tmp_path / "e", sess, None, 128) is None


def test_fetch_record_targeted_falls_back_and_recurses(tmp_path):
    # End-to-end over a Range-honouring server: targeted declines (nested archive), so
    # fetch_record whole-downloads (a no-Range GET) and recursion extracts the sub-archive.
    inner = _tar_bytes([("calc/vasprun.xml", b"<modeling>V</modeling>"), ("calc/OUTCAR", b"o")])
    outer = _zip_with([("data/inner.tar", inner)])
    sess = _ZipRangeSession({"http://x/outer.zip": outer})
    entry = _fetch_whole(_zip_rec("7005", "outer.zip", outer), sess, tmp_path / "raw")
    assert entry is not None and entry["n_calc_units"] == 1
    assert any(r is None for _u, r in sess.requests)     # a whole-file (no-Range) GET happened
    raw = [p.name for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert "vasprun.xml" in raw and "OUTCAR" in raw
