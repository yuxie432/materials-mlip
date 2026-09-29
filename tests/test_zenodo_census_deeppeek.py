"""Regression tests for the census deep peeks (zenodo_census/deeppeek.py) — each pins a defect an
independent review found on 2026-09-29 (false proofs that would prune real VASP as "VASP-free",
a py7zr spin, budget leaks, inverted ranges, a lost extractor, a changed evidence rule), plus the
helpers come from tests/test_zenodo_census.py."""

from __future__ import annotations

import io
import os
import struct
import tarfile
import time
import zipfile
import zlib
from pathlib import Path
from typing import Any

import pytest

from test_zenodo_census import (
    FakeRangeSession,
    FakeResp,
    _write_jsonl,
    compress,
    make_tar,
    url,
    zhit,
)
from zenodo_census import census as zc
from zenodo_census import deeppeek as dp
from zenodo_census import headpeek as hp
from zenodo_census import score as zs
from zenodo_census import signals as sg
from zenodo_census import triage as zt
from zenodo_harvest import fetch
from zenodo_harvest.manifest import read_jsonl

D, S = zipfile.ZIP_DEFLATED, zipfile.ZIP_STORED


def _zip_members(members: dict[str, tuple[bytes, int]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, (data, method) in members.items():
            zf.writestr(zipfile.ZipInfo(name), data, compress_type=method)
    return buf.getvalue()


def _served(sess: FakeRangeSession, total: int) -> int:
    n = 0
    for _, h in sess.requests:
        a, b = h["Range"][6:].split("-")
        n += min(int(b), total - 1) - int(a) + 1
    return n


def _t(title: str, rid: str, files: dict[str, bytes], tmp_path: Path) -> tuple[Path, dict]:
    census = _write_jsonl(tmp_path / "census.jsonl",
                          [zc.slim_hit(zhit(rid, dict(files), concept=str(int(rid) - 1),
                                            title=title))])
    zs.score(census, tmp_path / "scored.jsonl", excl=zs.Exclusions(), seeds=sg.Seeds())
    return census, {url(rid, k): b for k, b in files.items()}


def _triage(tmp_path: Path, census: Path, blobs: dict, **kw: Any) -> dict:
    return zt.triage(tmp_path / "scored.jsonl", census, tmp_path / "keep.jsonl",
                     session_factory=lambda: FakeRangeSession(blobs), residual_sample=0,
                     negative_sample=0, interval=0, head_bytes=64 << 10, **kw)


# --- 7z entries stored WITHOUT a name (7-Zip's -si) ------------------------------------------

def _nameless_7z(payload: bytes) -> bytes:
    """A real 7z whose single entry has no kName property (as 7-Zip writes for `-si`)."""
    py7zr = pytest.importorskip("py7zr")
    buf = io.BytesIO()
    with py7zr.SevenZipFile(buf, "w") as z:
        z.set_encoded_header_mode(False)
        z.writestr(payload, "PLACEHOLDER")
    blob = bytearray(buf.getvalue())
    ofs, hsize, _ = struct.unpack("<QQI", blob[12:32])
    hstart = 32 + ofs
    header = bytearray(blob[hstart:hstart + hsize])
    name = "PLACEHOLDER".encode("utf-16-le") + b"\0\0"
    i = header.find(name)
    header[i - 3] = 0x19                                   # kName -> kDummy: no name stored
    blob[hstart:hstart + hsize] = header
    blob[28:32] = struct.pack("<I", zlib.crc32(bytes(header)) & 0xFFFFFFFF)
    blob[8:12] = struct.pack("<I", zlib.crc32(bytes(blob[12:32])) & 0xFFFFFFFF)
    return bytes(blob)


def test_a_nameless_7z_entry_is_named_as_the_fetch_names_it_or_left_partial(tmp_path):
    tar = make_tar({"calc/INCAR": b"ENCUT=500", "calc/vasprun.xml": b"<modeling/>"})
    blob = _nameless_7z(tar)
    arc = tmp_path / "calcs.tar.7z"
    arc.write_bytes(blob)
    assert fetch._extract_7z(arc, tmp_path / "x", 1 << 30)[0] == ["calcs.tar"]  # fetch's name
    f = {"key": "calcs.tar.7z", "size": len(blob), "links": {"self": "u"}}
    ev = dp.sevenzip_peek(FakeRangeSession({"u": blob}), "u", len(blob), name="calcs.tar.7z")
    assert ev["complete"] and ev["n_nested"] == 1                  # the same name: a nested tar
    assert zt.file_verdict(f, ev)[0] == "unresolved"               # ... so never "empty"
    ev = dp.sevenzip_peek(FakeRangeSession({"u": blob}), "u", len(blob))      # no name given
    assert ev["status"] == "ok" and not ev["complete"]              # unknown content: partial
    assert zt.file_verdict(f, ev)[0] != "empty"
    single = _nameless_7z(b"<modeling/>")
    ev = dp.sevenzip_peek(FakeRangeSession({"u": single}), "u", len(single),
                          name="vasprun.xml.7z")
    assert ev["n_primary"] == 1


def test_triage_keeps_a_t1_record_whose_7z_holds_a_nameless_vasp_tar(tmp_path):
    blob = _nameless_7z(make_tar({"calc/INCAR": b"1", "calc/vasprun.xml": b"<modeling/>"}))
    census, blobs = _t("Ab initio DFT calculations of perovskites", "8001",
                       {"calcs.tar.7z": blob}, tmp_path)
    rep = _triage(tmp_path, census, blobs)
    assert {r["recid"] for r in read_jsonl(tmp_path / "keep.jsonl")} == {"8001"}
    assert rep["decisions"] == {"T1:strong_unresolved": 1}          # downloaded whole


# --- tar: tarfile's own hop rules --------------------------------------------------------------

def _pax_big_member_tar(data: bytes, after: dict[str, bytes]) -> bytes:
    """The layout tarfile writes for a member >= 8 GiB: ustar size 0, real size in PAX `size`."""
    ti = tarfile.TarInfo("proj/huge_wavefunction.bin")
    ti.size = 0
    ti.pax_headers = {"size": str(len(data))}
    out = io.BytesIO()
    out.write(ti.tobuf(tarfile.PAX_FORMAT, "utf-8", "surrogateescape"))
    out.write(data + b"\0" * (-len(data) % 512))
    for name, body in after.items():
        t2 = tarfile.TarInfo(name)
        t2.size = len(body)
        out.write(t2.tobuf(tarfile.PAX_FORMAT, "utf-8", "surrogateescape"))
        out.write(body + b"\0" * (-len(body) % 512))
    out.write(b"\0" * 1024)
    return out.getvalue()


@pytest.mark.parametrize("big", [b"\0" * 4096 + b"\x01" * 60_000, b"\x01" * 60_000])
def test_tar_walks_honour_the_pax_size_record(big):
    tar = _pax_big_member_tar(big, {"proj/calc/vasprun.xml": b"<modeling/>"})
    assert "proj/calc/vasprun.xml" in [m.name for m in tarfile.open(fileobj=io.BytesIO(tar))]
    ev = dp.tar_walk(FakeRangeSession({"u": tar}), "u", len(tar), stop_on_primary=False)
    assert ev["n_primary"] == 1 and ev["complete"] and ev["n_members"] == 2
    names, end_seen, _ = hp.walk_tar(tar)                           # the head-peek walk too
    assert "proj/calc/vasprun.xml" in names and end_seen


def _gnu_sparse_member(name: str, data: bytes) -> bytes:
    ti = tarfile.TarInfo(name)
    ti.size = len(data)
    hdr = bytearray(ti.tobuf(tarfile.GNU_FORMAT))
    hdr[156:157] = b"S"
    hdr[386:398] = b"%011o\0" % 0
    hdr[398:410] = b"%011o\0" % len(data)
    hdr[483:495] = b"%011o\0" % len(data)
    hdr[148:156] = b" " * 8
    hdr[148:156] = b"%06o\0 " % sum(hdr)
    return bytes(hdr) + data + b"\0" * (-len(data) % 512)


def test_tar_walks_list_gnu_sparse_members_and_hop_sized_directories_like_tarfile():
    body = b"<modeling/>" * 10
    tar = (make_tar({"calc/notes.txt": b"n"})[:-10240]
           + _gnu_sparse_member("calc/vasprun.xml", body) + b"\0" * 1024)
    assert [m.isfile() for m in tarfile.open(fileobj=io.BytesIO(tar))][-1]    # fetch: a file
    ev = dp.tar_walk(FakeRangeSession({"u": tar}), "u", len(tar), stop_on_primary=False)
    assert ev["n_primary"] == 1 and ev["complete"]
    assert "calc/vasprun.xml" in hp.walk_tar(tar)[0]
    d = tarfile.TarInfo("calc")
    d.type = tarfile.DIRTYPE
    hdr = bytearray(d.tobuf(tarfile.USTAR_FORMAT))
    hdr[124:136] = b"%011o\0" % 1024                    # a directory header with a size field
    hdr[148:156] = b" " * 8
    hdr[148:156] = b"%06o\0 " % sum(hdr)
    tar = bytes(hdr) + make_tar({"calc/vasprun.xml": b"<modeling/>"})
    assert [m.name for m in tarfile.open(fileobj=io.BytesIO(tar)) if m.isfile()] == [
        "calc/vasprun.xml"]
    ev = dp.tar_walk(FakeRangeSession({"u": tar}), "u", len(tar), stop_on_primary=False)
    assert ev["n_primary"] == 1 and ev["complete"]
    assert hp.walk_tar(tar)[0] == ["calc/vasprun.xml"]


def test_tar_walk_does_not_stop_at_an_appledouble_sidecar():
    tar = make_tar({"calc/traj.nc": b"x" * (3 << 20), "calc/._vasprun.xml": b"\0\x05\x16\x07" * 64,
                    "calc/vasprun.xml": b"<modeling/>"})
    ev = dp.tar_walk(FakeRangeSession({"u": tar}), "u", len(tar))
    assert ev["n_primary"] == 1


# --- 7z: py7zr can spin forever on a corrupt packed header -> a killable child ----------------

def _lzma1_header_7z(cut: bool) -> bytes:
    import lzma
    py7zr = pytest.importorskip("py7zr")
    try:
        from py7zr.archiveinfo import Folder, HeaderStreamsInfo
    except ImportError:                                  # pragma: no cover - other py7zr
        pytest.skip("py7zr internals differ")
    buf = io.BytesIO()
    with py7zr.SevenZipFile(buf, "w") as z:
        z.set_encoded_header_mode(False)
        for i in range(50):
            z.writestr(b"x" * 100, f"calc/run{i}/OUTCAR")
    blob = buf.getvalue()
    ofs, hsize, _ = struct.unpack("<QQI", blob[12:32])
    header, body = blob[32 + ofs:32 + ofs + hsize], blob[32:32 + ofs]
    filters = [{"id": lzma.FILTER_LZMA1, "preset": 7}]
    packed = lzma.compress(header, format=lzma.FORMAT_RAW, filters=filters)
    if cut:
        packed = packed[:len(packed) // 2]
    folder = Folder()
    folder.prepare_coderinfo(filters=filters)
    folder.unpacksizes = [len(header)]
    folder.crc = zlib.crc32(header) & 0xFFFFFFFF
    hs = HeaderStreamsInfo()
    hs.unpackinfo.folders = [folder]
    hs.packinfo.packpos = len(body)
    hs.packinfo.enable_digests = False
    hs.packinfo.numstreams = 1
    hs.packinfo.packsizes = [len(packed)]
    hs.packinfo.crcs = [zlib.crc32(packed) & 0xFFFFFFFF]
    out = io.BytesIO()
    hs.write(out)
    enc = out.getvalue()
    sig = bytearray(blob[:32])
    sig[12:20] = struct.pack("<Q", len(body) + len(packed))
    sig[20:28] = struct.pack("<Q", len(enc))
    sig[28:32] = struct.pack("<I", zlib.crc32(enc) & 0xFFFFFFFF)
    sig[8:12] = struct.pack("<I", zlib.crc32(bytes(sig[12:32])) & 0xFFFFFFFF)
    return bytes(sig) + body + packed + enc


def test_sevenzip_peek_terminates_on_a_header_py7zr_spins_on(monkeypatch):
    good = _lzma1_header_7z(cut=False)
    ev = dp.sevenzip_peek(FakeRangeSession({"u": good}), "u", len(good), name="runs.7z")
    assert ev["status"] == "ok" and ev["n_primary"] == 50
    monkeypatch.setattr(dp, "SEVENZIP_TIMEOUT", 3.0)
    bad = _lzma1_header_7z(cut=True)
    t0 = time.monotonic()
    ev = dp.sevenzip_peek(FakeRangeSession({"u": bad}), "u", len(bad), name="runs.7z")
    assert time.monotonic() - t0 < 15
    assert ev["status"] == "unreadable: timeout"                     # deterministic: cached


# --- nested zips ------------------------------------------------------------------------------

def test_a_transient_failure_inside_a_nested_tar_walk_is_never_cached(tmp_path, monkeypatch):
    monkeypatch.setattr(zt.time, "sleep", lambda *_: None)
    monkeypatch.setattr(hp.time, "sleep", lambda *_: None)
    tar = make_tar({"proj/traj.nc": os.urandom(3 << 20), "proj/calc/vasprun.xml": b"<modeling/>"})
    outer = _zip_members({"calc_data.tar": (tar, S)})
    census, blobs = _t("Thin films of oxides", "6003", {"bundle.zip": outer}, tmp_path)

    class Flaky(FakeRangeSession):       # reads deep inside the member fail (a one-off outage)
        def get(self, u: str, headers: dict | None = None, **kw: Any) -> FakeResp:
            a = int((headers or {}).get("Range", "bytes=0-0")[6:].split("-")[0] or 0)
            if (5 << 19) < a < len(outer) - 4096:
                self.requests.append((u, headers or {}))
                return FakeResp(503)
            return super().get(u, headers, **kw)

    zt.triage(tmp_path / "scored.jsonl", census, tmp_path / "keep.jsonl",
              session_factory=lambda: Flaky(blobs), residual_sample=0, negative_sample=0,
              interval=0, head_bytes=64 << 10)
    assert list(read_jsonl(tmp_path / "keep.jsonl")) == []            # not found this time ...
    _triage(tmp_path, census, blobs)                                  # ... but re-peeked next run
    assert {r["recid"] for r in read_jsonl(tmp_path / "keep.jsonl")} == {"6003"}


def test_every_nested_read_is_charged_to_the_budget_and_no_range_is_inverted():
    members = {f"d/{i:06d}/data_{i}.csv": (b"1", S) for i in range(3000)}
    members["z.tar.gz"] = (compress(make_tar({"a.txt": b"a"}), "gz"), D)
    outer = _zip_members(members)
    sess = FakeRangeSession({"u": outer})
    ev = dp.nested_zip_peek(sess, "u", len(outer), max_bytes=16 << 10)
    assert _served(sess, len(outer)) <= 16 << 10 and not ev["complete"]  # the outer CD too
    small = make_tar({f"imgs/f{i:05d}.png": os.urandom(2048) for i in range(3000)})
    outer = _zip_members({"a_calc.tar": (small, S), "b_calc.tar": (small, S)})
    sess = FakeRangeSession({"u": outer})
    ev = dp.nested_zip_peek(sess, "u", len(outer), max_bytes=4 << 20)
    assert _served(sess, len(outer)) <= 4 << 20 and ev["bytes_read"] <= 4 << 20
    tgz = compress(make_tar({"run/vasprun.xml": b"<modeling/>"}), "gz")
    bad = bytearray(_zip_members({"calc.tar.gz": (tgz, D)}))
    i = bad.find(b"PK\x01\x02")
    bad[i + 42:i + 46] = struct.pack("<I", len(bad) + 100)             # local offset past EOF
    sess = FakeRangeSession({"u": bytes(bad)})
    ev = dp.nested_zip_peek(sess, "u", len(bad))
    for _, h in sess.requests:
        a, b = h["Range"][6:].split("-")
        assert int(b) >= int(a), h["Range"]
    assert ev["status"] == "ok" and not ev["complete"]                 # deterministic, cached


def test_outer_vasp_inputs_are_not_new_evidence():
    outer = _zip_members({"inputs/INCAR": (b"ENCUT=1", D), "inputs/POSCAR": (b"p", D),
                          "figs.rar": (b"Rar!\x1a\x07\x01\x00" + b"z" * 50, S)})
    sess = FakeRangeSession({"u": outer})
    f = {"key": "bundle.zip", "size": len(outer), "links": {"self": "u"}}
    std = zt.peek_file(sess, "zip", f)
    deep = dp.nested_zip_peek(sess, "u", len(outer))
    assert zt.file_verdict(f, deep)[0] == zt.file_verdict(f, std)[0] == "unresolved"


@pytest.mark.parametrize("title,inner,expect", [
    ("Thin films of oxides", "calc_data.tar.gz", "vasp_evidence"),          # T2: found
    ("Ab initio DFT calculations of perovskites", "calc_data.rar", "strong_unresolved")])  # T1
def test_the_deep_overlay_keeps_the_aiida_export_extractor(tmp_path, title, inner, expect):
    payload = (compress(make_tar({"run/vasprun.xml": b"<modeling/>"}), "gz")
               if inner.endswith(".gz") else b"Rar!\x1a\x07\x01\x00" + b"z" * 60)
    export = _zip_members({f"nodes/ab/cd/0123-uuid/path/{inner}": (payload, D),
                           "nodes/ab/cd/0123-uuid/path/aiida.in": (b"&control\n/", D)})
    census, blobs = _t(title, "7003", {"export.aiida": export}, tmp_path)
    _triage(tmp_path, census, blobs)
    entry = next(iter(read_jsonl(tmp_path / "keep.jsonl")))
    assert entry["census"]["triage_reason"] == expect
    assert entry["files"][0].get("archive_kind") == "zip"        # fetch can extract it
