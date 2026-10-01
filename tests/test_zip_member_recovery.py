"""Zip archives fetch could not extract although their central directory listed VASP outputs.

Census T1 (2026-10-01) lost two such evidenced records:

* ``14809725`` — ONE member's local header is unreadable (``BadZipFile: Bad magic number for file
  header``); ``_extract_zip`` let that abort the whole archive, so every member after it was lost
  (97 calc units kept, ~110 single points dropped). Now the member is skipped, the rest extracted,
  and one ``extract_partial`` rejection names the casualties.
* ``3359829`` — a 5.4 GB zip, ``BadZipFile: Truncated file header``: a >4 GiB zip written without
  ZIP64 records keeps only the low 32 bits of each offset, zipfile reads the 4 GiB gap as
  prepended data and shifts every member by it. ``_open_zip_member`` retries a member at its
  offset ±k·4 GiB, accepted only when zipfile itself finds the signature AND the exact name there
  (and the CRC-32 checks out on read).

The wrap case is reproduced here with the module's ``_ZIP_OFFSET_WRAP`` monkeypatched to a few KB
so the same arithmetic runs on a small archive.
"""

from __future__ import annotations

import io
import json
import struct
import zipfile
from hashlib import md5

import pytest

from zenodo_harvest import fetch as fetch_mod
from zenodo_harvest.fetch import (PartialExtract, StagingBudget, _extract_zip, _open_zip_member,
                                  fetch_record)
from zenodo_harvest.manifest import RejectionLogger, read_jsonl

MEMBERS = [(f"calc{i}/OUTCAR", (f"OUTCAR {i} " * 300).encode()) for i in range(6)]


def _zip(members, method=zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", method) as z:
        for name, data in members:
            z.writestr(name, data)
    return buf.getvalue()


def _local_offsets(blob: bytes) -> dict[str, int]:
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return {i.filename: i.header_offset for i in z.infolist()}


def _break_local_header(blob: bytes, name: str) -> bytes:
    off = _local_offsets(blob)[name]
    assert blob[off:off + 4] == b"PK\x03\x04"
    return blob[:off] + b"XX\x03\x04" + blob[off + 4:]


def _wrap_offsets(blob: bytes, wrap: int) -> bytes:
    """What a non-ZIP64 writer emits past the 32-bit limit, at a small ``wrap``: every offset in
    the central directory (and the EOCD's directory offset) reduced modulo ``wrap``."""
    e = blob.rfind(b"PK\x05\x06")
    n, cd_size, cd_off = struct.unpack("<HII", blob[e + 10:e + 20])
    cd = bytearray(blob[cd_off:cd_off + cd_size])
    i = 0
    for _ in range(n):
        assert cd[i:i + 4] == b"PK\x01\x02"
        n_len, e_len, c_len = struct.unpack("<HHH", cd[i + 28:i + 34])
        (off,) = struct.unpack("<I", cd[i + 42:i + 46])
        cd[i + 42:i + 46] = struct.pack("<I", off % wrap)
        i += 46 + n_len + e_len + c_len
    eocd = bytearray(blob[e:])
    eocd[16:20] = struct.pack("<I", cd_off % wrap)
    return blob[:cd_off] + bytes(cd) + blob[cd_off + cd_size:e] + bytes(eocd)


def test_one_unreadable_member_no_longer_costs_the_archive(tmp_path):
    arc = tmp_path / "a.zip"
    arc.write_bytes(_break_local_header(_zip(MEMBERS), "calc2/OUTCAR"))
    with pytest.raises(PartialExtract) as ei:
        _extract_zip(arc, tmp_path / "out", 1 << 30)
    pe = ei.value
    assert sorted(pe.extracted) == sorted(n for n, _ in MEMBERS if n != "calc2/OUTCAR")
    assert len(pe.failures) == 1 and pe.failures[0].startswith("calc2/OUTCAR: BadZipFile")
    for name, data in MEMBERS:
        out = tmp_path / "out" / name
        assert (out.read_bytes() == data) if name != "calc2/OUTCAR" else not out.exists()


def test_crc_mismatch_member_is_removed_and_refunded(tmp_path):
    blob = bytearray(_zip(MEMBERS, zipfile.ZIP_STORED))
    off = _local_offsets(bytes(blob))["calc3/OUTCAR"]
    n_len, e_len = struct.unpack("<HH", blob[off + 26:off + 30])
    blob[off + 30 + n_len + e_len + 5] ^= 0xFF               # flip a data byte: CRC now fails
    arc = tmp_path / "a.zip"
    arc.write_bytes(bytes(blob))
    budget = StagingBudget(max_bytes=10 << 20, max_files=1000)
    with pytest.raises(PartialExtract) as ei:
        _extract_zip(arc, tmp_path / "out", 1 << 30, budget)
    assert "Bad CRC-32" in ei.value.failures[0]
    assert not (tmp_path / "out" / "calc3" / "OUTCAR").exists()   # no half/corrupt file kept
    kept = sum(len(d) for n, d in MEMBERS if n != "calc3/OUTCAR")
    assert budget.used_bytes == kept                         # the bad member's bytes refunded


def test_nothing_readable_is_still_a_whole_archive_error(tmp_path):
    blob = _zip(MEMBERS)
    for name, _ in MEMBERS:
        blob = _break_local_header(blob, name)
    arc = tmp_path / "a.zip"
    arc.write_bytes(blob)
    with pytest.raises(zipfile.BadZipFile, match="no member readable"):
        _extract_zip(arc, tmp_path / "out", 1 << 30)


@pytest.mark.parametrize("method", [zipfile.ZIP_DEFLATED, zipfile.ZIP_STORED])
def test_wrapped_offsets_of_a_non_zip64_large_zip_are_recovered(tmp_path, monkeypatch, method):
    blob = _zip(MEMBERS, method)
    wrap = len(blob) // 5                                     # several wraps, like 5.4 GB / 4 GiB
    monkeypatch.setattr(fetch_mod, "_ZIP_OFFSET_WRAP", wrap)
    arc = tmp_path / "big.zip"
    arc.write_bytes(_wrap_offsets(blob, wrap))
    with zipfile.ZipFile(arc) as z:                           # plain zipfile: the T1 failure
        with pytest.raises(zipfile.BadZipFile):
            z.open(z.infolist()[0]).read()
    names, extracted = _extract_zip(arc, tmp_path / "out", 1 << 30)
    assert sorted(extracted) == sorted(n for n, _ in MEMBERS)
    for name, data in MEMBERS:
        assert (tmp_path / "out" / name).read_bytes() == data


def test_shifted_candidate_needs_the_right_name(tmp_path, monkeypatch):
    """A shift that lands on ANOTHER member's valid header is refused (name check), so a
    member is never extracted with the wrong bytes."""
    monkeypatch.setattr(fetch_mod, "_ZIP_OFFSET_WRAP", 1)     # every offset is a "candidate"
    blob = _break_local_header(_zip(MEMBERS[:2]), "calc1/OUTCAR")
    arc = tmp_path / "a.zip"
    arc.write_bytes(blob)
    with zipfile.ZipFile(arc) as z:
        info = z.getinfo("calc1/OUTCAR")
        with pytest.raises(zipfile.BadZipFile):
            _open_zip_member(z, info, len(blob))


class _Resp:
    def __init__(self, content: bytes):
        self.status_code, self._content = 200, content
        self.headers = {"Content-Length": str(len(content))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, n):
        for i in range(0, len(self._content), n):
            yield self._content[i:i + n]

    @property
    def content(self):
        return self._content

    def close(self):
        pass


class _Session:
    """Serves whole files (a Range request gets a 200 too, so targeted zip fetch falls back to
    the whole download — the path both T1 losses took)."""

    def __init__(self, blobs):
        self.blobs = blobs

    def get(self, url, stream=False, timeout=None, headers=None):
        return _Resp(self.blobs[url])

    def close(self):
        pass


def test_fetch_record_keeps_good_members_and_logs_extract_partial(tmp_path):
    blob = _break_local_header(_zip(MEMBERS), "calc4/OUTCAR")
    rec = {"recid": "14809725",
           "files": [{"key": "dft.zip", "download": "http://x/dft.zip", "size": len(blob),
                      "checksum": "md5:" + md5(blob).hexdigest()}]}
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, _Session({"http://x/dft.zip": blob}), tmp_path / "raw",
                         max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == len(MEMBERS) - 1
    rows = list(read_jsonl(tmp_path / "rej.jsonl"))
    assert [(r["id"], r["reason"], r["n_failed"]) for r in rows] == [
        ("14809725:dft.zip", "extract_partial", 1)]
    assert "calc4/OUTCAR" in rows[0]["detail"]
    assert not (tmp_path / "raw" / "14809725" / "dft.zip").exists()   # archive dropped as usual


def test_nested_zip_with_a_bad_member_is_partially_extracted(tmp_path):
    inner = _break_local_header(_zip(MEMBERS[:3]), "calc0/OUTCAR")
    outer = _zip([("runs/inner.zip", inner)], zipfile.ZIP_STORED)
    rec = {"recid": "1",
           "files": [{"key": "outer.zip", "download": "http://x/o.zip", "size": len(outer),
                      "checksum": "md5:" + md5(outer).hexdigest()}]}
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    entry = fetch_record(rec, _Session({"http://x/o.zip": outer}), tmp_path / "raw",
                         max_bytes=None, rej=rej)
    rej.close()
    assert entry is not None and entry["n_calc_units"] == 2
    rows = list(read_jsonl(tmp_path / "rej.jsonl"))
    assert [r["reason"] for r in rows] == ["extract_partial"]
    assert rows[0]["id"].startswith("1:outer.zip/") and json.dumps(rows[0])
