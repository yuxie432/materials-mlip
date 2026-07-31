"""Bare single-compression files inside an archive (``parameters.csv.bz2``,
``band_data.json.gz``) must NOT be treated as nested tarballs.

Regression for recid 15307432: a ``TPD.tgz`` holding ~158k ``parameters.csv.bz2`` data
files produced 158k ``extract_error`` rejections (each failing ``tarfile.open``) and
stalled the pipeline for hours. The misnamed-tarball heuristic is kept for a record's
TOP-LEVEL file (``_is_archive``) but off for MEMBERS (``_nested_archive_kind``); a
per-record failure cap is the hard backstop.
"""
from __future__ import annotations

import bz2
import io
import json
import tarfile
import zipfile
from pathlib import Path

from zenodo_harvest.fetch import (
    _MAX_NESTED_EXTRACT_ERRORS, _extract_tar, _is_archive, _nested_archive_kind,
    _recurse_nested_archives, _want_member)
from zenodo_harvest.manifest import RejectionLogger


def _reasons(path: Path) -> list[str]:
    return [json.loads(line)["reason"] for line in path.read_text().splitlines() if line.strip()]


def _add(tf: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tf.addfile(info, io.BytesIO(data))


def test_nested_archive_kind_vs_is_archive():
    # genuine multi-file archives -> a nested archive under BOTH predicates
    for real in ("run.zip", "data.tar", "data.tar.gz", "data.tgz", "data.tar.bz2",
                 "data.tar.xz", "data.tar.zst", "data.tzst", "run.rar", "run.7z"):
        assert _nested_archive_kind(real) is not None, real
        assert _is_archive(real) is not None, real
    # bare single-compression data files -> NOT a nested archive (the flood culprit)
    for bare in ("parameters.csv.bz2", "band_data.json.gz", "notes.txt.xz", "blob.zst"):
        assert _nested_archive_kind(bare) is None, bare
    # ...yet the TOP-LEVEL misnamed-tarball heuristic in _is_archive is preserved
    assert _is_archive("research_data.gz") == "tar"
    assert _nested_archive_kind("research_data.gz") is None


def test_want_member_skips_bare_compressed_but_keeps_vasp_and_real_archives():
    assert _want_member("vasprun.xml")
    assert _want_member("OUTCAR.gz")            # compressed VASP primary -> still wanted
    assert _want_member("vasprun.xml.gz")       # compressed VASP primary -> still wanted
    assert _want_member("nested.tar.gz")        # genuine nested archive -> still wanted
    assert not _want_member("parameters.csv.bz2")     # the culprit
    assert not _want_member("band_structure.json.gz")
    assert not _want_member("blob.zst")


def test_member_cap_for_bare_vasp_gz_keeps_bomb_guard():
    # a compressed VASP primary is a real file -> keeps the member cap (not the archive
    # bypass), while a genuine nested archive gets the container bypass.
    from zenodo_harvest.fetch import _member_cap_for
    assert _member_cap_for("OUTCAR.gz", member_cap=123) == 123
    assert _member_cap_for("nested.tar.gz", member_cap=123) > 123


def test_extract_and_recurse_no_flood_on_bare_compressed(tmp_path: Path):
    """A TPD.tgz-shaped archive: one real vasprun.xml among many ``.csv.bz2``. Only the
    vasprun is extracted; the bz2 data files are neither staged nor recursed, and the
    recursion emits ZERO ``extract_error``."""
    arc = tmp_path / "TPD.tgz"
    with tarfile.open(arc, "w:gz") as tf:
        _add(tf, "TPD/run/vasprun.xml", b"<modeling></modeling>")
        for i in range(64):
            _add(tf, f"TPD/UQ/mechanisms/File_{i}/parameters.csv.bz2",
                 bz2.compress(b"a,b,c\n1,2,3\n"))
    dest = tmp_path / "out"
    _names, extracted = _extract_tar(arc, dest, member_cap=1 << 30, budget=None)
    assert {Path(e).name for e in extracted} == {"vasprun.xml"}, extracted
    assert not any(p.name.endswith(".csv.bz2") for p in dest.rglob("*")), "bz2 was staged!"
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    _recurse_nested_archives(dest, 1 << 30, None, rej, "15307432:TPD.tgz")
    rej.close()
    assert "extract_error" not in _reasons(tmp_path / "rej.jsonl")


def test_genuine_nested_archive_still_recursed(tmp_path: Path):
    """A real nested ``.zip`` inside a ``.tar`` is still unpacked (no regression)."""
    inner = tmp_path / "inner.zip"
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("calc/OUTCAR", "TITEL = PAW_PBE\n")
    outer = tmp_path / "outer.tar"
    with tarfile.open(outer, "w") as tf:
        tf.add(inner, arcname="sub/inner.zip")
    dest = tmp_path / "out"
    _names, _extracted = _extract_tar(outer, dest, member_cap=1 << 30, budget=None)
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    _extra_names, extra_vasp, _tr = _recurse_nested_archives(
        dest, 1 << 30, None, rej, "rec:outer.tar")
    rej.close()
    assert any(Path(v).name == "OUTCAR" for v in extra_vasp), extra_vasp
    assert "extract_error" not in _reasons(tmp_path / "rej.jsonl")


def test_recurse_caps_extract_error_flood(tmp_path: Path):
    """Belt-and-suspenders: a pile of genuinely-corrupt REAL nested archives is bounded —
    after the cap one summary rejection replaces the per-member flood, and the remaining
    archives are left unprocessed."""
    root = tmp_path / "extracted"
    root.mkdir()
    for i in range(8):
        (root / f"corrupt_{i}.zip").write_bytes(b"not a zip file")
    rej = RejectionLogger(tmp_path / "rej.jsonl")
    _recurse_nested_archives(root, 1 << 30, None, rej, "rec:top", err_budget=[5])
    rej.close()
    reasons = _reasons(tmp_path / "rej.jsonl")
    assert reasons.count("extract_error") == 4                 # cap 5 -> 4 logged, then capped
    assert reasons.count("nested_extract_errors_capped") == 1
    assert len(reasons) == 5                                   # bounded: 3 corrupt zips skipped


def test_default_cap_is_sane():
    assert _MAX_NESTED_EXTRACT_ERRORS >= 100    # high enough never to bite a legit record
