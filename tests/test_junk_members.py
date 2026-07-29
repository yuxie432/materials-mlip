"""AppleDouble / __MACOSX / .DS_Store cruft must never be treated as a VASP file or
a nested archive (regression test for the macOS-tarred-upload bug: ``._vasprun.xml``
sidecars were extracted and fed to pymatgen, and ``._X.zip`` sidecars were unzipped —
producing spurious parse/BadZipFile errors and a duplicate-calc displacement risk)."""
from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

from zenodo_harvest.fetch import (
    _extract_tar, _extract_zip, _find_calc_units, _is_junk_member, _want_member)


def test_is_junk_member():
    for junk in ("._OUTCAR", "run/._vasprun.xml", "._Data.zip", ".DS_Store",
                 "d/.DS_Store", "__MACOSX/foo", "__MACOSX/a/b", "x/__MACOSX/y"):
        assert _is_junk_member(junk), junk
    for real in ("OUTCAR", "run/vasprun.xml", "Data.zip", "a_._b/vasprun.xml",
                 "sub/CONTCAR", "vasprun.xml.gz"):
        assert not _is_junk_member(real), real


def test_want_member_rejects_junk():
    assert _want_member("vasprun.xml")
    assert _want_member("nested.tar.gz")
    assert not _want_member("._vasprun.xml")   # AppleDouble of a real primary
    assert not _want_member("._nested.zip")    # AppleDouble of a nested archive
    assert not _want_member(".DS_Store")


def _add(tf: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tf.addfile(info, io.BytesIO(data))


def test_extract_tar_skips_appledouble(tmp_path: Path):
    """A Mac-tarred run dir: only the real vasprun.xml/OUTCAR come out; the ._ sidecars,
    the ._X.zip sidecar, and .DS_Store are all skipped."""
    arc = tmp_path / "run.tar"
    with tarfile.open(arc, "w") as tf:
        _add(tf, "run/vasprun.xml", b"<modeling></modeling>")
        _add(tf, "run/._vasprun.xml", b"\x00\x05\x16\x07AppleDouble")  # sidecar
        _add(tf, "run/OUTCAR", b"TITEL = PAW_PBE\n")
        _add(tf, "run/._OUTCAR", b"\x00\x05\x16\x07")
        _add(tf, "run/.DS_Store", b"\x00\x00")
        _add(tf, "run/._Data.zip", b"\x00\x05\x16\x07not-a-zip")       # would BadZipFile
    dest = tmp_path / "out"
    _names, extracted = _extract_tar(arc, dest, member_cap=1 << 30, budget=None)
    got = {Path(e).name for e in extracted}
    assert got == {"vasprun.xml", "OUTCAR"}, got
    assert not any(n.startswith("._") for n in got)
    assert not (dest / "run" / "._vasprun.xml").exists()


def test_find_calc_units_ignores_appledouble(tmp_path: Path):
    """A ._vasprun.xml beside a real vasprun.xml must not seed a duplicate/garbage unit."""
    d = tmp_path / "extracted" / "run"
    d.mkdir(parents=True)
    (d / "vasprun.xml").write_text("<modeling/>")
    (d / "._vasprun.xml").write_bytes(b"\x00\x05\x16\x07")
    (d / "OUTCAR").write_text("x")
    (d / "._OUTCAR").write_bytes(b"\x00")
    units = _find_calc_units(tmp_path / "extracted")
    assert len(units) == 1, units
    assert Path(units[0]["vasprun"]).name == "vasprun.xml"


def test_extract_zip_skips_macosx(tmp_path: Path):
    arc = tmp_path / "run.zip"
    with zipfile.ZipFile(arc, "w") as zf:
        zf.writestr("run/vasprun.xml", "<modeling/>")
        zf.writestr("__MACOSX/run/._vasprun.xml", "\x00\x05\x16\x07")
        zf.writestr("run/._vasprun.xml", "\x00\x05\x16\x07")
    dest = tmp_path / "out"
    _names, extracted = _extract_zip(arc, dest, member_cap=1 << 30, budget=None)
    assert {Path(e).name for e in extracted} == {"vasprun.xml"}, extracted
