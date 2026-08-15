"""Embedded electronic-structure availability probe (fix #2).

DOS / eigenvalues / projected data are frequently written straight into vasprun.xml with no
standalone DOSCAR/EIGENVAL/PROCAR file, so the fetch-stage filename scan under-counted them.
:func:`zenodo_harvest.parse._scan_vasprun_electronic` recovers them with a streaming tag scan
(never parsing the arrays). These tests are dependency-light — they need numpy/ase (imported
by ``parse``) but NOT pymatgen — so the probe is covered even in a minimal environment; the
end-to-end parse path is exercised in tests/test_parse_integration.py.
"""

from __future__ import annotations

import bz2
import gzip
import lzma
from pathlib import Path

from zenodo_harvest.parse import (
    _embedded_electronic_structure,
    _scan_vasprun_electronic,
)

_WITH = (b"<modeling><calculation><energy></energy>"
         b"<eigenvalues><array></array></eigenvalues>"
         b"<dos><total></total></dos>"
         b"</calculation></modeling>")
_WITHOUT = (b"<modeling><calculation><energy></energy>"
            b"<structure></structure></calculation></modeling>")


def _write(path: Path, data: bytes) -> str:
    if str(path).endswith(".gz"):
        with gzip.open(path, "wb") as fh:
            fh.write(data)
    elif str(path).endswith(".bz2"):
        with bz2.open(path, "wb") as fh:
            fh.write(data)
    elif str(path).endswith(".xz"):
        with lzma.open(path, "wb") as fh:
            fh.write(data)
    else:
        path.write_bytes(data)
    return str(path)


def test_scan_detects_embedded_dos_and_eigenvalues(tmp_path):
    p = _write(tmp_path / "vasprun.xml", _WITH)
    found = _scan_vasprun_electronic(p)
    assert found["dos"] is True
    assert found["eigenvalues"] is True
    assert found["projected"] is False           # no <projected> in this file


def test_scan_reports_absence(tmp_path):
    found = _scan_vasprun_electronic(_write(tmp_path / "vasprun.xml", _WITHOUT))
    assert found == {"dos": False, "eigenvalues": False, "projected": False}


def test_scan_handles_projected(tmp_path):
    data = _WITH[:-len(b"</calculation></modeling>")] + b"<projected></projected></calculation></modeling>"
    found = _scan_vasprun_electronic(_write(tmp_path / "vasprun.xml", data))
    assert found["projected"] is True


def test_scan_reads_compressed(tmp_path):
    # the scan opens gz / bz2 / xz transparently (fetch stages vaspruns still-compressed)
    for suffix in (".gz", ".bz2", ".xz"):
        p = _write(tmp_path / f"vasprun.xml{suffix}", _WITH)
        found = _scan_vasprun_electronic(p)
        assert found["dos"] and found["eigenvalues"], suffix


def test_scan_finds_tag_split_across_chunks(tmp_path):
    # Place <eigenvalues> so it straddles the 1 MiB read-chunk boundary: the scan carries a
    # small tail between chunks, so a boundary-split tag must still be found (not a false
    # negative). Uncompressed so the byte layout is exactly the read layout.
    chunk = 1 << 20
    prefix = b"<modeling>"
    tag = b"<eigenvalues>"
    pad = b"x" * (chunk - len(prefix) - 5)   # tag's '<' lands 5 bytes before the boundary
    data = prefix + pad + tag + b"</eigenvalues><dos></dos></modeling>"
    assert data[chunk - 5:chunk - 5 + len(tag)] == tag   # confirm it straddles the boundary
    found = _scan_vasprun_electronic(_write(tmp_path / "vasprun.xml", data))
    assert found["eigenvalues"] and found["dos"]


def test_embedded_dispatch_prefers_vasprun_then_none(tmp_path):
    vp = _write(tmp_path / "vasprun.xml", _WITH)
    assert _embedded_electronic_structure(vp, None)["dos"] is True
    # no vasprun and no vaspout -> nothing embedded (OUTCAR path uses filename flags instead)
    assert _embedded_electronic_structure(None, None) == {
        "dos": False, "eigenvalues": False, "projected": False}
