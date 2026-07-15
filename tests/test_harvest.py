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
from pathlib import Path

import pytest

from zenodo_harvest import discover as discover_mod
from zenodo_harvest.fetch import _PARSE_RE, _find_calc_units, _unit_role, _unit_tag
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
