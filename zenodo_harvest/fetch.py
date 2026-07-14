"""Stage 2 — fetch.

Download the VASP-relevant files for each triaged record and lay them out on
disk as one directory per calculation, ready for parsing. Design goals:

* **Cheap on disk / bandwidth**: from archives we extract only the files needed
  to parse (`vasprun.xml`, `OUTCAR`, `INCAR`, structures, …) and merely *record
  the presence* of heavy files (CHGCAR, WAVECAR, DOSCAR, EIGENVAL, …) as
  availability flags — never extracting them (mentor: record, don't store).
* **Resumable**: records already in ``fetched.jsonl`` are skipped wholesale, and
  within a record any file already present with a matching md5 is skipped — so an
  interrupted harvest resumes cleanly, with no duplicate work or manifest lines
  (important on the cluster).
* **Robust**: size-capped, checksum-verified, zip-slip-safe; unsupported archives
  (`.rar`, `.7z` — no stdlib/portable tooling) are logged as rejections, not fatal.
"""

from __future__ import annotations

import logging
import os
import re
import tarfile
import zipfile
from hashlib import md5
from pathlib import Path
from typing import Any

import requests

from . import config
from .manifest import JsonlWriter, RejectionLogger, read_jsonl

logger = logging.getLogger(__name__)

# Files worth extracting for parsing / provenance. A canonical VASP stem taken as
# a *word* — at the start of the basename or after a path/name separator — with
# any suffix. Deliberately kept at least as permissive as triage's
# ``models._VASP_RE`` so fetch never drops a file triage already confirmed
# (e.g. ``site1_OUTCAR``, ``Si.vasprun.xml``, ``vasprun_1.xml``). Over-matching is
# recall-safe: the pymatgen parse in stage 3 is the real precision gate. POTCAR
# is kept only to read its titel strings. Heavy outputs (CHGCAR/DOSCAR/…) contain
# no stem word and fall through to the availability branch.
_PARSE_STEMS = ("vasprun", "vaspout", "outcar", "incar", "kpoints",
                "poscar", "contcar", "potcar", "oszicar")
_PARSE_RE = re.compile(r"(?:^|[/_.\-])(?:" + "|".join(_PARSE_STEMS) + r")", re.IGNORECASE)

# Heavy outputs: presence recorded as availability, contents never extracted.
_AVAILABILITY = {
    "charge_density": re.compile(r"^(chgcar|chg|aeccar\d*|parchg)", re.IGNORECASE),
    "wavefunction": re.compile(r"^wavecar", re.IGNORECASE),
    "dos": re.compile(r"^doscar", re.IGNORECASE),
    "eigenvalues": re.compile(r"^eigenval", re.IGNORECASE),
    "projected": re.compile(r"^procar", re.IGNORECASE),
    "local_potential": re.compile(r"^locpot", re.IGNORECASE),
    "elf": re.compile(r"^elfcar", re.IGNORECASE),
}

_TAR_SUFFIXES = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")


def _session(token: str | None) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "zenodo-harvest/0.1 (fetch)"
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s


def _md5(path: Path, chunk: int = 1 << 20) -> str:
    h = md5()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def download_file(
    session: requests.Session, url: str, dest: Path, expected_md5: str | None,
    max_bytes: int | None = None,
) -> tuple[bool, str]:
    """Download ``url`` to ``dest``, verifying md5. Returns (ok, reason)."""
    if dest.exists() and expected_md5 and _md5(dest) == expected_md5:
        return True, "cached"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with session.get(url, stream=True, timeout=120) as r:
            if r.status_code != 200:
                return False, f"http_{r.status_code}"
            clen = r.headers.get("Content-Length")
            if max_bytes and clen and int(clen) > max_bytes:
                return False, "over_size_cap"
            written = 0
            with tmp.open("wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    written += len(chunk)
                    if max_bytes and written > max_bytes:
                        fh.close(); tmp.unlink(missing_ok=True)
                        return False, "over_size_cap"
                    fh.write(chunk)
    except requests.RequestException as exc:
        tmp.unlink(missing_ok=True)
        return False, f"download_error:{type(exc).__name__}"
    if expected_md5 and _md5(tmp) != expected_md5:
        tmp.unlink(missing_ok=True)
        return False, "md5_mismatch"
    tmp.replace(dest)
    return True, "downloaded"


def _safe_members(names: list[str]) -> dict[str, Any]:
    """Scan archive member names for availability, without extracting anything."""
    avail = {k: False for k in _AVAILABILITY}
    avail_files: dict[str, str] = {}
    for name in names:
        base = name.rsplit("/", 1)[-1]
        for kind, rx in _AVAILABILITY.items():
            if rx.match(base):
                avail[kind] = True
                avail_files.setdefault(kind, name)
    return {"availability": avail, "availability_files": avail_files}


def _is_within(base: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


def _extract_zip(path: Path, dest: Path) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = info.filename.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / info.filename
            if not _is_within(dest, out):
                continue  # zip-slip guard
            out.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, out.open("wb") as dst:
                dst.write(src.read())
            extracted.append(info.filename)
    return names, extracted


def _extract_tar(path: Path, dest: Path) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with tarfile.open(path) as tf:
        names = tf.getnames()
        for member in tf.getmembers():
            if not member.isfile():
                continue
            base = member.name.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / member.name
            if not _is_within(dest, out):
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            src = tf.extractfile(member)
            if src is None:
                continue
            with out.open("wb") as dst:
                dst.write(src.read())
            extracted.append(member.name)
    return names, extracted


def _is_archive(name: str) -> str | None:
    low = name.lower()
    if low.endswith(".zip"):
        return "zip"
    if low.endswith(_TAR_SUFFIXES):
        return "tar"
    if low.endswith((".rar", ".7z")):
        return "unsupported"
    # A single gzipped file (e.g. OUTCAR.gz) is not an archive to unpack; it is
    # handled by the _PARSE_RE / availability branches in fetch_record.
    return None


# Role detection for grouping extracted files into calc units. Separator-tolerant
# (``site1_OUTCAR``, ``Si.vasprun.xml``) and suffix-tolerant (``vasprun_1.xml``),
# consistent with ``_PARSE_RE`` above; vasprun/vaspout additionally pin the
# expected extension so a stray ``vasprun.json`` isn't mistaken for the primary.
_ROLE_RES: tuple[tuple[str, "re.Pattern[str]"], ...] = (
    ("vasprun", re.compile(r"(?:^|[/_.\-])vasprun[\w.\-]*\.xml(?:\.(?:gz|bz2|xz|zst))?$", re.IGNORECASE)),
    ("vaspout", re.compile(r"(?:^|[/_.\-])vaspout[\w.\-]*\.h5$", re.IGNORECASE)),
    ("outcar",  re.compile(r"(?:^|[/_.\-])outcar", re.IGNORECASE)),
    ("incar",   re.compile(r"(?:^|[/_.\-])incar", re.IGNORECASE)),
    ("contcar", re.compile(r"(?:^|[/_.\-])contcar", re.IGNORECASE)),
    ("poscar",  re.compile(r"(?:^|[/_.\-])poscar", re.IGNORECASE)),
    ("potcar",  re.compile(r"(?:^|[/_.\-])potcar", re.IGNORECASE)),
)


def _unit_role(base: str) -> str | None:
    """Which calc-unit slot a filename fills (vasprun/outcar/incar/…), or None."""
    for role, rx in _ROLE_RES:
        if rx.search(base):
            return role
    return None


def _find_calc_units(root: Path) -> list[dict[str, str]]:
    """Group extracted files into one calc unit per directory holding VASP output."""
    by_dir: dict[Path, dict[str, str]] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        role = _unit_role(p.name)
        if role is None:
            continue
        slot = by_dir.setdefault(p.parent, {})
        if role == "vasprun" and "vasprun" in slot:
            # prefer the canonical vasprun.xml if several exist in one dir
            if p.name.lower() == "vasprun.xml":
                slot["vasprun"] = str(p)
        else:
            slot.setdefault(role, str(p))
    units = []
    for d, slot in sorted(by_dir.items()):
        if any(k in slot for k in ("vasprun", "vaspout", "outcar")):
            units.append({"dir": str(d), **slot})
    return units


def fetch_record(rec: dict, session: requests.Session, raw_dir: Path,
                 max_bytes: int | None, rej: RejectionLogger) -> dict | None:
    """Download + stage one record. Returns a fetched-manifest entry or None."""
    recid = rec["recid"]
    dest = raw_dir / recid
    availability = {k: False for k in _AVAILABILITY}
    availability_files: dict[str, str] = {}
    got_any = False

    for f in rec["files"]:
        key, url, size = f["key"], f.get("download"), f.get("size") or 0
        cksum = (f.get("checksum") or "").split(":", 1)[-1] or None
        base = (key or "").rsplit("/", 1)[-1]
        kind = _is_archive(base)

        if kind == "unsupported":
            rej.reject("fetch", f"{recid}:{key}", "archive_unsupported", format=base.split(".")[-1])
            continue

        if kind in ("zip", "tar"):
            if max_bytes and size > max_bytes:
                rej.reject("fetch", f"{recid}:{key}", "over_size_cap", size=size)
                continue
            arc = dest / base
            ok, why = download_file(session, url, arc, cksum, max_bytes)
            if not ok:
                rej.reject("fetch", f"{recid}:{key}", why, size=size)
                continue
            try:
                names, extracted = (_extract_zip if kind == "zip" else _extract_tar)(
                    arc, dest / "extracted")
            except (zipfile.BadZipFile, tarfile.TarError, EOFError) as exc:
                rej.reject("fetch", f"{recid}:{key}", "extract_error", detail=str(exc))
                continue
            info = _safe_members(names)
            for k, v in info["availability"].items():
                availability[k] = availability[k] or v
            availability_files.update(info["availability_files"])
            arc.unlink(missing_ok=True)  # drop the archive; keep only extracted VASP files
            got_any = got_any or bool(extracted)

        elif _PARSE_RE.search(base):  # directly-exposed VASP file
            if max_bytes and size > max_bytes:
                rej.reject("fetch", f"{recid}:{key}", "over_size_cap", size=size)
                continue
            out = dest / "extracted" / key
            if not _is_within(dest / "extracted", out):
                rej.reject("fetch", f"{recid}:{key}", "unsafe_path")  # path traversal guard
                continue
            ok, why = download_file(session, url, out, cksum, max_bytes)
            if ok:
                got_any = True
            else:
                rej.reject("fetch", f"{recid}:{key}", why, size=size)

        else:  # heavy / irrelevant direct file -> availability only
            for akind, rx in _AVAILABILITY.items():
                if rx.match(base):
                    availability[akind] = True
                    availability_files.setdefault(akind, key)

    if not got_any:
        rej.reject("fetch", recid, "no_vasp_files_fetched")
        return None

    units = _find_calc_units(dest / "extracted")
    if not units:
        rej.reject("fetch", recid, "no_calc_units_after_extract")
        return None

    return {
        "recid": recid,
        "provenance": {
            "source": "zenodo",
            "record_id": recid,
            "conceptrecid": rec.get("conceptrecid"),
            "doi": rec.get("doi"),
            "conceptdoi": rec.get("conceptdoi"),
            "url": rec.get("zenodo_url"),
            "title": rec.get("title"),
            "creators": rec.get("creators"),
            "license": rec.get("license"),
            "publication_date": rec.get("publication_date"),
            "keywords": rec.get("keywords"),
        },
        "local_dir": str(dest),
        "n_calc_units": len(units),
        "calc_units": units,
        "availability": availability,
        "availability_files": availability_files,
    }


def _done_recids(out_path: Path) -> set[str]:
    """recids already staged in a prior ``fetched.jsonl`` (record-level resume)."""
    if not out_path.is_file():
        return set()
    return {rec["recid"] for rec in read_jsonl(out_path) if rec.get("recid")}


def fetch(
    in_path: str | Path,
    out_path: str | Path = config.MANIFEST_DIR / "fetched.jsonl",
    raw_dir: str | Path = config.RAW_DIR,
    rejections_path: str | Path = config.MANIFEST_DIR / "rejections.jsonl",
    max_bytes: int | None = 500_000_000,
    max_records: int | None = None,
    token: str | None = None,
) -> dict:
    """Fetch all records in ``in_path`` (a triaged keep-list).

    Resumable at the record level: any recid already present in ``out_path`` is
    skipped, so a re-run after an interrupted harvest neither re-downloads it nor
    appends a duplicate manifest line. ``max_records`` caps records fetched *this
    run* (newly staged), not counting resumed skips.
    """
    out_path, raw_dir = Path(out_path), Path(raw_dir)
    done = _done_recids(out_path)
    rej = RejectionLogger(rejections_path)
    stats = {"records": 0, "fetched": 0, "skipped_existing": 0, "calc_units": 0}
    with _session(token or os.environ.get("ZENODO_TOKEN")) as session, \
            JsonlWriter(out_path) as out:
        for rec in read_jsonl(in_path):
            stats["records"] += 1
            if rec["recid"] in done:
                stats["skipped_existing"] += 1
                continue
            entry = fetch_record(rec, session, raw_dir, max_bytes, rej)
            if entry:
                out.write(entry)
                stats["fetched"] += 1
                stats["calc_units"] += entry["n_calc_units"]
            if max_records and stats["fetched"] >= max_records:
                break
    rej.close()
    stats["rejections"] = rej.n
    logger.info("fetch: %s", stats)
    return {"out_path": str(out_path), **stats}
