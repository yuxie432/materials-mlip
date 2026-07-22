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
* **Robust**: size-capped (archive *and* per-member uncompressed), checksum-verified,
  zip-slip-safe, streamed (zip/tar/rar never load a whole member into RAM; 7z
  decompresses selected members in memory — py7zr's API — bounded by the member
  cap). `.zip`/`.tar*` use the stdlib; `.rar`/`.7z` are handled when the optional
  `archives` extra (rarfile/py7zr) is installed, else logged as a rejection rather
  than being fatal.
"""

from __future__ import annotations

import logging
import os
import re
import tarfile
import time
import zipfile
from hashlib import md5
from pathlib import Path
from typing import Any

import requests

from . import config
from .client import _parse_retry_after
from .manifest import JsonlWriter, RejectionLogger, read_jsonl

logger = logging.getLogger(__name__)

# Optional archive backends (installed via the ``archives`` extra). ``.rar`` also
# needs an ``unrar``/``bsdtar`` binary on PATH; ``rarfile`` raises at open time if
# it is missing, which we catch and log as a clean skip rather than a crash.
try:
    import py7zr  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    py7zr = None  # type: ignore
try:
    import rarfile  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    rarfile = None  # type: ignore
try:
    import zstandard  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    zstandard = None  # type: ignore

# Per-member *uncompressed* size cap: bounds disk and defuses decompression bombs
# (the archive-level max_bytes only caps the *compressed* download). Extraction
# streams in chunks, so memory stays ~chunk-sized regardless of member size.
DEFAULT_MAX_MEMBER_BYTES = 2_000_000_000

# Record-level rejection reasons that are terminal (re-running can't help): the
# archive downloaded + extracted fine but held no usable VASP outputs. These recids
# are skipped on resume so their (often large) archives aren't re-downloaded and
# re-rejected every run. Transient reasons (download_error/http_*) are NOT here, so
# they still retry. See --retry-rejected to override.
_TERMINAL_REJECT_REASONS = {"no_calc_units_after_extract", "no_vasp_files_fetched"}

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
# zstd-compressed tarballs: common for large ML/DFT datasets (measured on Zenodo:
# ~100+ GB of `.tar.zst` VASP training data). tarfile gained native `r:zst` only in
# CPython 3.14, so we decompress via the `zstandard` lib (the `archives` extra) and
# stream the result through tarfile — see `_extract_tar_zst`.
_ZST_TAR_SUFFIXES = (".tar.zst", ".tzst")
# Split/spanned archive part names (`foo.z01`, `foo.z02`, …). The stdlib cannot
# reassemble a multi-volume zip, so these are surfaced as an explicit rejection
# rather than silently ignored (which looked like "record had no VASP").
_MULTIPART_RE = re.compile(r"\.(z\d{2}|r\d{2}|part\d+\.rar)$", re.IGNORECASE)


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
    """Download ``url`` to ``dest``, verifying md5. Returns (ok, reason).

    On HTTP 429 the download backs off (``min(Retry-After, 120)`` s) and retries in
    place, up to 3 attempts; only once that budget is spent does it fall through to
    the ``http_429`` transient rejection (so a still-throttled file simply retries on
    the next run rather than being dropped as a hard failure).
    """
    if dest.exists() and expected_md5 and _md5(dest) == expected_md5:
        return True, "cached"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=120) as r:
                if r.status_code == 429 and attempt < 2:
                    wait = min(_parse_retry_after(r.headers.get("Retry-After")), 120)
                    logger.warning("download rate limited; sleeping %ss", wait)
                    time.sleep(wait)
                    continue
                if r.status_code != 200:
                    return False, f"http_{r.status_code}"  # incl. http_429 after retries
                clen = r.headers.get("Content-Length")
                if max_bytes and clen and int(clen) > max_bytes:
                    return False, "over_size_cap"
                written = 0
                with tmp.open("wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        written += len(chunk)
                        if max_bytes and written > max_bytes:
                            fh.close()
                            tmp.unlink(missing_ok=True)
                            return False, "over_size_cap"
                        fh.write(chunk)
        except requests.RequestException as exc:
            tmp.unlink(missing_ok=True)
            return False, f"download_error:{type(exc).__name__}"
        except OSError as exc:
            # e.g. ENOSPC (disk/quota full) while writing the .part on cluster scratch.
            # Treat as a TRANSIENT per-file failure (not a hard crash of the whole run):
            # unlink the partial, return a non-terminal reason so a later resume retries.
            tmp.unlink(missing_ok=True)
            return False, f"write_error:{type(exc).__name__}"
        break  # a non-429 response streamed to completion
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


def _copy_capped(src: Any, out: Path, cap: int) -> bool:
    """Stream ``src`` -> ``out`` in chunks; abort (delete partial) if over ``cap``.

    Streaming keeps peak memory ~chunk-sized regardless of the member's uncompressed
    size (the old ``src.read()`` allocated the whole member — an OOM / zip-bomb risk).
    """
    written = 0
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("wb") as dst:
        for chunk in iter(lambda: src.read(1 << 20), b""):
            written += len(chunk)
            if written > cap:
                dst.close()
                out.unlink(missing_ok=True)
                return False
            dst.write(chunk)
    return True


def _extract_zip(path: Path, dest: Path, member_cap: int) -> tuple[list[str], list[str]]:
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
            if info.file_size > member_cap:  # header says it's too big -> skip pre-extract
                logger.warning("skip oversized member %s (%d B) in %s", base, info.file_size, path.name)
                continue
            with zf.open(info) as src:
                if _copy_capped(src, out, member_cap):
                    extracted.append(info.filename)
    return names, extracted


def _extract_tar(path: Path, dest: Path, member_cap: int) -> tuple[list[str], list[str]]:
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
            if member.size > member_cap:
                logger.warning("skip oversized member %s (%d B) in %s", base, member.size, path.name)
                continue
            src = tf.extractfile(member)
            if src is None:
                continue
            if _copy_capped(src, out, member_cap):
                extracted.append(member.name)
    return names, extracted


def _extract_rar(path: Path, dest: Path, member_cap: int) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with rarfile.RarFile(path) as rf:  # type: ignore[union-attr]
        names = rf.namelist()
        for info in rf.infolist():
            if info.isdir():
                continue
            base = info.filename.rsplit("/", 1)[-1]
            if not _PARSE_RE.search(base):
                continue
            out = dest / info.filename
            if not _is_within(dest, out):
                continue
            if getattr(info, "file_size", 0) > member_cap:
                logger.warning("skip oversized member %s in %s", base, path.name)
                continue
            with rf.open(info) as src:
                if _copy_capped(src, out, member_cap):
                    extracted.append(info.filename)
    return names, extracted


def _extract_7z(path: Path, dest: Path, member_cap: int) -> tuple[list[str], list[str]]:
    extracted: list[str] = []
    with py7zr.SevenZipFile(path, "r") as zf:  # type: ignore[union-attr]
        infos = zf.list()
        names = [i.filename for i in infos]
        sizes = {i.filename: getattr(i, "uncompressed", 0) or 0 for i in infos}
        targets = [
            n for n in names
            if not n.endswith("/")
            and _PARSE_RE.search(n.rsplit("/", 1)[-1])
            and _is_within(dest, dest / n)
            and sizes.get(n, 0) <= member_cap
        ]
        if not targets:
            return names, extracted
        for name, bio in zf.read(targets).items():  # py7zr decompresses selected members
            out = dest / name
            out.parent.mkdir(parents=True, exist_ok=True)
            data = bio.read()
            if len(data) > member_cap:
                continue
            out.write_bytes(data)
            extracted.append(name)
    return names, extracted


def _extract_tar_zst(path: Path, dest: Path, member_cap: int) -> tuple[list[str], list[str]]:
    """Extract VASP files from a zstd-compressed tarball (``.tar.zst``/``.tzst``).

    Streams the decompressed bytes through ``tarfile`` in streaming (``r|``) mode so
    peak memory/disk stays ~chunk-sized regardless of the (often huge) decompressed
    size — the same selective, cap-guarded, zip-slip-safe extraction as ``_extract_tar``.
    """
    extracted: list[str] = []
    names: list[str] = []
    dctx = zstandard.ZstdDecompressor()  # type: ignore[union-attr]
    with path.open("rb") as fh, dctx.stream_reader(fh) as reader:
        # streaming tar: members arrive in order; extractfile() works on the current one.
        with tarfile.open(fileobj=reader, mode="r|") as tf:
            for member in tf:
                names.append(member.name)
                if not member.isfile():
                    continue
                base = member.name.rsplit("/", 1)[-1]
                if not _PARSE_RE.search(base):
                    continue
                out = dest / member.name
                if not _is_within(dest, out):
                    continue
                if member.size > member_cap:
                    logger.warning("skip oversized member %s (%d B) in %s", base, member.size, path.name)
                    continue
                src = tf.extractfile(member)
                if src is None:
                    continue
                if _copy_capped(src, out, member_cap):
                    extracted.append(member.name)
    return names, extracted


_EXTRACTORS = {"zip": _extract_zip, "tar": _extract_tar, "rar": _extract_rar,
               "sevenzip": _extract_7z, "tarzst": _extract_tar_zst}

# Exceptions a bad/truncated archive can raise across all backends — caught per
# archive so one corrupt file rejects just that file, not the whole run. The zstd
# backend adds its own error type when installed.
_EXTRACT_ERRORS: tuple[type[BaseException], ...] = (
    zipfile.BadZipFile, tarfile.TarError, EOFError, OSError, ValueError,
)
if zstandard is not None:  # pragma: no branch - trivial
    _EXTRACT_ERRORS = (*_EXTRACT_ERRORS, zstandard.ZstdError)


def _is_archive(name: str) -> str | None:
    low = name.lower()
    if low.endswith(".zip"):
        return "zip"
    if low.endswith(_TAR_SUFFIXES):
        return "tar"
    if low.endswith(".rar"):
        return "rar"
    if low.endswith(".7z"):
        return "sevenzip"
    if low.endswith(_ZST_TAR_SUFFIXES):
        return "tarzst"
    if low.endswith(".zst"):
        # A bare `.zst` is ambiguous: a single compressed VASP file (``OUTCAR.zst``)
        # stays a direct-file download (handled by _PARSE_RE below); anything else is
        # treated as a zstd tarball (``Research_Data.zst`` etc. seen on Zenodo).
        stem = low[:-4].rsplit("/", 1)[-1]
        return None if _PARSE_RE.search(stem) else "tarzst"
    # A single gzipped file (e.g. OUTCAR.gz) is not an archive to unpack; it is
    # handled by the _PARSE_RE / availability branches in fetch_record.
    return None


def _archive_subdir(base: str) -> str:
    """A filesystem-safe per-archive extraction subdir (avoids cross-archive
    member-path collisions when one record ships multiple archives)."""
    stem = base
    for suf in (*_TAR_SUFFIXES, *_ZST_TAR_SUFFIXES, ".zip", ".rar", ".7z", ".zst"):
        if stem.lower().endswith(suf):
            stem = stem[: -len(suf)]
            break
    return re.sub(r"[^A-Za-z0-9._-]", "_", stem) or "archive"


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


_PRIMARY_ROLES = ("vasprun", "vaspout", "outcar")


def _unit_tag(base: str, role: str) -> str:
    """Distinguishing prefix/suffix of a filename once its role word is removed.

    ``site1_OUTCAR`` -> ``site1``; ``vasprun.xml`` -> ``""``; ``vasprun_1.xml`` ->
    ``1``. Files sharing a tag in a multi-calc directory belong to the same calc.
    """
    low = base.lower()
    i = low.find(role)
    tag = (low[:i] + low[i + len(role):]) if i != -1 else low
    tag = re.sub(r"\.(xml|h5|gz|bz2|xz|zst|json)$", "", tag)
    return tag.strip(" ._-/")


def _assign_role(slot: dict[str, str], role: str, path: Path) -> None:
    if role == "vasprun" and "vasprun" in slot:
        if path.name.lower() == "vasprun.xml":  # prefer the canonical name if several
            slot["vasprun"] = str(path)
    else:
        slot.setdefault(role, str(path))


def _find_calc_units(root: Path) -> list[dict[str, str]]:
    """Group extracted files into calc units — one per distinct primary output.

    Grouping by directory alone silently collapses multiple independent
    calculations that share a flat directory but differ only by a filename
    prefix/suffix (e.g. ``site1_OUTCAR`` + ``site2_OUTCAR``): all-but-one primary is
    dropped and the survivors are cross-paired. So within each directory: with a
    single primary the whole directory is one unit (the common per-calc-directory
    layout — tagged inputs stay attached); with several primaries we split by tag so
    each OUTCAR/vasprun/vaspout seeds its own unit, pairing same-tag inputs and
    sharing untagged inputs across them.
    """
    by_dir: dict[Path, list[tuple[str, Path]]] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        role = _unit_role(p.name)
        if role is not None:
            by_dir.setdefault(p.parent, []).append((role, p))

    units: list[dict[str, str]] = []
    for _d, items in sorted(by_dir.items()):
        primaries = [(r, p) for r, p in items if r in _PRIMARY_ROLES]
        if not primaries:
            continue  # inputs only (POSCAR/INCAR/…): no energies/forces to parse
        if len(primaries) == 1:
            slot: dict[str, str] = {"dir": str(_d)}
            for r, p in items:
                _assign_role(slot, r, p)
            units.append(slot)
            continue
        # multiple primaries in one flat directory -> split into one unit per tag
        groups: dict[str, list[tuple[str, Path]]] = {}
        shared_inputs: list[tuple[str, Path]] = []
        for r, p in items:
            tag = _unit_tag(p.name, r)
            if r not in _PRIMARY_ROLES and tag == "":
                shared_inputs.append((r, p))  # untagged input applies to every calc here
            else:
                groups.setdefault(tag, []).append((r, p))
        for _tag, grp in sorted(groups.items()):
            if not any(r in _PRIMARY_ROLES for r, _ in grp):
                continue  # a tagged input with no matching primary -> nothing to seed
            slot = {"dir": str(_d)}
            for r, p in (*grp, *shared_inputs):
                _assign_role(slot, r, p)
            units.append(slot)
    return units


def fetch_record(rec: dict, session: requests.Session, raw_dir: Path,
                 max_bytes: int | None, rej: RejectionLogger,
                 max_member_bytes: int = DEFAULT_MAX_MEMBER_BYTES) -> dict | None:
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

        # A split/spanned archive part (foo.z01, foo.r01, foo.partN.rar) cannot be
        # reassembled by the stdlib. Surface it explicitly instead of letting it fall
        # through to the availability branch (which silently produced no VASP files and
        # looked like "record had no VASP").
        if kind is None and _MULTIPART_RE.search(base):
            rej.reject("fetch", f"{recid}:{key}", "archive_multipart_unsupported",
                       detail="split/spanned archive; not reassembled")
            continue

        # Optional-backend archives: skip cleanly (visible rejection) when the backing
        # library isn't installed, rather than silently dropping the record's data.
        _missing_backend = (
            (kind == "rar" and rarfile is None)
            or (kind == "sevenzip" and py7zr is None)
            or (kind == "tarzst" and zstandard is None)
        )
        if _missing_backend:
            rej.reject("fetch", f"{recid}:{key}", "archive_unsupported",
                       format=base.rsplit(".", 1)[-1], detail="install the 'archives' extra")
            continue

        if kind is not None:  # a real archive to download + selectively extract
            if max_bytes and size > max_bytes:
                rej.reject("fetch", f"{recid}:{key}", "over_size_cap", size=size)
                continue
            arc = dest / base
            ok, why = download_file(session, url, arc, cksum, max_bytes)
            if not ok:
                rej.reject("fetch", f"{recid}:{key}", why, size=size)
                continue
            # Extract into a per-archive subdir so identical member paths across
            # multiple archives in one record can't overwrite each other.
            extract_dir = dest / "extracted" / _archive_subdir(base)
            try:
                names, extracted = _EXTRACTORS[kind](arc, extract_dir, max_member_bytes)
            except _EXTRACT_ERRORS as exc:
                rej.reject("fetch", f"{recid}:{key}", "extract_error",
                           detail=f"{type(exc).__name__}: {exc}")
                arc.unlink(missing_ok=True)
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

    # Store paths RELATIVE to raw_dir. Absolute paths break the manifest as soon as
    # staged data is moved between cluster scratch areas; parse resolves these back
    # against its --raw-dir. calc_id keys off the primary path relative to
    # <local_dir>/extracted, so the recid prefix cancels and the id is unchanged.
    rel_units = [{k: str(Path(v).relative_to(raw_dir)) for k, v in u.items()} for u in units]

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
            "resource_type": rec.get("resource_type"),  # source-quality tag for late filtering
            "publication_date": rec.get("publication_date"),
            "keywords": rec.get("keywords"),
        },
        "local_dir": str(dest.relative_to(raw_dir)),
        "n_calc_units": len(units),
        "calc_units": rel_units,
        "availability": availability,
        "availability_files": availability_files,
    }


def _done_recids(out_path: Path) -> set[str]:
    """recids already staged in a prior ``fetched.jsonl`` (record-level resume)."""
    if not out_path.is_file():
        return set()
    return {rec["recid"] for rec in read_jsonl(out_path) if rec.get("recid")}


def _terminal_reject_recids(rejections_path: Path) -> set[str]:
    """recids terminally rejected in a prior run (won't succeed on retry).

    Reading these into the skip set stops fetch from re-downloading + re-extracting
    + re-rejecting the same (often large) archives on every resume, and stops
    rejections.jsonl from growing without bound. Only record-level terminal reasons
    count; transient per-file failures (download errors, 5xx) are left to retry.
    """
    if not rejections_path.is_file():
        return set()
    out: set[str] = set()
    for r in read_jsonl(rejections_path):
        if (r.get("stage") == "fetch" and r.get("reason") in _TERMINAL_REJECT_REASONS
                and isinstance(r.get("id"), str) and ":" not in r["id"]):  # ":" => per-file id
            out.add(r["id"])
    return out


def fetch(
    in_path: str | Path,
    out_path: str | Path = config.MANIFEST_DIR / "fetched.jsonl",
    raw_dir: str | Path = config.RAW_DIR,
    rejections_path: str | Path = config.MANIFEST_DIR / "rejections.jsonl",
    max_bytes: int | None = 500_000_000,
    max_records: int | None = None,
    token: str | None = None,
    retry_rejected: bool = False,
    max_member_bytes: int = DEFAULT_MAX_MEMBER_BYTES,
) -> dict:
    """Fetch all records in ``in_path`` (a triaged keep-list).

    Resumable at the record level: any recid already staged in ``out_path`` — or
    terminally rejected in ``rejections_path`` (unless ``retry_rejected``) — is
    skipped, so a re-run after an interrupted harvest neither re-downloads it nor
    appends duplicate manifest/rejection lines. Pass ``retry_rejected=True`` to
    reprocess previously-rejected records (e.g. after raising ``max_bytes`` or
    installing the ``archives`` extra). ``max_records`` caps records fetched *this
    run* (newly staged), not counting resumed skips.

    ``max_bytes`` caps each downloaded file/archive (``None`` = no cap); the archive
    is deleted right after its VASP files are extracted, so persistent disk is the
    extracted files, not the archive. ``max_member_bytes`` caps each single extracted
    file (guards against decompression bombs / a runaway multi-GB vasprun.xml).
    """
    out_path, raw_dir = Path(out_path), Path(raw_dir)
    rejections_path = Path(rejections_path)
    done = _done_recids(out_path)
    if not retry_rejected:
        done |= _terminal_reject_recids(rejections_path)
    rej = RejectionLogger(rejections_path)
    stats = {"records": 0, "fetched": 0, "skipped_existing": 0, "calc_units": 0}
    with _session(token or os.environ.get("ZENODO_TOKEN")) as session, \
            JsonlWriter(out_path) as out:
        for rec in read_jsonl(in_path):
            stats["records"] += 1
            if rec["recid"] in done:
                stats["skipped_existing"] += 1
                continue
            entry = fetch_record(rec, session, raw_dir, max_bytes, rej, max_member_bytes)
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
