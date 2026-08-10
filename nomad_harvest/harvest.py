"""NOMAD stages 0-2: discover (keyset) → license-gate + Zenodo-dedup → fetch raw vasprun.

Emits a ``fetched.jsonl`` in the EXACT shape the shared :mod:`zenodo_harvest.parse`
consumes (``provenance`` / ``local_dir`` / ``calc_units`` / ``availability``), so stages
3-5 (parse → store → merge/verify) run unchanged on NOMAD data. The only reuse from the
Zenodo package is a handful of shared helpers (``_find_calc_units``, ``_safe_members``,
the JSONL/rejection I/O, the license gate) — nothing in that package is modified.

Design (docs/NOMAD_HARVEST.md): direct uploads only; dedup vs Zenodo on ``references``;
pull ONLY the ``vasprun.xml`` (heavy outputs recorded as availability, never fetched);
stage under a canonical name so varied NOMAD naming/compression parses unchanged.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

# Shared, UNMODIFIED helpers from the Zenodo pipeline (reuse, not copy).
from zenodo_harvest import config
from zenodo_harvest.fetch import _find_calc_units, _safe_members
from zenodo_harvest.manifest import JsonlWriter, RejectionLogger, read_jsonl
from zenodo_harvest.models import is_reusable_license

from .client import CANDIDATE_REQUIRED, NomadClient, direct_upload_vasp_query

logger = logging.getLogger(__name__)


# --- provenance / dedup ---------------------------------------------------------

def entry_url(entry_id: str) -> str:
    """Human-facing NOMAD entry page (for provenance/attribution)."""
    return f"https://nomad-lab.eu/prod/v1/gui/entry/id/{entry_id}"


def slim_candidate(entry: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields the pipeline uses, so the keep-list manifest stays lean.

    The API returns ~24 KB/entry (see :data:`~nomad_harvest.client.CANDIDATE_REQUIRED`);
    writing that verbatim to the keep-list would make a full-harvest manifest ~170 GB. We
    persist just what triage/dedup/attribution/fetch need — dropping the large ``results``
    sub-trees and other unused metadata — preserving the nested paths ``build_fetched_entry``
    and ``choose_primary``/discover read.
    """
    results = entry.get("results") or {}
    material = results.get("material") or {}
    method = results.get("method") or {}
    sim = method.get("simulation") or {}
    dft = sim.get("dft") or {}
    return {
        "entry_id": entry.get("entry_id"),
        "upload_id": entry.get("upload_id"),
        "mainfile": entry.get("mainfile"),
        "external_db": entry.get("external_db"),
        "external_id": entry.get("external_id"),
        "license": entry.get("license"),
        "references": entry.get("references") or [],
        "datasets": [{"doi": ds.get("doi")} for ds in (entry.get("datasets") or [])
                     if isinstance(ds, dict) and ds.get("doi")],
        "authors": [{"name": a.get("name")} for a in (entry.get("authors") or [])
                    if isinstance(a, dict)],
        "upload_create_time": entry.get("upload_create_time"),
        "results": {
            "material": {"elements": material.get("elements"),
                         "chemical_formula_reduced": material.get("chemical_formula_reduced")},
            "method": {"method_name": method.get("method_name"),
                       "simulation": {"program_name": sim.get("program_name"),
                                      "program_version": sim.get("program_version"),
                                      "dft": {"xc_functional_names": dft.get("xc_functional_names")}}},
        },
    }


def normalize_doi(text: str) -> str | None:
    """Extract a bare, lower-cased DOI (``10.xxxx/…``) from a string/URL, else None."""
    if not text:
        return None
    m = re.search(r"10\.\d{4,9}/[^\s\"'<>]+", str(text).strip(), re.IGNORECASE)
    return m.group(0).rstrip(".").lower() if m else None


def references_of(entry: dict[str, Any]) -> list[str]:
    """All reference-ish strings on an entry: ``references`` + ``datasets[].doi``."""
    refs = [str(r) for r in (entry.get("references") or [])]
    for ds in entry.get("datasets") or []:
        if isinstance(ds, dict) and ds.get("doi"):
            refs.append(str(ds["doi"]))
    return refs


def zenodo_dois(metadata_path: str | Path) -> set[str]:
    """DOIs already in the Zenodo dataset (for dedup), from its ``metadata.jsonl``.

    Reads each record's ``provenance.doi``/``conceptdoi`` and normalises them. A missing
    file yields an empty set (dedup then keeps everything — safe default).
    """
    dois: set[str] = set()
    p = Path(metadata_path)
    if not p.is_file():
        return dois
    for rec in read_jsonl(p):
        prov = rec.get("provenance", {}) or {}
        for key in ("doi", "conceptdoi"):
            d = normalize_doi(str(prov.get(key) or ""))
            if d:
                dois.add(d)
    return dois


def zenodo_overlap(entry: dict[str, Any], known_dois: set[str]) -> str | None:
    """A reason string if this entry likely duplicates a Zenodo record, else None.

    A Zenodo-origin upload is NOT tagged with an ``external_db``; it is detectable only
    via a ``zenodo.org`` URL or a shared DOI in the entry's references (a Zenodo DOI is
    ``10.5281/zenodo.<n>``, so that prefix is caught even without the Zenodo DOI list).
    """
    for ref in references_of(entry):
        low = str(ref).lower()
        if "zenodo.org" in low:
            return f"zenodo_url:{ref}"
        d = normalize_doi(low)
        if d and (d in known_dois or d.startswith("10.5281/zenodo.")):
            return f"shared_doi:{d}"
    return None


# --- staging (fetch) ------------------------------------------------------------

_VASPRUN_RE = re.compile(r"vasprun.*\.xml", re.IGNORECASE)
_OUTCAR_RE = re.compile(r"(?:^|[/_.\-])outcar", re.IGNORECASE)
_COMPRESS_SUFFIXES = (".gz", ".bz2", ".xz")


def _compression_suffix(name: str) -> str:
    low = name.lower()
    for suf in _COMPRESS_SUFFIXES:
        if low.endswith(suf):
            return suf
    return ""


def canonical_staged_name(remote_name: str, role: str) -> str:
    """Canonical local filename for a fetched primary, preserving compression.

    NOMAD names vary wildly (``vasprun.xml.bz2``, ``GEO3_vasprun.xml.bz2``,
    ``vasprun.xml.relax1``); staging under a canonical name (``vasprun.xml``/``OUTCAR``
    plus any ``.gz/.bz2/.xz``) guarantees the shared ``_find_calc_units`` role regex and
    pymatgen's ``zopen`` both accept it, whatever the upload called it.
    """
    base = "vasprun.xml" if role == "vasprun" else "OUTCAR"
    return base + _compression_suffix(remote_name)


def choose_primary(rawdir: dict[str, Any]) -> tuple[str, str] | None:
    """Pick the file to fetch as the calc's primary: ``(remote_path, role)`` or None.

    Prefers the entry's ``mainfile`` when it is a vasprun/OUTCAR, else the first
    vasprun.xml among the raw files, else the first OUTCAR. Heavy/other files are ignored.
    """
    files = [f.get("path") or "" for f in (rawdir.get("files") or [])]
    mainfile = rawdir.get("mainfile") or ""
    if mainfile:
        base = mainfile.rsplit("/", 1)[-1]
        if _VASPRUN_RE.search(base):
            return mainfile, "vasprun"
        if _OUTCAR_RE.search(base):
            return mainfile, "outcar"
    for path in files:
        if _VASPRUN_RE.search(path.rsplit("/", 1)[-1]):
            return path, "vasprun"
    for path in files:
        if _OUTCAR_RE.search(path.rsplit("/", 1)[-1]):
            return path, "outcar"
    return None


def raw_path_rel(full_path: str, mainfile: str | None) -> str:
    """Convert an upload-root file path (as ``rawdir`` reports it) to the path the
    per-entry raw endpoint expects — **relative to the mainfile's directory**.

    Verified live: ``GET /entries/{id}/raw/<upload-root path>`` returns 404, whereas
    ``/raw/<path relative to the mainfile's dir>`` returns 200. All of a NOMAD entry's raw
    files live in the mainfile's directory, so this is the segment after that dir (a plain
    basename for the common flat layout); a file not under it falls back to its basename.
    """
    main_dir = mainfile.rsplit("/", 1)[0] if (mainfile and "/" in mainfile) else ""
    if main_dir and full_path.startswith(main_dir + "/"):
        return full_path[len(main_dir) + 1:]
    return full_path.rsplit("/", 1)[-1]


def stage_entry(client: NomadClient, entry: dict[str, Any], raw_dir: Path,
                want_outcar: bool = False) -> tuple[Path, dict[str, Any]] | None:
    """Download an entry's primary VASP file (+ optional OUTCAR) into the staging tree.

    Lays files out as ``<raw_dir>/<entry_id>/extracted/calc/<canonical-name>`` — exactly
    the layout the shared parser expects (``local_dir=<entry_id>``, primary under
    ``extracted/``). Returns ``(dest_dir, rawdir_listing)`` or None if no VASP primary is
    present. Heavy-output availability is derived from the listing, never fetched.
    """
    entry_id = entry["entry_id"]
    rd = client.rawdir(entry_id)
    mainfile = rd.get("mainfile") or ""
    primary = choose_primary(rd)
    if primary is None:
        return None
    remote_path, role = primary
    dest = raw_dir / entry_id
    calc_dir = dest / "extracted" / "calc"
    client.download_raw_file(entry_id, raw_path_rel(remote_path, mainfile),
                             calc_dir / canonical_staged_name(remote_path, role))
    if want_outcar and role == "vasprun":
        for f in rd.get("files") or []:
            p = f.get("path") or ""
            if _OUTCAR_RE.search(p.rsplit("/", 1)[-1]):
                client.download_raw_file(entry_id, raw_path_rel(p, mainfile),
                                         calc_dir / canonical_staged_name(p, "outcar"))
                break
    return dest, rd


def build_fetched_entry(entry: dict[str, Any], raw_dir: Path, dest: Path,
                        rawdir_listing: dict[str, Any]) -> dict[str, Any] | None:
    """Assemble the ``fetched.jsonl`` record the shared parser consumes.

    Groups staged files into calc units via the shared ``_find_calc_units``, records
    heavy-output availability from the rawdir listing, and writes NOMAD provenance
    (``source="nomad"``, ``record_id=entry_id``, DOI/URL/license/references/external_db)
    so ``metadata.jsonl`` carries correct attribution. Returns None if no parseable calc
    unit was staged.

    NB: the shared parser currently namespaces ``calc_id`` as ``zenodo:<recid>:…`` and
    tags each extxyz frame ``source="zenodo"`` (both hard-coded there). The AUTHORITATIVE
    source is ``metadata.jsonl``'s ``provenance.source`` (correctly ``"nomad"`` here). Only
    when NOMAD data is *merged into the shared dataset* (Phase 3) does the ``calc_id``
    prefix matter — at which point the shared parser should gain a ``source`` parameter
    (a small, backward-compatible change to review then). For an isolated NOMAD dataset it
    is cosmetic.
    """
    entry_id = entry["entry_id"]
    units = _find_calc_units(dest / "extracted")
    if not units:
        return None
    rel_units = [{k: str(Path(v).relative_to(raw_dir)) for k, v in u.items()} for u in units]
    names = [f.get("path") or "" for f in (rawdir_listing.get("files") or [])]
    avail = _safe_members(names)
    refs = references_of(entry)
    doi = next((normalize_doi(r) for r in refs if normalize_doi(r)), None)
    results = entry.get("results") or {}
    method = results.get("method") or {}
    sim = method.get("simulation") or {}
    dft = sim.get("dft") or {}
    return {
        "recid": entry_id,
        "provenance": {
            "source": "nomad",
            "record_id": entry_id,
            "upload_id": entry.get("upload_id"),
            "doi": doi,
            "url": entry_url(entry_id),
            "license": entry.get("license"),
            "external_db": entry.get("external_db"),      # None for direct uploads
            "external_id": entry.get("external_id"),
            "references": refs,
            "authors": [a.get("name") for a in (entry.get("authors") or [])
                        if isinstance(a, dict)],
            "mainfile": entry.get("mainfile"),
            "functional": dft.get("xc_functional_names"),
            "program_version": sim.get("program_version"),
            "elements": (results.get("material") or {}).get("elements"),
            "upload_create_time": entry.get("upload_create_time"),
        },
        "local_dir": str(dest.relative_to(raw_dir)),
        "n_calc_units": len(units),
        "calc_units": rel_units,
        "availability": avail["availability"],
        "availability_files": avail["availability_files"],
    }


# --- orchestration --------------------------------------------------------------

def discover_candidates(client: NomadClient, out_path: str | Path,
                        query: dict[str, Any] | None = None,
                        max_entries: int | None = None,
                        page_size: int = 1000,
                        license_gate: bool = True,
                        zenodo_metadata: str | Path | None = None) -> dict[str, Any]:
    """Stage 0/1: keyset-scan the keep-query -> a candidate keep-list JSONL.

    Applies the license gate (keep only redistributable licences) and Zenodo dedup inline,
    logging every drop with a reason so recall stays auditable. Resumable: entries already
    in ``out_path`` are skipped.
    """
    out = Path(out_path)
    query = query or direct_upload_vasp_query()
    known = zenodo_dois(zenodo_metadata) if zenodo_metadata else set()
    seen = {rec["entry_id"] for rec in read_jsonl(out)} if out.is_file() else set()
    rej = RejectionLogger(out.parent / "nomad_rejections.jsonl")
    kept = dropped = 0
    with JsonlWriter(out) as w:
        for entry in client.iter_entries(query, required=CANDIDATE_REQUIRED,
                                          page_size=page_size, max_entries=max_entries):
            eid = entry["entry_id"]
            if eid in seen:
                continue
            seen.add(eid)
            if license_gate and not is_reusable_license(entry.get("license")):
                rej.reject("nomad_discover", eid, "non_redistributable_license",
                           license=entry.get("license"))
                dropped += 1
                continue
            dup = zenodo_overlap(entry, known)
            if dup:
                rej.reject("nomad_discover", eid, "duplicate_of_zenodo", detail=dup)
                dropped += 1
                continue
            w.write(slim_candidate(entry))
            kept += 1
    rej.close()
    logger.info("nomad discover: kept %d, dropped %d", kept, dropped)
    return {"out": str(out), "kept": kept, "dropped": dropped}


def fetch_candidates(client: NomadClient, in_path: str | Path,
                     raw_dir: str | Path = config.RAW_DIR,
                     out_path: str | Path | None = None,
                     want_outcar: bool = False,
                     max_records: int | None = None) -> dict[str, Any]:
    """Stage 2: stage each candidate's vasprun (+optional OUTCAR) -> ``fetched.jsonl``.

    Resumable (entries already in ``out_path`` are skipped) and audited (every failure is
    logged with a reason, never silently dropped). Output is consumed unchanged by
    ``zenodo_harvest.parse``.
    """
    raw_dir = Path(raw_dir)
    out = Path(out_path) if out_path else raw_dir.parent / "manifests" / "nomad_fetched.jsonl"
    done = {rec["recid"] for rec in read_jsonl(out)} if out.is_file() else set()
    rej = RejectionLogger(raw_dir.parent / "manifests" / "nomad_fetch_rejections.jsonl")
    staged = failed = 0
    with JsonlWriter(out) as w:
        for i, entry in enumerate(read_jsonl(in_path)):
            if max_records is not None and i >= max_records:
                break
            eid = entry["entry_id"]
            if eid in done:
                continue
            try:
                res = stage_entry(client, entry, raw_dir, want_outcar=want_outcar)
                if res is None:
                    rej.reject("nomad_fetch", eid, "no_vasp_primary")
                    failed += 1
                    continue
                dest, rd = res
                fe = build_fetched_entry(entry, raw_dir, dest, rd)
                if fe is None:
                    rej.reject("nomad_fetch", eid, "no_calc_units_after_stage")
                    failed += 1
                    continue
                w.write(fe)
                staged += 1
            except Exception as exc:  # noqa: BLE001 - one bad entry must not abort the run
                rej.reject("nomad_fetch", eid, "fetch_error",
                           detail=f"{type(exc).__name__}: {exc}")
                failed += 1
    rej.close()
    logger.info("nomad fetch: staged %d, failed %d", staged, failed)
    return {"out": str(out), "staged": staged, "failed": failed}
