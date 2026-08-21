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

import json
import logging
import math
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

# Shared, UNMODIFIED helpers from the Zenodo pipeline (reuse, not copy). The disk/inode
# valve (StagingBudget) + its charged-mkdir/dir-usage helpers are generic byte/inode
# accountants, so the NOMAD fetch paces itself against the CSD3 quota with the exact same,
# already-hardened machinery the Zenodo fetch uses.
from zenodo_harvest import config
from zenodo_harvest.fetch import (
    BudgetExceeded,
    StagingBudget,
    _charged_mkdir,
    _dir_usage,
    _find_calc_units,
    _safe_members,
)
from zenodo_harvest.manifest import JsonlWriter, RejectionLogger, read_jsonl
from zenodo_harvest.models import is_reusable_license

from . import upload_zip
from .client import CANDIDATE_REQUIRED, NomadClient, direct_upload_vasp_query
from .upload_zip import UploadNotAvailable, ZipMember

logger = logging.getLogger(__name__)

_FETCH_LOG_EVERY = 2000        # log a fetch-progress line every this many staged entries
# Abort a per-entry fallback after this many CONSECUTIVE failures: when an upload's pre-packed
# /uploads/{id}/raw 500s, its entries fall back to /entries/{id}/rawdir which 500s too, and each
# 500 costs ~60 s of client retries — so a single bad upload (thousands of entries) would burn the
# whole wallclock. Bailing after a short run of failures caps that at ~minutes; the skipped entries
# retry on the next resume. (Root-caused from a live stall, 2026-08-20.)
_FALLBACK_MAX_CONSEC_FAIL = 8
# Persistent dead-upload skip-list. An upload whose pre-packed /uploads/{id}/raw zip 500s AND
# whose per-entry fallback also stages nothing is re-attempted EVERY resume, each attempt burning
# ~9 min (a ~60 s-retry CD read + up to 8 fallback failures) — so a handful of permanently-broken
# uploads (e.g. iTP_-amqTACgDJsJVfQRvA, 4P6jmC…; the `fetch_error` rejections) waste growing time.
# We persist a per-upload consecutive-failed-pass count and stop attempting one once it reaches
# this threshold; a pass that stages ANY of the upload's entries clears the count (so a transient
# 500 that later recovers is not abandoned). 3 gives a genuinely-transient outage a few resumes to
# clear before we give up. Clearing the skip-list file re-attempts everything (entries stay in the
# keep-list — nothing is lost, only deferred). (Root-caused from the re-burn seen in both
# nomad-pipeline-34* logs, 2026-08-21.)
_DEAD_UPLOAD_MAX_FAILS = 3
_DEAD_UPLOADS_NAME = "nomad_dead_uploads.json"


def _load_dead_uploads(path: Path) -> dict[str, int]:
    """Load the {upload_id: consecutive-failed-pass count} skip-list, {} if absent/unreadable."""
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
        return {str(k): int(v) for k, v in data.items()} if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_dead_uploads(path: Path, counts: dict[str, int]) -> None:
    """Persist the skip-list atomically (drop zero counts to keep the file small)."""
    live = {k: v for k, v in counts.items() if v > 0}
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(live))
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)


class RecordTooBig(Exception):
    """One entry's own footprint exceeds the WHOLE disk/inode budget — no parse+purge can
    make room, so it is skipped (logged ``record_exceeds_disk_budget``) rather than
    deferred, which would livelock the pacing loop. For NOMAD this only bites under an
    absurdly small ``--max-disk-bytes``/``--max-disk-files`` (a single vasprun is tiny),
    but it keeps the valve's forward-progress guarantee identical to the Zenodo fetch."""


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
    props = results.get("properties") or {}
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
            # available_properties is NOMAD's authoritative list of derived properties the
            # normalizer found for this entry — the primary source of DOS/eigenvalue
            # availability (see nomad_metadata_availability). Kept slim (just this list).
            "properties": {"available_properties": props.get("available_properties") or []},
        },
    }


def normalize_doi(text: str) -> str | None:
    """Extract a bare, lower-cased DOI (``10.xxxx/…``) from a string/URL, else None."""
    if not text:
        return None
    m = re.search(r"10\.\d{4,9}/[^\s\"'<>]+", str(text).strip(), re.IGNORECASE)
    return m.group(0).rstrip(".").lower() if m else None


# NOMAD normalizer available-property -> our availability flag. NOMAD's parsed metadata is
# the AUTHORITATIVE source of electronic-structure availability (mentor/memo): for VASP-DFT
# direct uploads it carries DOS (`dos_electronic_new` ~84%, legacy `dos_electronic` ~7%) and,
# rarely, an electronic band structure (`band_structure_electronic` ~0.4% — the eigenvalue
# signal). Charge density / wavefunction / local potential / ELF / PROCAR-projections and
# magnetization are NOT in NOMAD's metadata, so those fall back to the filename scan of the
# rawdir listing (+ the shared parser's embedded-vasprun probe for dos/eigenvalues/projected).
# The unreliable `trajectory` available-property is deliberately NOT mapped (docs/NOMAD_HARVEST.md).
_NOMAD_AVAIL_FROM_PROP = {
    "dos_electronic_new": "dos",
    "dos_electronic": "dos",
    "band_structure_electronic": "eigenvalues",
    "eigenvalues_electronic": "eigenvalues",
}


def nomad_metadata_availability(entry: dict[str, Any]) -> dict[str, bool]:
    """Availability flags derivable from an entry's NOMAD metadata (authoritative).

    Reads ``results.properties.available_properties`` (preserved by :func:`slim_candidate`)
    and maps the electronic-structure entries to our flags. Returns only the flags NOMAD's
    metadata actually asserts True; everything else is left to the filename fallback in
    :func:`build_fetched_entry`. Empty dict when the field is absent (older keep-lists)."""
    props = (entry.get("results") or {}).get("properties") or {}
    out: dict[str, bool] = {}
    for ap in props.get("available_properties") or []:
        kind = _NOMAD_AVAIL_FROM_PROP.get(str(ap))
        if kind:
            out[kind] = True
    return out


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


def _rawdir_size(rawdir_listing: dict[str, Any], path: str) -> int:
    """Declared size (bytes) of a file in a rawdir listing, 0 if absent/unknown.

    NOMAD serves raw files verbatim and we stage them byte-for-byte (never decompressing),
    so the declared size equals the on-disk size — which makes it an *exact* pre-flight
    check against the disk budget (unlike a Zenodo archive, whose extracted size the header
    cannot predict; hence Zenodo charges as-written and NOMAD can also check up front)."""
    for f in rawdir_listing.get("files") or []:
        if (f.get("path") or "") == path:
            try:
                return int(f.get("size") or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def _byte_charger(budget: StagingBudget | None,
                  own: list[int] | None) -> Callable[[int], None] | None:
    """An ``on_chunk`` hook that charges the disk-byte budget as each chunk lands.

    Returns None when there is no active budget (the client then streams unmetered). A
    refused charge raises :class:`BudgetExceeded` — for the reclaimable case
    ``StagingBudget.charge`` raises it itself; the returned-False (unfittable) case is
    turned into the same signal here, which after the up-front :meth:`StagingBudget.check`
    pre-flight in :func:`stage_entry` is effectively unreachable (a defensive belt)."""
    if budget is None or not budget.enabled:
        return None

    def on_chunk(n_bytes: int) -> None:
        if not budget.charge(n_bytes, 0, own=own):
            raise BudgetExceeded("disk-byte budget reached mid-file")

    return on_chunk


def _refund_and_delete(dest: Path, budget: StagingBudget | None,
                       own: list[int] | None) -> None:
    """Roll a staged entry back: refund its EXACT reservation (``own`` — bytes+inodes
    charged, which reverses cleanly whether the tree is whole or partial) and delete its
    tree. One entry stages under its own ``<raw_dir>/<entry_id>/``, so a worker rolling one
    back never touches another's data."""
    if budget is not None and own is not None:
        budget.refund(own[0], own[1], own=own)
    shutil.rmtree(dest, ignore_errors=True)


def stage_entry(client: NomadClient, entry: dict[str, Any], raw_dir: Path,
                want_outcar: bool = False,
                budget: StagingBudget | None = None) -> tuple[Path, dict[str, Any]] | None:
    """Download an entry's primary VASP file (+ optional OUTCAR) into the staging tree.

    Lays files out as ``<raw_dir>/<entry_id>/extracted/calc/<canonical-name>`` — exactly
    the layout the shared parser expects (``local_dir=<entry_id>``, primary under
    ``extracted/``). Returns ``(dest_dir, rawdir_listing)`` or None if no VASP primary is
    present. Heavy-output availability is derived from the listing, never fetched.

    ``budget`` (:class:`StagingBudget`, thread-safe) paces staging against the CSD3 disk
    **and inode** quota. Every directory (``_charged_mkdir``), every file inode, and every
    byte (per chunk, via :func:`_byte_charger`) is charged as it lands and tracked in this
    record's own tally, so a rollback refunds *exactly* what was charged. Two failure modes:

    * budget full of OTHER records' data → :class:`BudgetExceeded` propagates (the entry is
      rolled back whole and the caller defers it to after a parse+purge reclaim);
    * this entry alone exceeds the whole budget → :class:`RecordTooBig` (skip + log, never
      deferred, so the pacing loop cannot spin on it).

    Either way the partial tree is rolled back and its reservation refunded before the
    exception leaves, so a deferred/skipped entry leaves nothing staged.
    """
    entry_id = entry["entry_id"]
    rd = client.rawdir(entry_id)
    mainfile = rd.get("mainfile") or ""
    primary = choose_primary(rd)
    if primary is None:
        return None
    to_stage: list[tuple[str, str]] = [primary]  # (remote_path, role)
    if want_outcar and primary[1] == "vasprun":
        for f in rd.get("files") or []:
            p = f.get("path") or ""
            if _OUTCAR_RE.search(p.rsplit("/", 1)[-1]):
                to_stage.append((p, "outcar"))
                break

    dest = raw_dir / entry_id
    calc_dir = dest / "extracted" / "calc"
    own: list[int] | None = None
    if budget is not None and budget.enabled:
        budget.begin_record()          # per-thread own-usage tally (workers stage in parallel)
        own = budget.own_handle()
    try:
        # Inodes for the staging dirs (charged before creation; raises/False if over budget).
        if not _charged_mkdir(calc_dir, budget, own):
            raise RecordTooBig(f"{entry_id}: staging dirs exceed the whole inode budget")
        # Up-front fit check on the declared footprint (exact for NOMAD — see _rawdir_size):
        # unfittable => RecordTooBig (skip); budget full of others => BudgetExceeded (defer).
        if budget is not None and budget.enabled:
            declared = sum(_rawdir_size(rd, p) for p, _role in to_stage)
            if not budget.check(declared, len(to_stage), own=own):
                raise RecordTooBig(
                    f"{entry_id}: {declared} B / {len(to_stage)} file(s) exceed the whole "
                    f"disk budget")
        for remote_path, role in to_stage:
            if (budget is not None and budget.enabled
                    and not budget.charge(0, 1, own=own)):   # one inode for this file
                raise BudgetExceeded("inode budget reached before staging a raw file")
            client.download_raw_file(
                entry_id, raw_path_rel(remote_path, mainfile),
                calc_dir / canonical_staged_name(remote_path, role),
                on_chunk=_byte_charger(budget, own))
    except BaseException:
        _refund_and_delete(dest, budget, own)
        raise
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
    # Availability: NOMAD's parsed metadata is the PRIMARY source (authoritative for DOS /
    # eigenvalues — see nomad_metadata_availability); the filename scan of the rawdir listing
    # is the FALLBACK for everything NOMAD does not index (charge density / wavefunction /
    # local potential / ELF / projections). OR the two so neither undercounts the other, and
    # the shared parser adds any dos/eigenvalues/projected embedded in the vasprun on top. A
    # NOMAD entry is exactly one calc (one mainfile -> one entry), so this is already per-calc.
    avail = _safe_members(names)
    for kind, present in nomad_metadata_availability(entry).items():
        if present:
            avail["availability"][kind] = True
            avail["availability_files"].setdefault(kind, "nomad:available_properties")
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


# --- fetch: targeted extraction from the pre-packed upload zip ------------------
# NOMAD stores each published upload as ONE zip (raw-public.plain.zip); GET /uploads/{id}/raw
# streams it straight off disk with HTTP Range + multi-range. So the fetch addresses data BY
# UPLOAD: read each upload's central directory once, then multi-range-pull just the kept
# entries' mainfiles out of the zip (see nomad_harvest.upload_zip + docs/NOMAD_HARVEST.md §3).
# That is ~15-30 MB/s vs ~0.3 for the old entries/raw/query path, which re-parses the whole
# upload zip's central directory ~4x per file. The endpoint is rate-limited to one connection
# per IP every ~5 s, so the fetch is SERIAL (concurrency cannot help); grouping entries by
# upload keeps the request count small (~30k for the whole 7.1M corpus). An upload whose zip
# cannot be read, or any individual member that fails to extract, falls back to the per-entry
# entries/{id}/raw path (a SEPARATE throttle bucket) — coverage is never reduced.


def _role_of(name: str) -> str | None:
    """Calc-unit role of a filename: ``vasprun``/``outcar``, else None."""
    base = name.rsplit("/", 1)[-1]
    if _VASPRUN_RE.search(base):
        return "vasprun"
    if _OUTCAR_RE.search(base):
        return "outcar"
    return None


def _dir_of(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""


def _sibling_outcar(members: dict[str, ZipMember], mainfile: str) -> ZipMember | None:
    """First OUTCAR member in the same directory as ``mainfile`` (for ``--want-outcar``)."""
    d = _dir_of(mainfile)
    for name, m in members.items():
        if _dir_of(name) == d and _OUTCAR_RE.search(name.rsplit("/", 1)[-1]):
            return m
    return None


def _cd_listing_for_entry(members: dict[str, ZipMember], mainfile: str) -> dict[str, Any]:
    """A synthetic rawdir listing (``{"mainfile", "files": [{"path","size"}]}``) of the files
    in the entry's calc directory, drawn from the upload's central directory. Fed to
    :func:`build_fetched_entry` so availability (CHGCAR/DOSCAR/…) is derived from the zip's own
    member list — reliable and free, replacing the old fragile per-batch ``rawdir/query`` call.
    """
    d = _dir_of(mainfile)
    files = [{"path": name, "size": m.uncomp_size}
             for name, m in members.items() if _dir_of(name) == d]
    return {"mainfile": mainfile, "files": files}


def _fallback_stage(client: NomadClient, entry: dict[str, Any], raw_dir: Path,
                    want_outcar: bool, budget: StagingBudget
                    ) -> tuple[dict[str, Any] | None, tuple[str, str] | None]:
    """Per-entry fallback (the robust ``entries/{id}/raw`` path — a SEPARATE throttle bucket
    from the uploads endpoint) for an entry the pre-packed path could not deliver. Returns
    ``(fetched_entry|None, (reason, detail)|None)``; raises :class:`BudgetExceeded` to defer."""
    res = stage_entry(client, entry, raw_dir, want_outcar=want_outcar, budget=budget)
    if res is None:
        return None, ("no_vasp_primary", "")
    dest, rd = res
    fe = build_fetched_entry(entry, raw_dir, dest, rd)
    if fe is None:
        _refund_and_delete(dest, budget, budget.own_handle() if budget.enabled else None)
        return None, ("no_calc_units_after_stage", "")
    return fe, None


def _fallback_entries(client: NomadClient, entries: list[dict[str, Any]], raw_dir: Path,
                      budget: StagingBudget, want_outcar: bool, w: JsonlWriter,
                      done: set[str], rej: RejectionLogger, stats: dict[str, Any],
                      max_records: int | None) -> bool:
    """Per-entry fallback for a list of entries. Returns True if the disk valve deferred.

    Bails out after ``_FALLBACK_MAX_CONSEC_FAIL`` consecutive failures: an upload whose pre-packed
    endpoint 500s falls back here in bulk, and if ``entries/{id}/rawdir`` is also erroring every
    call costs ~60 s of retries — so without this a single bad upload would consume the whole
    wallclock (root-caused from a live stall on upload 4P6jmC…, 2026-08-20). The skipped entries
    stay out of ``done`` and are retried on the next resume."""
    consec_fail = 0
    for entry in entries:
        eid = entry["entry_id"]
        if eid in done:
            continue
        if max_records and stats["staged"] >= max_records:
            return False
        if budget.full():
            return True
        try:
            fe, reject = _fallback_stage(client, entry, raw_dir, want_outcar, budget)
        except BudgetExceeded:
            return True
        except RecordTooBig as exc:
            rej.reject("nomad_fetch", eid, "record_exceeds_disk_budget", detail=str(exc))
            stats["failed"] += 1
            continue
        except Exception as exc:  # noqa: BLE001 - one bad entry must not abort the run
            rej.reject("nomad_fetch", eid, "fetch_error", detail=f"{type(exc).__name__}: {exc}")
            stats["failed"] += 1
            consec_fail += 1
            if consec_fail >= _FALLBACK_MAX_CONSEC_FAIL:
                logger.warning("per-entry fallback: %d consecutive failures (endpoint likely "
                               "erroring for this upload); skipping its remaining entries this "
                               "pass — they retry on the next resume", consec_fail)
                break
            continue
        consec_fail = 0                                    # a success clears the streak
        stats["per_entry_fallback"] += 1
        if fe is None:
            assert reject is not None
            rej.reject("nomad_fetch", eid, reject[0], detail=reject[1])
            stats["failed"] += 1
        else:
            w.write(fe)
            done.add(eid)
            stats["staged"] += 1
    return False


# Whole-vs-targeted chooser. The /uploads/{id}/raw endpoint costs ~5 s per request (1 in-flight
# per IP), so TARGETED needs ceil(n/256)+1 throttled requests per upload, while a WHOLE-STREAM is
# a few transfer-bound requests spanning every wanted member (upload_zip.stream_members, chunked).
# Whole-stream transfers the whole member SPAN (wanted + interior bloat); targeted transfers only
# the WANTED bytes. So the trade-off is BANDWIDTH-DEPENDENT (root-caused live 2026-08-20): at a
# HIGH transfer rate the fetch is throttle-bound, so whole's ~few requests beat targeted's
# ceil(n/256)+1 and it wins; at a LOW rate the fetch is transfer-bound, so whole's ~1.35x extra
# bloat bytes make it SLOWER than targeted, which moves fewer bytes. We therefore compare modelled
# wall-times at the CURRENT achieved rate (`rate_bps`, an aggregate the fetch loop feeds in): a
# slow server automatically favours targeted, a fast one favours whole. This fixes the earlier
# rate-independent cap, which over-fetched (chose whole) on a slow server. Only the FETCH mechanism
# differs; staged files, calc_units, availability and provenance are byte-identical either way.
# No absolute span cap: stream_members chunks a big span into bounded ~512 MB requests.
_DEFAULT_RATE_BPS = 4 << 20            # initial rate guess before any upload completes (favours targeted)
_MIN_RATE_BPS = 1 << 20                # floor so a 0/tiny estimate can't blow up the wall-time model


def _wanted_members(members: dict[str, ZipMember], entries: list[dict[str, Any]],
                    want_outcar: bool) -> list[ZipMember]:
    """The zip members this upload's kept entries would stage (each mainfile + optional sibling
    OUTCAR) — the same set the fetch admits, used to size the whole-vs-targeted decision."""
    out: list[ZipMember] = []
    for entry in entries:
        mf = entry.get("mainfile") or ""
        m = members.get(mf)
        if m is None or _role_of(mf) is None:
            continue
        out.append(m)
        if want_outcar and _role_of(mf) == "vasprun":
            oc = _sibling_outcar(members, mf)
            if oc is not None:
                out.append(oc)
    return out


def _should_whole_stream(members: dict[str, ZipMember], entries: list[dict[str, Any]],
                         want_outcar: bool, rate_bps: float) -> bool:
    """True if this upload is cheaper fetched as a (chunked) whole-stream than as targeted
    multi-range requests, at the CURRENT achieved transfer rate ``rate_bps`` (see the module
    comment above). Compares modelled wall-times: whole = one transfer-bound stream over the member
    SPAN; targeted = ceil(n/256)+1 requests, each ≥ the ~5 s throttle, moving only the WANTED bytes.
    At a low rate the extra bloat bytes make whole lose; at a high rate the saved requests make it
    win. False for an upload needing only one member batch (targeted is already minimal)."""
    wanted = _wanted_members(members, entries, want_outcar)
    n = len(wanted)
    if n <= upload_zip.MAX_RANGES_PER_REQUEST:          # <=1 targeted batch already: whole saves ~nothing
        return False
    wanted_bytes = sum(m.on_disk_size for m in wanted)
    if wanted_bytes <= 0:
        return False
    ends = [m.local_offset + 30 + len(m.name) + upload_zip._LOCAL_EXTRA_PAD + m.comp_size
            for m in wanted]
    span = max(ends) - min(m.local_offset for m in wanted)
    rate = max(rate_bps, _MIN_RATE_BPS)
    interval = NomadClient.UPLOAD_MIN_INTERVAL
    n_req_targeted = math.ceil(n / upload_zip.MAX_RANGES_PER_REQUEST) + 1
    t_targeted = max(n_req_targeted * interval, wanted_bytes / rate)   # throttle- OR transfer-bound
    t_whole = interval + span / rate                                  # one transfer-bound stream
    return t_whole < t_targeted


def _maybe_log_progress(stats: dict[str, Any], client: NomadClient, t0: float,
                        req0: int, next_log: list[int]) -> None:
    """Emit a ``nomad fetch progress`` line once ``staged`` crosses the next
    ``_FETCH_LOG_EVERY`` threshold. ``next_log`` is a 1-element mutable cursor so it advances
    ACROSS uploads AND across the flushes INSIDE one upload — the latter is what gives live
    visibility during a long single targeted upload (a 16894-entry / 33 GB upload can take ~80 min;
    without this, nothing logged between its start line and completion, so `tail -f` looked frozen)."""
    if stats["staged"] < next_log[0]:
        return
    e = max(time.monotonic() - t0, 1e-6)
    logger.info("nomad fetch progress: %d staged | %.1f MB/s | %.2f req/s | %.1f entries/s | "
                "%d whole + %d targeted uploads",
                stats["staged"], stats["staged_bytes"] / (1 << 20) / e,
                (getattr(client, "upload_gets", 0) - req0) / e, stats["staged"] / e,
                stats["whole_uploads"], stats["targeted_uploads"])
    while stats["staged"] >= next_log[0]:               # advance past every crossed threshold
        next_log[0] += _FETCH_LOG_EVERY


def _fetch_upload(client: NomadClient, upload_id: str, entries: list[dict[str, Any]],
                  raw_dir: Path, budget: StagingBudget, want_outcar: bool, w: JsonlWriter,
                  done: set[str], rej: RejectionLogger, stats: dict[str, Any],
                  max_records: int | None, rate_bps: float = _DEFAULT_RATE_BPS,
                  progress_cb: Callable[[], None] | None = None
                  ) -> tuple[bool, bool]:
    """Fetch one upload's kept entries by targeted extraction from its pre-packed zip.

    Reads the upload's central directory once, admits each entry against the disk/inode valve
    (reserving its EXACT footprint from the CD), and pulls the members in multi-range batches,
    each CRC-verified. Writes staged entries to ``w``; anything the zip cannot deliver goes to
    the per-entry fallback. Returns ``(deferred, cd_unavailable)``: ``deferred`` True if the
    valve deferred (budget full of reclaimable data → caller stops and the pacing loop resumes
    this part after a parse+purge); ``cd_unavailable`` True if the pre-packed zip's central
    directory could not be read (the dead-upload signal for the skip-list — see fetch_candidates).
    """
    try:
        members, _total = upload_zip.read_central_directory(client, upload_id)
    except UploadNotAvailable as exc:
        logger.info("upload %s not addressable (%s); per-entry fallback for %d entries",
                    upload_id, exc, len(entries))
        deferred = _fallback_entries(client, entries, raw_dir, budget, want_outcar,
                                     w, done, rej, stats, max_records)
        return deferred, True

    # Choose the fetch mechanism for this whole upload: one transfer-bound stream (low-bloat,
    # many entries) or targeted multi-range batches (high-bloat or few entries). When streaming,
    # the batch is NOT flushed mid-loop (below) — the single final flush spans every admitted
    # member in one request.
    whole = _should_whole_stream(members, entries, want_outcar, rate_bps)
    stats["whole_uploads" if whole else "targeted_uploads"] += 1
    # Per-upload log = the live signal of what the fetch is doing RIGHT NOW. A whole-stream pulls the
    # whole upload in ONE request and commits its entries (manifest + stats["staged"]) only when that
    # request FINISHES, so during a big low-bloat upload's multi-minute stream the counters and the
    # periodic tracker look frozen even though members are landing in raw/. This line fires as the
    # upload STARTS, so a long download is visible (and shows the whole/targeted choice + its size).
    _wm = _wanted_members(members, entries, want_outcar)
    _span_mb = (max((m.local_offset + 30 + len(m.name) + upload_zip._LOCAL_EXTRA_PAD + m.comp_size
                     for m in _wm), default=0)
                - min((m.local_offset for m in _wm), default=0)) / (1 << 20)
    logger.info("fetch upload %s: %d entries via %s (%d members, span %.0f MB)", upload_id,
                len(entries), "whole-stream" if whole else "targeted", len(_wm), _span_mb)

    pending: list[tuple[dict[str, Any], list[tuple[ZipMember, Path, str]], list[int]]] = []
    batch: list[tuple[ZipMember, Path]] = []
    to_fallback: list[dict[str, Any]] = []
    deferred = False

    def flush() -> None:
        nonlocal pending, batch
        if not batch:
            return
        results = (upload_zip.stream_members(client, upload_id, batch) if whole
                   else upload_zip.fetch_members(client, upload_id, batch))
        for entry, plan, own in pending:
            dest_root = raw_dir / entry["entry_id"]
            if all(results.get(dp) for _m, dp, _r in plan):
                listing = _cd_listing_for_entry(members, entry.get("mainfile") or "")
                fe = build_fetched_entry(entry, raw_dir, dest_root, listing)
                if fe is not None:
                    w.write(fe)
                    done.add(entry["entry_id"])
                    stats["staged"] += 1
                    stats["staged_bytes"] += sum(m.on_disk_size for m, _dp, _r in plan)
                else:                                   # staged files but no parseable calc unit
                    _refund_and_delete(dest_root, budget, own)
                    rej.reject("nomad_fetch", entry["entry_id"],
                               "no_calc_units_after_stage", detail="")
                    stats["failed"] += 1
            else:                                       # a member failed (CRC/short/transport)
                _refund_and_delete(dest_root, budget, own)
                to_fallback.append(entry)
        pending = []
        batch = []
        if progress_cb is not None:                     # live progress WITHIN a big upload's flushes
            progress_cb()

    for entry in entries:
        if max_records and stats["staged"] >= max_records:
            break
        eid = entry["entry_id"]
        mainfile = entry.get("mainfile") or ""
        member = members.get(mainfile)
        role = _role_of(mainfile)
        if member is None or role is None:              # mainfile absent from the zip (rare)
            to_fallback.append(entry)
            continue
        targets = [(member, role)]
        if want_outcar and role == "vasprun":
            oc = _sibling_outcar(members, mainfile)
            if oc is not None:
                targets.append((oc, "outcar"))
        footprint_bytes = sum(m.on_disk_size for m, _ in targets)
        footprint_inodes = len(targets) + 3            # <entry_id>/extracted/calc + the files
        own = [0, 0]
        try:
            fits = budget.charge(footprint_bytes, footprint_inodes, own=own)
        except BudgetExceeded:
            deferred = True                            # budget full of reclaimable data -> stop
            break
        if not fits:                                   # this entry alone exceeds the budget
            rej.reject("nomad_fetch", eid, "record_exceeds_disk_budget",
                       detail=f"{footprint_bytes} B / {footprint_inodes} inode(s)")
            stats["failed"] += 1
            continue
        calc_dir = raw_dir / eid / "extracted" / "calc"
        plan = [(m, calc_dir / canonical_staged_name(m.name, r), r) for m, r in targets]
        pending.append((entry, plan, own))
        batch.extend((m, dp) for m, dp, _r in plan)
        # Targeted flushes each full multi-range batch as it fills; whole-stream accumulates the
        # whole upload and streams it all in the single final flush (one transfer-bound request).
        if not whole and len(batch) >= upload_zip.MAX_RANGES_PER_REQUEST:
            flush()
    flush()                                            # stage the admitted remainder, then...

    if to_fallback and not deferred:                   # ...recover anything the zip missed
        deferred = _fallback_entries(client, to_fallback, raw_dir, budget, want_outcar,
                                     w, done, rej, stats, max_records)
    return deferred, False


def _dataset_record_ids(dataset_dir: str | Path) -> set[str]:
    """entry_ids already parsed into the dataset (``provenance.record_id`` in ``metadata.jsonl``),
    for the global fetch-skip. Empty set if there is no dataset yet."""
    ids: set[str] = set()
    meta = Path(dataset_dir) / "metadata.jsonl"
    if not meta.is_file():
        return ids
    for rec in read_jsonl(meta):
        rid = (rec.get("provenance") or {}).get("record_id")
        if rid:
            ids.add(str(rid))
    return ids


def fetch_candidates(client: NomadClient, in_path: str | Path,
                     raw_dir: str | Path = config.RAW_DIR,
                     out_path: str | Path | None = None,
                     want_outcar: bool = False,
                     max_records: int | None = None,
                     max_disk_bytes: int | None = None,
                     max_disk_files: int | None = None,
                     dataset_dir: str | Path | None = None,
                     dataset_record_ids: set[str] | None = None) -> dict[str, Any]:
    """Stage 2: pull each candidate's mainfile out of its upload's PRE-PACKED zip.

    The default (and only) NOMAD fetch path. Groups the input's entries by ``upload_id`` and
    processes upload by upload — reading each upload's central directory once and multi-range
    fetching just the wanted members (``nomad_harvest.upload_zip``). Same contract as before:

    * **Resumable** — entries already in ``out_path`` are skipped.
    * **Disk/inode paced** — ``max_disk_bytes``/``max_disk_files`` (``None`` = unbounded) set
      ``stopped_disk_budget`` so the overlapped ``pipeline`` reclaims (parse+purge) and resumes.
    * **Audited** — every drop is logged with a reason.
    * **Serial** — the ``/uploads/{id}/raw`` endpoint allows one connection per IP every ~5 s,
      so there is no ``workers`` knob here (a 2nd connection just 429s). Requests are paced in
      :meth:`~nomad_harvest.client.NomadClient.upload_raw_get`.

    Coverage is identical to the exact-mainfile design: the vasprun (or OUTCAR for OUTCAR-
    mainfile entries) is fetched whole, so the shared parser recovers full ``calc_parameters``;
    availability is derived from the upload's central directory + ``available_properties``.
    """
    raw_dir = Path(raw_dir)
    manifests = raw_dir.parent / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    out = Path(out_path) if out_path else manifests / "nomad_fetched.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    done = {rec["recid"] for rec in read_jsonl(out)} if out.is_file() else set()
    # GLOBAL skip: also skip any entry whose calc is already in the dataset. The per-part
    # `.fetched.jsonl` resume only recognises entries done under the SAME PARTS split, so changing
    # PARTS between runs would otherwise re-download everything already parsed. Skipping by the
    # dataset's own record_ids makes a PARTS change (and any manifest mismatch) free of re-fetch —
    # a parsed entry is never re-downloaded. (Only skips PARSED entries; a fetched-but-unparsed
    # backlog is not in the dataset and still resumes normally.)
    # `dataset_record_ids` (precomputed once by the pipeline, HOISTED out of the per-part loop)
    # takes precedence over reading metadata.jsonl here — the read is O(dataset) and, called once
    # per part, was the dominant cost of the resume "churn" over already-done parts (measured 2026-08-21).
    n_dataset_skip = 0
    parsed_ids = dataset_record_ids if dataset_record_ids is not None else (
        _dataset_record_ids(dataset_dir) if dataset_dir is not None else None)
    if parsed_ids is not None:
        n_dataset_skip = len(parsed_ids - done)
        done |= parsed_ids
    rej = RejectionLogger(manifests / "nomad_fetch_rejections.jsonl")
    # Dead-upload skip-list: uploads whose pre-packed zip has failed the last few passes (see
    # _DEAD_UPLOAD_MAX_FAILS). Loaded once; updated + saved at the end of this call.
    dead_path = manifests / _DEAD_UPLOADS_NAME
    dead = _load_dead_uploads(dead_path)
    stats: dict[str, Any] = {"records": 0, "staged": 0, "failed": 0, "skipped_existing": 0,
                             "uploads": 0, "whole_uploads": 0, "targeted_uploads": 0,
                             "per_entry_fallback": 0, "staged_bytes": 0,
                             "dead_skipped": 0, "dead_pending": 0,
                             "stopped_disk_budget": False, "stopped_on": ""}
    raw_dir.mkdir(parents=True, exist_ok=True)
    base_b, base_f = (_dir_usage(raw_dir) if (max_disk_bytes or max_disk_files) else (0, 0))
    budget = StagingBudget(max_disk_bytes, max_disk_files, base_b, base_f)

    # Group this input's entries by upload_id (first-seen order). A pipeline part is bounded
    # and holds each upload WHOLE (split_by_upload), so this is bounded memory and guarantees
    # each upload's central directory is read exactly once.
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order: list[str] = []
    for entry in read_jsonl(in_path):
        stats["records"] += 1
        if entry.get("entry_id") in done:
            stats["skipped_existing"] += 1
            continue
        up = entry.get("upload_id") or ""
        if up not in groups:
            order.append(up)
        groups[up].append(entry)

    t0 = time.monotonic()
    req0 = getattr(client, "upload_gets", 0)
    next_log = [_FETCH_LOG_EVERY]                        # mutable cursor (advances inside a big upload too)

    def _progress() -> None:
        _maybe_log_progress(stats, client, t0, req0, next_log)

    with JsonlWriter(out) as w:
        for up in order:
            if budget.full() or (max_records and stats["staged"] >= max_records):
                break
            # Dead-upload skip: this upload's pre-packed zip has failed the last
            # _DEAD_UPLOAD_MAX_FAILS passes, so don't waste ~9 min/pass re-attempting it. Its
            # entries stay in the keep-list (retried if the skip-list is cleared) — never lost.
            if dead.get(up, 0) >= _DEAD_UPLOAD_MAX_FAILS:
                stats["dead_skipped"] += len(groups[up])
                continue
            stats["uploads"] += 1
            # Feed the CURRENT achieved transfer rate (aggregate over this call) to the whole-vs-
            # targeted chooser, so it prefers targeted on a slow server and whole on a fast one.
            _el = time.monotonic() - t0
            rate_bps = (stats["staged_bytes"] / _el if stats["staged_bytes"] > 0 and _el > 1
                        else float(_DEFAULT_RATE_BPS))
            staged_before = stats["staged"]
            stop, cd_unavailable = _fetch_upload(client, up, groups[up], raw_dir, budget,
                                                 want_outcar, w, done, rej, stats, max_records,
                                                 rate_bps, progress_cb=_progress)
            # Update the dead-upload skip-list: a zip whose CD is unreadable AND whose per-entry
            # fallback staged nothing is a failed pass (increment); any progress clears the count
            # (so a transient 500 that later recovers is never abandoned). Only CD-unavailable
            # uploads are tracked — an individually-bad entry is rejected terminally elsewhere. A
            # disk-valve DEFER (`stop`) is not a failure — the upload wasn't fully attempted — so it
            # neither increments nor clears (it retries next pass with the count intact).
            if stop:
                break
            if cd_unavailable and stats["staged"] == staged_before:
                dead[up] = dead.get(up, 0) + 1
                if dead[up] < _DEAD_UPLOAD_MAX_FAILS:
                    stats["dead_pending"] += 1          # will be retried on a later pass
            elif up in dead:
                del dead[up]                            # recovered -> drop from the skip-list
            # Progress after each upload too (catches a whole-stream's single flush + the final
            # state); the intra-upload flushes above already logged during a long targeted upload.
            _progress()

    rej.close()
    _save_dead_uploads(dead_path, dead)
    if stats["dead_skipped"] or stats["dead_pending"]:
        logger.info("nomad fetch dead-uploads: %d entries skipped (>= %d failed passes), "
                    "%d uploads pending retry", stats["dead_skipped"], _DEAD_UPLOAD_MAX_FAILS,
                    stats["dead_pending"])
    if budget.full():
        stats["stopped_disk_budget"] = True
        stats["stopped_on"] = budget.pause or budget.hit_limit
    stats.update(budget.stats())
    stats["items_over_whole_budget"] = budget.unfittable
    stats["dataset_skipped"] = n_dataset_skip     # entries skipped because already in the dataset
    # Throughput diagnostics (the answer to "download- or request-limited?"): MB/s is the achieved
    # transfer rate; req/s against the ~0.2 (1-per-5s) throttle ceiling shows whether the endpoint's
    # rate limit or raw bandwidth is the bound; whole vs targeted shows the hybrid is engaging.
    elapsed = max(time.monotonic() - t0, 1e-6)
    stats["upload_requests"] = getattr(client, "upload_gets", 0) - req0
    stats["fetch_seconds"] = round(elapsed, 1)
    logger.info("nomad fetch: %s", stats)
    logger.info("nomad fetch rate: %.2f MB/s | %.2f req/s (ceil ~0.2 = 1-per-5s throttle) | "
                "%.1f entries/s | %d uploads = %d whole + %d targeted | fallback %d",
                stats["staged_bytes"] / (1 << 20) / elapsed,
                stats["upload_requests"] / elapsed, stats["staged"] / elapsed,
                stats["uploads"], stats["whole_uploads"], stats["targeted_uploads"],
                stats["per_entry_fallback"])
    return {"out": str(out), **stats}


def split_by_upload(in_path: str | Path, parts: int, out_dir: str | Path) -> dict[str, Any]:
    """Split a keep-list into ``parts`` part-files, keeping every upload's entries in ONE part.

    This is the NOMAD analog of :func:`zenodo_harvest.dataset_ops.split_manifest`, but it
    partitions by **upload** rather than by line: all of an upload's entries land in the same
    part, so the fetch reads each upload's central directory exactly once (a round-robin split
    would scatter an upload across all parts → one CD read per part). Parts are balanced by
    entry count via LPT (assign the biggest uploads first to the least-loaded part), fully
    deterministic (``(-count, upload_id)`` sort) so a resumed pipeline re-derives identical
    parts. Returns the same ``{"parts_written": [{"path","lines"}], …}`` shape as
    ``split_manifest`` so the shared pipeline drives it unchanged. Two streaming passes over the
    keep-list; only the per-upload counts (~3,800 entries) are held in memory.
    """
    if parts < 1:
        raise ValueError(f"parts must be >= 1, got {parts}")
    in_path = Path(in_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = defaultdict(int)
    for entry in read_jsonl(in_path):
        counts[entry.get("upload_id") or ""] += 1
    load = [0] * parts
    assign: dict[str, int] = {}
    for up, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        i = min(range(parts), key=lambda k: load[k])
        assign[up] = i
        load[i] += c
    stem = in_path.stem
    paths = [out_dir / f"{stem}.part-{i:03d}.jsonl" for i in range(parts)]
    handles = [p.open("w") for p in paths]
    lines = [0] * parts
    try:
        for entry in read_jsonl(in_path):
            i = assign.get(entry.get("upload_id") or "", 0)
            handles[i].write(json.dumps(entry) + "\n")
            lines[i] += 1
    finally:
        for h in handles:
            h.close()
    return {"parts_written": [{"path": str(paths[i]), "lines": lines[i]} for i in range(parts)],
            "parts": parts, "uploads": len(counts)}
