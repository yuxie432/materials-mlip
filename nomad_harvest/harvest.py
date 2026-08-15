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
import shutil
import tempfile
import threading
import zipfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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

from .client import CANDIDATE_REQUIRED, NomadClient, direct_upload_vasp_query

logger = logging.getLogger(__name__)


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


def fetch_candidates(client: NomadClient, in_path: str | Path,
                     raw_dir: str | Path = config.RAW_DIR,
                     out_path: str | Path | None = None,
                     want_outcar: bool = False,
                     max_records: int | None = None,
                     max_disk_bytes: int | None = None,
                     max_disk_files: int | None = None,
                     workers: int = 1) -> dict[str, Any]:
    """Stage 2: stage each candidate's vasprun (+optional OUTCAR) -> ``fetched.jsonl``.

    Resumable (entries already in ``out_path`` are skipped) and audited (every failure is
    logged with a reason, never silently dropped). Output is consumed unchanged by
    ``zenodo_harvest.parse``.

    ``max_disk_bytes`` / ``max_disk_files`` (``None`` = unbounded) are the **disk/inode
    valve**: staging stops cleanly once the raw tree reaches a limit, setting
    ``stopped_disk_budget`` in the returned stats, so the overlapped ``pipeline`` can
    ``parse``+``purge-raw`` to reclaim and then resume this batch. This is how a harvest far
    bigger than the CSD3 quota (1 TB **and** 1M inodes) runs inside it — paced, not capped
    on total volume. ``workers`` > 1 downloads that many entries concurrently (entries are
    independent; :class:`StagingBudget` is thread-safe), which matters because NOMAD's raw
    endpoint is latency-bound and allows ~10 concurrent. ``max_records`` caps entries newly
    staged THIS run (resumed skips don't count).

    The returned stats carry ``staged``/``failed``/``skipped_existing``,
    ``stopped_disk_budget``/``stopped_on``, and the budget's high-water marks
    (``peak_staged_bytes``/``peak_staged_files``) so a run is auditable from its summary.
    """
    raw_dir = Path(raw_dir)
    manifests = raw_dir.parent / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    out = Path(out_path) if out_path else manifests / "nomad_fetched.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    done = {rec["recid"] for rec in read_jsonl(out)} if out.is_file() else set()
    rej = RejectionLogger(manifests / "nomad_fetch_rejections.jsonl")
    stats: dict[str, Any] = {"records": 0, "staged": 0, "failed": 0, "skipped_existing": 0,
                             "stopped_disk_budget": False, "stopped_on": ""}

    # Create the staging root FIRST (it is the accounting root; a walk of it never counts
    # the root itself), then seed the budget with whatever a prior, not-yet-purged run left
    # staged (resume-aware). From here the accounting is incremental and exact.
    raw_dir.mkdir(parents=True, exist_ok=True)
    base_bytes, base_files = (_dir_usage(raw_dir) if (max_disk_bytes or max_disk_files)
                              else (0, 0))
    budget = StagingBudget(max_disk_bytes, max_disk_files, base_bytes, base_files)

    def _build(entry: dict[str, Any]) -> tuple[dict[str, Any] | None, tuple[str, str] | None]:
        """Stage + assemble one entry. Returns ``(fetched_entry|None, (reason,detail)|None)``.
        Raises :class:`BudgetExceeded` to signal a defer (budget full of reclaimable data)."""
        try:
            res = stage_entry(client, entry, raw_dir, want_outcar=want_outcar, budget=budget)
        except RecordTooBig as exc:
            return None, ("record_exceeds_disk_budget", str(exc))
        if res is None:
            return None, ("no_vasp_primary", "")
        dest, rd = res
        fe = build_fetched_entry(entry, raw_dir, dest, rd)
        if fe is None:  # staged a primary but no calc unit resolved: reclaim its space now
            _refund_and_delete(dest, budget, budget.own_handle() if budget.enabled else None)
            return None, ("no_calc_units_after_stage", "")
        return fe, None

    def _serial(count_records: bool) -> None:
        with JsonlWriter(out) as w:
            for entry in read_jsonl(in_path):
                if count_records:
                    stats["records"] += 1
                eid = entry["entry_id"]
                if eid in done:
                    if count_records:
                        stats["skipped_existing"] += 1
                    continue
                if max_records and stats["staged"] >= max_records:
                    break
                if budget.full():  # stop before starting a new record once a limit is hit
                    stats["stopped_disk_budget"], stats["stopped_on"] = True, budget.hit_limit
                    break
                try:
                    fe, reject = _build(entry)
                except BudgetExceeded:
                    stats["stopped_disk_budget"] = True
                    stats["stopped_on"] = budget.pause or budget.hit_limit
                    break
                except Exception as exc:  # noqa: BLE001 - one bad entry must not abort the run
                    rej.reject("nomad_fetch", eid, "fetch_error",
                               detail=f"{type(exc).__name__}: {exc}")
                    stats["failed"] += 1
                    continue
                if fe is None:
                    assert reject is not None
                    rej.reject("nomad_fetch", eid, reject[0], detail=reject[1])
                    stats["failed"] += 1
                    continue
                w.write(fe)
                done.add(eid)
                stats["staged"] += 1

    def _parallel(count_records: bool) -> None:
        rej_lock = threading.Lock()

        def _proc(entry: dict[str, Any]) -> tuple[str, str, Any]:
            eid = entry["entry_id"]
            try:
                fe, reject = _build(entry)
                return ("ok", eid, fe) if fe is not None else ("reject", eid, reject)
            except BudgetExceeded:
                return ("defer", eid, None)
            except Exception as exc:  # noqa: BLE001
                return ("error", eid, f"{type(exc).__name__}: {exc}")

        def _collect(fut: Any, w: JsonlWriter) -> None:
            kind, eid, payload = fut.result()
            if kind == "ok":
                w.write(payload)
                done.add(eid)
                stats["staged"] += 1
            elif kind == "reject":
                with rej_lock:
                    rej.reject("nomad_fetch", eid, payload[0], detail=payload[1])
                stats["failed"] += 1
            elif kind == "error":
                with rej_lock:
                    rej.reject("nomad_fetch", eid, "fetch_error", detail=payload)
                stats["failed"] += 1
            # "defer": budget full, nothing staged; the post-loop budget check reports it.

        with JsonlWriter(out) as w, ThreadPoolExecutor(max_workers=workers) as ex:
            inflight: set = set()
            submitted = 0
            for entry in read_jsonl(in_path):
                if count_records:
                    stats["records"] += 1
                eid = entry["entry_id"]
                if eid in done:
                    if count_records:
                        stats["skipped_existing"] += 1
                    continue
                if max_records and submitted >= max_records:
                    break
                if budget.full():
                    stats["stopped_disk_budget"], stats["stopped_on"] = True, budget.hit_limit
                    break
                inflight.add(ex.submit(_proc, entry))
                submitted += 1
                if len(inflight) >= workers * 2:  # bound memory + refresh admission decisions
                    finished, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    for f in finished:
                        _collect(f, w)
            for f in wait(inflight)[0]:  # drain the rest
                _collect(f, w)

    if workers > 1:
        _parallel(count_records=True)
        # Forward-progress guard (mirrors the Zenodo fetch): if a whole parallel pass staged
        # NOTHING and stopped on the budget, the in-flight entries each fitted alone but
        # filled the budget between them and were all rolled back — handing the same batch
        # back would repeat that forever. Retry serially against the just-emptied budget,
        # where each entry either stages or is recognised as too big for the whole budget.
        if stats["staged"] == 0 and budget.full():
            logger.warning("nomad parallel fetch staged nothing (%s limit); retrying "
                           "serially so the pacing loop cannot stall", budget.hit_limit)
            budget.pause = ""
            stats["stopped_disk_budget"], stats["stopped_on"] = False, ""
            _serial(count_records=False)
    else:
        _serial(count_records=True)

    rej.close()
    # Authoritative post-loop check: a worker can fill the budget after the submit loop's
    # last admission decision (or the very last record can), and stopped_disk_budget is what
    # tells `pipeline` to reclaim + resume this part (deferred entries are absent from the
    # manifest, so they are re-fetched).
    if budget.full():
        stats["stopped_disk_budget"] = True
        stats["stopped_on"] = budget.pause or budget.hit_limit
    stats.update(budget.stats())
    stats["items_over_whole_budget"] = budget.unfittable
    logger.info("nomad fetch: %s", stats)
    return {"out": str(out), **stats}


# --- BULK fetch (fixes #1 + #2: ~2 requests per BATCH, not 2 per entry) ----------
# Measured: per-entry fetch is latency-bound (~1 s/entry, 2 requests each) -> ~89 days for
# 7.1M, and per-entry concurrency trips NOMAD's rate limiter (503 storm). The fix is to
# collapse requests: one `rawdir/query` (availability) + one `raw/query` with
# `include_files=[<exact mainfiles>]` (the download, verified to return EXACTLY the wanted
# files -> no over-fetch) per batch. That turns a latency-bound fetch into a bandwidth-bound
# one, and lets a few concurrent long-lived batch streams multiply throughput WITHOUT
# spiking the request rate. Coverage/quality are unchanged: the exact mainfile is pulled
# (vasprun OR OUTCAR, so OUTCAR-only entries are kept), the FULL vasprun is fetched (so the
# enhanced parser recovers full calc_parameters), and any entry the zip can't deliver falls
# back to the per-entry path.

_BULK_MAX_BATCH = 300  # entries per raw/query zip (bounds zip size + server assembly time)


def _member_role(mainfile: str) -> str | None:
    """The calc-unit role of an entry's mainfile: ``vasprun``/``outcar``, else None."""
    base = (mainfile or "").rsplit("/", 1)[-1]
    if _VASPRUN_RE.search(base):
        return "vasprun"
    if _OUTCAR_RE.search(base):
        return "outcar"
    return None


def _outcar_in_listing(listing: dict[str, Any]) -> str | None:
    """First OUTCAR path in a rawdir listing (for ``--want-outcar``), else None."""
    for f in listing.get("files") or []:
        p = f.get("path") or ""
        if _OUTCAR_RE.search(p.rsplit("/", 1)[-1]):
            return p
    return None


def _stage_zip_member(zf: zipfile.ZipFile, member: str, out: Path,
                      budget: StagingBudget | None, own: list[int] | None) -> None:
    """Extract one zip member to ``out``, charging the disk/inode valve exactly as
    :func:`stage_entry` does per download. Raises :class:`RecordTooBig` (this file alone
    exceeds the whole budget) or :class:`BudgetExceeded` (budget full of reclaimable data →
    defer). The member is a VASP file stored verbatim in the zip, so the bytes written equal
    ``ZipInfo.file_size`` and charging per chunk keeps the on-disk footprint exact."""
    if not _charged_mkdir(out.parent, budget, own):
        raise RecordTooBig(f"{member}: staging dirs exceed the whole inode budget")
    size = zf.getinfo(member).file_size
    if budget is not None and budget.enabled:
        if not budget.check(size, 1, own=own):
            raise RecordTooBig(f"{member}: {size} B exceeds the whole disk budget")
        if not budget.charge(0, 1, own=own):
            raise BudgetExceeded("inode budget reached before a zip member")
    with zf.open(member) as src, out.open("wb") as dst:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            if budget is not None and budget.enabled and not budget.charge(len(chunk), 0, own=own):
                raise BudgetExceeded("disk-byte budget reached mid-member")
            dst.write(chunk)


def _bulk_stage_batch(
    client: NomadClient, batch: list[dict[str, Any]], raw_dir: Path,
    budget: StagingBudget | None, tmp_dir: Path, want_outcar: bool,
) -> tuple[dict[str, dict], list[dict], list[tuple[str, str, str]], bool]:
    """Fetch + stage one batch with TWO bulk requests. Returns ``(staged, fallback,
    rejects, deferred)``:

      * ``staged``   — ``{entry_id: fetched_entry}`` fully staged + assembled here;
      * ``fallback`` — entries the zip did not deliver (missing member / no role) → the
        caller retries them per-entry, so **coverage is never reduced**;
      * ``rejects``  — ``(entry_id, reason, detail)`` for entries that alone exceed the budget;
      * ``deferred`` — True if a valve limit was hit mid-batch (budget full of reclaimable
        data): the already-staged entries are kept, the rest are left for a resumed run.
    """
    ids = [e["entry_id"] for e in batch]
    try:
        listings = client.bulk_rawdir(ids)          # fix #2: availability for the whole batch
    except Exception as exc:  # noqa: BLE001 - availability is best-effort; proceed without it
        logger.debug("bulk_rawdir failed (%s); staging without availability", type(exc).__name__)
        listings = {}

    include: list[str] = []
    outcar_of: dict[str, str] = {}
    for e in batch:
        mf = e.get("mainfile") or ""
        include.append(mf)
        if want_outcar and _member_role(mf) == "vasprun":
            oc = _outcar_in_listing(listings.get(e["entry_id"], {}))
            if oc:
                include.append(oc)
                outcar_of[e["entry_id"]] = oc

    zpath = tmp_dir / f"batch_{ids[0][:10]}_{len(ids)}.zip"
    try:
        client.bulk_raw_zip(ids, {"include_files": include}, zpath)   # fix #1: one zip
    except Exception as exc:  # noqa: BLE001 - whole batch failed -> everyone falls back
        logger.warning("bulk zip failed for a %d-entry batch (%s); per-entry fallback",
                       len(batch), type(exc).__name__)
        return {}, list(batch), [], False

    staged: dict[str, dict] = {}
    fallback: list[dict] = []
    rejects: list[tuple[str, str, str]] = []
    deferred = False
    try:
        with zipfile.ZipFile(zpath) as zf:
            members = set(zf.namelist())
            for e in batch:
                eid, uid, mf = e["entry_id"], e.get("upload_id"), e.get("mainfile") or ""
                role = _member_role(mf)
                member = f"{uid}/{mf}"
                if role is None or member not in members:
                    fallback.append(e)                 # zip didn't deliver -> per-entry path
                    continue
                dest = raw_dir / eid
                own: list[int] | None = None
                if budget is not None and budget.enabled:
                    budget.begin_record()
                    own = budget.own_handle()
                try:
                    _stage_zip_member(
                        zf, member,
                        dest / "extracted" / "calc" / canonical_staged_name(mf, role),
                        budget, own)
                    oc = outcar_of.get(eid)
                    if oc and f"{uid}/{oc}" in members:
                        _stage_zip_member(
                            zf, f"{uid}/{oc}",
                            dest / "extracted" / "calc" / canonical_staged_name(oc, "outcar"),
                            budget, own)
                    # Availability (charge density / DOS / eigenvalues / …) comes from the
                    # rawdir file listing and is a MANDATORY provenance field. bulk_rawdir is
                    # best-effort (it can 5xx for a whole batch — the bulk endpoints are flaky);
                    # when it didn't cover this entry, recover the listing with a per-entry
                    # rawdir rather than writing an all-False availability block. This costs one
                    # extra request ONLY on the (rare) failure path — the common path stays ~2
                    # requests/batch — and guarantees availability is never silently dropped.
                    listing = listings.get(eid)
                    if not (listing and listing.get("files")):
                        try:
                            listing = client.rawdir(eid)
                        except Exception:  # noqa: BLE001 - keep the calc; availability stays empty
                            listing = {"files": []}
                    fe = build_fetched_entry(e, raw_dir, dest, listing)
                    if fe is None:                     # staged a primary but no calc unit
                        _refund_and_delete(dest, budget, own)
                        rejects.append((eid, "no_calc_units_after_stage", ""))
                    else:
                        staged[eid] = fe
                except RecordTooBig as exc:
                    _refund_and_delete(dest, budget, own)
                    rejects.append((eid, "record_exceeds_disk_budget", str(exc)))
                except BudgetExceeded:
                    _refund_and_delete(dest, budget, own)
                    deferred = True
                    break                              # keep what's staged; resume the rest
    finally:
        zpath.unlink(missing_ok=True)
    return staged, fallback, rejects, deferred


def fetch_candidates_bulk(
    client: NomadClient, in_path: str | Path,
    raw_dir: str | Path = config.RAW_DIR, out_path: str | Path | None = None,
    want_outcar: bool = False, max_records: int | None = None,
    max_disk_bytes: int | None = None, max_disk_files: int | None = None,
    workers: int = 1, batch_size: int = _BULK_MAX_BATCH,
    tmp_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Stage 2, BULK: stage the keep-list with ~2 requests per ``batch_size`` entries.

    The recommended fetch path (the per-entry :func:`fetch_candidates` is the fallback).
    Same contract — resumable via ``out_path``; the disk/inode valve sets
    ``stopped_disk_budget`` so the overlapped ``pipeline`` reclaims + resumes; every drop is
    audited — and identical coverage/quality (exact mainfiles → OUTCAR-only entries kept; the
    full vasprun → enhanced ``calc_parameters``; any undelivered entry falls back per-entry).

    ``workers`` streams that many batch zips concurrently — safe here because bulk requests
    are few and long-lived (they multiply bandwidth without tripping NOMAD's req/s limiter,
    unlike per-entry concurrency). The transient batch zips live in ``tmp_dir`` (node-local,
    e.g. ``$TMPDIR`` / CSD3 ``/local``), NOT under ``raw_dir``, so they never count against
    the staging quota.
    """
    raw_dir = Path(raw_dir)
    manifests = raw_dir.parent / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    out = Path(out_path) if out_path else manifests / "nomad_fetched.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    done = {rec["recid"] for rec in read_jsonl(out)} if out.is_file() else set()
    rej = RejectionLogger(manifests / "nomad_fetch_rejections.jsonl")
    stats: dict[str, Any] = {"records": 0, "staged": 0, "failed": 0, "skipped_existing": 0,
                             "bulk_fallback": 0, "batches": 0,
                             "stopped_disk_budget": False, "stopped_on": ""}
    raw_dir.mkdir(parents=True, exist_ok=True)
    base_b, base_f = (_dir_usage(raw_dir) if (max_disk_bytes or max_disk_files) else (0, 0))
    budget = StagingBudget(max_disk_bytes, max_disk_files, base_b, base_f)
    own_tmp = tmp_dir is None
    tmpd = Path(tmp_dir) if tmp_dir else Path(tempfile.mkdtemp(prefix="nomad_bulkzip_"))
    tmpd.mkdir(parents=True, exist_ok=True)
    write_lock = threading.Lock()
    rej_lock = threading.Lock()

    def _fallback_one(entry: dict[str, Any]) -> tuple[dict | None, tuple[str, str] | None]:
        """Per-entry staging (the robust path) for entries the bulk zip couldn't deliver."""
        try:
            res = stage_entry(client, entry, raw_dir, want_outcar=want_outcar, budget=budget)
        except RecordTooBig as exc:
            return None, ("record_exceeds_disk_budget", str(exc))
        if res is None:
            return None, ("no_vasp_primary", "")
        dest, rd = res
        fe = build_fetched_entry(entry, raw_dir, dest, rd)
        if fe is None:
            _refund_and_delete(dest, budget, budget.own_handle() if budget.enabled else None)
            return None, ("no_calc_units_after_stage", "")
        return fe, None

    def _process(batch: list[dict[str, Any]]) -> tuple[list, bool, int]:
        """Worker task: bulk-stage a batch + per-entry fallback. Returns
        ``(results, deferred, n_fallback)`` where results is ``[("ok", eid, fe) |
        ("reject", eid, (reason, detail))]``."""
        results: list = []
        staged, fallback, rejects, deferred = _bulk_stage_batch(
            client, batch, raw_dir, budget, tmpd, want_outcar)
        for eid, fe in staged.items():
            results.append(("ok", eid, fe))
        for eid, reason, detail in rejects:
            results.append(("reject", eid, (reason, detail)))
        if not deferred:
            for entry in fallback:
                try:
                    fe2, rj = _fallback_one(entry)
                except BudgetExceeded:
                    deferred = True
                    break
                if fe2 is not None:
                    results.append(("ok", entry["entry_id"], fe2))
                else:
                    assert rj is not None
                    results.append(("reject", entry["entry_id"], rj))
        return results, deferred, len(fallback)

    deferred = False

    def _handle(results: list, w: JsonlWriter) -> None:
        for kind, eid, payload in results:
            if kind == "ok":
                with write_lock:
                    w.write(payload)
                done.add(eid)
                stats["staged"] += 1
            else:  # reject
                with rej_lock:
                    rej.reject("nomad_fetch", eid, payload[0], detail=payload[1])
                stats["failed"] += 1

    def _batches() -> Any:
        batch: list[dict[str, Any]] = []
        for entry in read_jsonl(in_path):
            stats["records"] += 1
            if entry["entry_id"] in done:
                stats["skipped_existing"] += 1
                continue
            batch.append(entry)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    try:
        with JsonlWriter(out) as w, ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
            inflight: set = set()
            for batch in _batches():
                if deferred or budget.full() or (max_records and stats["staged"] >= max_records):
                    break
                stats["batches"] += 1
                inflight.add(ex.submit(_process, batch))
                if len(inflight) >= max(workers, 1):
                    finished, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    for f in finished:
                        results, dfr, nfb = f.result()
                        _handle(results, w)
                        stats["bulk_fallback"] += nfb
                        deferred = deferred or dfr
            for f in wait(inflight)[0]:                 # drain
                results, dfr, nfb = f.result()
                _handle(results, w)
                stats["bulk_fallback"] += nfb
                deferred = deferred or dfr
    finally:
        if own_tmp:
            shutil.rmtree(tmpd, ignore_errors=True)

    rej.close()
    if deferred or budget.full():
        stats["stopped_disk_budget"] = True
        stats["stopped_on"] = budget.pause or budget.hit_limit
    stats.update(budget.stats())
    stats["items_over_whole_budget"] = budget.unfittable
    logger.info("nomad bulk fetch: %s", stats)
    return {"out": str(out), **stats}
