"""Paper links for census records — three independent channels, each cached and resumable.

A deposit with bare metadata ("Accompanying data for …") usually still points at, or is pointed at
by, the paper it supports, and that paper's own metadata says whether it is a VASP study:

* **DataCite events** — Crossref papers whose reference lists cite a Zenodo DOI (the paper -> data
  direction, independent of what the depositor wrote): ``/events?prefix=10.5281&source-id=crossref
  &relation-type-id=references``, 46,171 links (2026-09-25), ~47 cursor pages of 1,000.
  (DataCite's per-DOI ``citationCount`` is mostly the depositor's own ``IsSupplementTo`` links,
  which the census already holds, and list responses omit the citing DOIs — so events it is.)
* **Europe PMC** — open-access full texts that mention VASP *and* Zenodo (~589 papers): the Zenodo
  identifiers in their data-availability statements. Independent of Zenodo AND of reference
  lists, which makes it the recall check for the whole method.
* **OpenAlex** — for each paper DOI (linked from a record, or citing one), whether its reference
  list cites the VASP method papers, plus its primary topic's field. Singleton lookups by DOI are
  free (list/filter calls are metered since 2026), so the lookups are cached one line per DOI.

Only anonymous sessions are used here: no contact e-mail and never the Zenodo token (those hosts
are not Zenodo). ``OPENALEX_API_KEY`` is passed through when set.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import requests

from zenodo_harvest.client import _parse_retry_after
from zenodo_harvest.manifest import JsonlWriter, read_jsonl

from .census import truncate_torn_tail
from .signals import norm_doi

logger = logging.getLogger(__name__)

USER_AGENT = "zenodo-census/0.1 (materials-mlip research)"
DATACITE_EVENTS = "https://api.datacite.org/events"
EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_FULLTEXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/{id}/fullTextXML"
OPENALEX_WORK = "https://api.openalex.org/works/doi:{doi}"
EPMC_QUERY = ('("VASP" OR "Vienna ab initio" OR "Vienna Ab initio Simulation Package") '
              'AND zenodo')
# OpenAlex ids of the VASP method papers (and Blöchl's PAW paper, which every VASP-PAW study
# cites): Kresse & Furthmüller 1996 PRB + CMS, Kresse & Joubert 1999, Blöchl 1994, Kresse &
# Hafner 1993 + 1994 — verified live 2026-09-25 (cited 124k/77k/86k/93k/46k/23k times).
VASP_WORKS = frozenset({"W2083222334", "W2007395042", "W1979544533", "W1970127494",
                        "W2079105963", "W2087698390"})
_ZENODO_ID_RES = (
    re.compile(r"10\.5281/zenodo\.(\d{1,10})", re.IGNORECASE),
    re.compile(r"zenodo\.org/(?:records?|deposit|uploads)/(\d{1,10})", re.IGNORECASE),
)


def new_session() -> requests.Session:
    """An anonymous session (no Authorization, no contact address)."""
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def zenodo_ids_in(text: str) -> list[str]:
    """Zenodo record ids named in free text (DOIs and record URLs), in first-seen order."""
    seen: dict[str, None] = {}
    for rx in _ZENODO_ID_RES:
        for m in rx.finditer(text or ""):
            seen.setdefault(m.group(1), None)
    return list(seen)


def _get(session: requests.Session, url: str, params: dict[str, Any] | None = None,
         timeout: float = 90, attempts: int = 5) -> requests.Response | None:
    """GET with bounded retries on 429/5xx/network errors (Retry-After honoured). None when the
    budget is spent; the caller treats that as transient (not cached)."""
    for attempt in range(attempts):
        try:
            r = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            logger.warning("GET %s: %s (retry %d)", url, type(exc).__name__, attempt + 1)
            time.sleep(min(2 ** attempt, 60))
            continue
        if r.status_code == 429 or r.status_code >= 500:
            wait = (_parse_retry_after(r.headers.get("Retry-After"), 2 ** attempt)
                    if r.status_code == 429 else 2 ** attempt)
            logger.warning("GET %s: HTTP %d (retry %d in %ss)", url, r.status_code,
                           attempt + 1, wait)
            time.sleep(min(wait, 120))
            continue
        return r
    return None


# ---------------------------------------------------------------------------
# DataCite: Crossref papers that reference Zenodo DOIs
# ---------------------------------------------------------------------------

def datacite_references(out_path: str | Path, session: requests.Session | None = None,
                        page_size: int = 1000, max_pages: int | None = None) -> dict[str, Any]:
    """Page every Crossref->Zenodo ``references`` event into ``out_path`` as
    ``{"zenodo_id", "citing_doi", "occurred"}`` lines. Resumable: the next-page cursor URL is
    kept in ``<out>.cursor`` after every page (a finished run leaves ``done``)."""
    out = Path(out_path)
    cursor_path = Path(str(out) + ".cursor")
    meta_path = Path(str(out) + ".meta.json")
    session = session or new_session()
    nxt = cursor_path.read_text().strip() if cursor_path.is_file() else ""
    if nxt == "done":
        return {"out": str(out), "status": "already complete"}
    truncate_torn_tail(out)

    def save_cursor(value: str) -> None:            # atomic: a kill never leaves half a URL
        tmp = cursor_path.with_name(cursor_path.name + ".tmp")
        tmp.write_text(value)
        os.replace(tmp, cursor_path)
    params: dict[str, Any] | None = None
    if not nxt:
        nxt = DATACITE_EVENTS
        params = {"prefix": "10.5281", "source-id": "crossref",
                  "relation-type-id": "references", "page[size]": page_size,
                  "page[cursor]": 1}
    pages = rows = 0
    with JsonlWriter(out) as w:
        while nxt:
            r = _get(session, nxt, params)
            if r is None or r.status_code != 200:
                raise RuntimeError(f"DataCite events page failed: "
                                   f"{'no response' if r is None else r.status_code}")
            j = r.json()
            if not meta_path.is_file() and (j.get("meta") or {}).get("total") is not None:
                meta_path.write_text(json.dumps({"total": j["meta"]["total"]}))
            for e in j.get("data") or []:
                a = e.get("attributes") or {}
                zid = zenodo_ids_in(str(a.get("obj-id") or ""))
                citing = norm_doi(str(a.get("subj-id") or ""))
                if zid and citing:
                    w.write({"zenodo_id": zid[0], "citing_doi": citing,
                             "occurred": (a.get("occurred-at") or "")[:10]})
                    rows += 1
            pages += 1
            nxt = ((j.get("links") or {}).get("next") or "") if j.get("data") else ""
            params = None
            save_cursor(nxt or "done")
            if max_pages and pages >= max_pages:
                break
    complete = cursor_path.read_text().strip() == "done"
    summary: dict[str, Any] = {"out": str(out), "pages": pages, "links": rows,
                               "complete": complete}
    if complete and meta_path.is_file():
        total = int(json.loads(meta_path.read_text()).get("total") or 0)
        have = sum(1 for _ in read_jsonl(out)) if out.is_file() else 0
        summary["expected_total"] = total
        if total and have < 0.95 * total:
            summary["warning"] = (f"only {have} of {total} events written — the pull may have "
                                  "ended early (delete the .cursor and re-run to redo it)")
            logger.warning("datacite references: %s", summary["warning"])
    logger.info("datacite references: %s", summary)
    return summary


def _with_concepts(out: dict[str, set[str]], versions: dict[str, str] | None
                   ) -> dict[str, set[str]]:
    """Also file each entry under its record's concept id (a paper often cites one VERSION's DOI;
    the census holds the newest version, matched by concept)."""
    for zid, vals in list(out.items()):
        concept = (versions or {}).get(zid)
        if concept and concept != zid:
            out.setdefault(concept, set()).update(vals)
    return out


def load_citing(path: str | Path, versions: dict[str, str] | None = None
                ) -> dict[str, set[str]]:
    """``zenodo_id -> {citing paper DOI}`` from :func:`datacite_references` output (keyed by the
    concept too when ``versions`` — :func:`load_versions` — is given)."""
    out: dict[str, set[str]] = {}
    if Path(path).is_file():
        for row in read_jsonl(path):
            out.setdefault(str(row["zenodo_id"]), set()).add(str(row["citing_doi"]))
    return _with_concepts(out, versions)


# ---------------------------------------------------------------------------
# Europe PMC: VASP papers whose full text names a Zenodo record
# ---------------------------------------------------------------------------

def epmc_mentions(out_path: str | Path, session: requests.Session | None = None,
                  query: str = EPMC_QUERY, page_size: int = 1000,
                  interval: float = 0.2) -> dict[str, Any]:
    """Search Europe PMC full text for ``query`` and write, per open-access hit, the Zenodo ids its
    full text names: ``{"source_id", "doi", "title", "year", "zenodo_ids"}``. Resumable per paper
    (ids already written are skipped); hits without an EPMC full text are logged as such."""
    out = Path(out_path)
    session = session or new_session()
    truncate_torn_tail(out)
    done = {str(r["source_id"]) for r in read_jsonl(out)} if out.is_file() else set()
    hits: list[dict[str, Any]] = []
    cursor = "*"
    while True:
        r = _get(session, EPMC_SEARCH, {"query": query, "format": "json", "pageSize": page_size,
                                        "cursorMark": cursor, "resultType": "lite"})
        if r is None or r.status_code != 200:
            raise RuntimeError("Europe PMC search failed")
        j = r.json()
        batch = (j.get("resultList") or {}).get("result") or []
        hits.extend(batch)
        nxt = j.get("nextCursorMark")
        if not batch or not nxt or nxt == cursor:
            break
        cursor = nxt
    stats = {"hits": len(hits), "fetched": 0, "no_fulltext": 0, "with_zenodo": 0,
             "skipped_done": 0}
    with JsonlWriter(out) as w:
        for h in hits:
            # PMC articles by PMCID; preprints (source PPR) have full text under their own id
            sid = str(h.get("pmcid") or (h.get("id") if h.get("source") == "PPR" else "") or "")
            if not sid:
                stats["no_fulltext"] += 1
                continue
            if sid in done:
                stats["skipped_done"] += 1
                continue
            time.sleep(interval)
            fr = _get(session, EPMC_FULLTEXT.format(id=sid), timeout=120)
            if fr is None or fr.status_code != 200:
                stats["no_fulltext"] += 1
                continue
            ids = zenodo_ids_in(fr.text)
            stats["fetched"] += 1
            stats["with_zenodo"] += bool(ids)
            w.write({"source_id": sid, "doi": norm_doi(str(h.get("doi") or "")),
                     "title": str(h.get("title") or "")[:200], "year": h.get("pubYear"),
                     "zenodo_ids": ids})
            done.add(sid)
    summary = {"out": str(out), "query": query, **stats}
    logger.info("europe pmc: %s", summary)
    return summary


def load_epmc(path: str | Path, versions: dict[str, str] | None = None
              ) -> dict[str, set[str]]:
    """``zenodo_id -> {EPMC source id}`` (keyed by the concept too, given ``versions``)."""
    out: dict[str, set[str]] = {}
    if Path(path).is_file():
        for row in read_jsonl(path):
            for zid in row.get("zenodo_ids") or []:
                out.setdefault(str(zid), set()).add(str(row["source_id"]))
    return _with_concepts(out, versions)


# ---------------------------------------------------------------------------
# Zenodo version ids -> concept ids (for links that cite an older version)
# ---------------------------------------------------------------------------

def linked_ids(datacite_path: str | Path, epmc_path: str | Path) -> set[str]:
    ids = set(load_citing(datacite_path)) | set(load_epmc(epmc_path))
    return ids


def resolve_versions(ids: Iterable[str], out_path: str | Path, client: Any,
                     batch: int | None = None) -> dict[str, Any]:
    """Map Zenodo record ids to their concept ids with batched version-aware searches
    (``q=recid:(a OR b …)``, ``all_versions=true``, 100 per request on the 30 req/min search
    endpoint; 25 without a token — Zenodo's anonymous page cap), appending ``{"id",
    "conceptrecid"}`` lines (``conceptrecid`` null when Zenodo returns nothing for that id —
    deleted/restricted — so a re-run skips it too). A batch that returns NO hit at all is not
    cached (that smells of a query problem, not of ids that do not exist) and is retried next run.
    ``all_versions=true`` is what makes older versions visible (verified live: ``recid:18774``, an
    old pymatgen version, → 0 hits without it, 1 with it). ``client`` is a ``ZenodoClient``."""
    out = Path(out_path)
    batch = batch or (100 if getattr(client, "token", None) else 25)
    truncate_torn_tail(out)
    done = {str(r["id"]) for r in read_jsonl(out)} if out.is_file() else set()
    todo = sorted({str(i) for i in ids if str(i).isdigit()} - done, key=int)
    stats = {"todo": len(todo), "resolved": 0, "unknown": 0, "batches_without_hits": 0}
    with JsonlWriter(out) as w:
        for i in range(0, len(todo), batch):
            chunk = todo[i:i + batch]
            page = client.search_page("recid:(" + " OR ".join(chunk) + ")", size=len(chunk),
                                      extra={"all_versions": "true"})
            found: dict[str, str] = {}
            for h in (page.get("hits") or {}).get("hits") or []:
                rid = str(h.get("id"))
                found[rid] = str(h.get("conceptrecid") or rid)
            if not found and len(chunk) >= 5:
                stats["batches_without_hits"] += 1
                logger.warning("resolve versions: no hit for a batch of %d ids (first %s) — not "
                               "cached; retried next run", len(chunk), chunk[0])
                continue
            for rid in chunk:
                w.write({"id": rid, "conceptrecid": found.get(rid)})
                stats["resolved" if rid in found else "unknown"] += 1
    logger.info("resolve versions: %s", stats)
    return {"out": str(out), **stats}


def load_versions(path: str | Path) -> dict[str, str]:
    """``record id -> concept id`` from :func:`resolve_versions` output (unknowns left out)."""
    out: dict[str, str] = {}
    if Path(path).is_file():
        for row in read_jsonl(path):
            if row.get("conceptrecid"):
                out[str(row["id"])] = str(row["conceptrecid"])
    return out


# ---------------------------------------------------------------------------
# OpenAlex: does a paper cite the VASP method papers? which field is it in?
# ---------------------------------------------------------------------------

class _Pacer:
    """At most one request START every ``interval`` seconds across threads."""

    def __init__(self, interval: float):
        self.interval = interval
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            time.sleep(start - now)


def openalex_row(doi: str, work: dict[str, Any] | None, status: int | str) -> dict[str, Any]:
    """The cached verdict for one DOI (``work`` = the OpenAlex response, or None)."""
    if not work:
        return {"doi": doi, "status": status}
    refs = {str(x).rsplit("/", 1)[-1] for x in work.get("referenced_works") or []}
    own = str(work.get("id") or "").rsplit("/", 1)[-1]
    topic = work.get("primary_topic") or {}
    return {"doi": doi, "status": status, "type": work.get("type"),
            "year": work.get("publication_year"),
            "n_refs": int(work.get("referenced_works_count") or len(refs)),
            # the linked DOI may BE a VASP method paper (it does not cite itself)
            "cites_vasp": bool(refs & VASP_WORKS) or own in VASP_WORKS,
            "field": (topic.get("field") or {}).get("display_name"),
            "subfield": (topic.get("subfield") or {}).get("display_name"),
            "topic": topic.get("display_name")}


def openalex_lookup(dois: Iterable[str], cache_path: str | Path, *,
                    session_factory: Any = new_session, workers: int = 4,
                    interval: float = 0.125, max_lookups: int | None = None) -> dict[str, Any]:
    """Look each DOI up in OpenAlex (free singleton calls), appending one verdict line per DOI to
    ``cache_path``; DOIs already cached are skipped. 404 (unknown to OpenAlex) is cached as such;
    transient failures are not cached, so a re-run retries them. ``interval`` paces request
    starts across the ``workers`` threads (default 8/s)."""
    cache = Path(cache_path)
    truncate_torn_tail(cache)
    have = {str(r["doi"]) for r in read_jsonl(cache)} if cache.is_file() else set()
    todo = sorted({d for d in dois if d and d not in have})
    if max_lookups:
        todo = todo[:max_lookups]
    pacer = _Pacer(interval)
    tls = threading.local()
    key = os.environ.get("OPENALEX_API_KEY")
    select = ("id,doi,type,publication_year,referenced_works_count,referenced_works,"
              "primary_topic")
    stats = {"todo": len(todo), "ok": 0, "not_found": 0, "failed": 0}

    def one(doi: str) -> dict[str, Any] | None:
        s = getattr(tls, "s", None)
        if s is None:
            s = tls.s = session_factory()
        pacer.wait()
        params = {"select": select, **({"api_key": key} if key else {})}
        r = _get(s, OPENALEX_WORK.format(doi=doi), params, timeout=60)
        if r is None:
            return None
        if r.status_code == 404:
            return openalex_row(doi, None, 404)
        if r.status_code != 200:
            return None
        try:
            return openalex_row(doi, r.json(), 200)
        except ValueError:
            return None

    with JsonlWriter(cache) as w, ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {pool.submit(one, d): d for d in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            row = fut.result()
            if row is None:
                stats["failed"] += 1
                continue
            w.write(row)
            stats["ok" if row["status"] == 200 else "not_found"] += 1
            if i % 2000 == 0:
                logger.info("openalex: %d/%d looked up", i, len(todo))
    logger.info("openalex: %s", stats)
    return {"cache": str(cache), **stats}


def load_openalex(path: str | Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if Path(path).is_file():
        for row in read_jsonl(path):
            out[str(row["doi"])] = row
    return out
