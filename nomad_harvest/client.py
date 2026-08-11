"""Polite, retrying HTTP client for the NOMAD v1 REST API (reads only, anonymous).

API facts baked in (live-verified 2026-08-05, NOMAD 1.4.3.post1):

* Base ``https://nomad-lab.eu/prod/v1/api/v1``; anonymous reads work, so no token —
  pass ``owner="public"`` for published, non-embargoed scope.
* Search: ``POST /entries/query`` with ``query`` / ``pagination`` / ``required`` /
  ``aggregations``. VASP filter ``results.method.simulation.program_name = "VASP"``
  (14,748,789 hits); DFT-only ``results.method.method_name = "DFT"``.
* **Keyset pagination only.** Offset (``page``/``page_offset``) is hard-capped at the
  10,000th result (HTTP 422); ``page_size`` max 10,000. Scroll the FULL set by feeding
  each page's ``next_page_after_value`` into the next ``page_after_value``.
* Raw files: ``GET /entries/{id}/rawdir`` (list) and ``GET /entries/{id}/raw/{path}``
  (one file, with ``decompress`` / ``offset`` / ``length`` params).
* Rate limit floor ~30 req/s or ~10 concurrent, **no rate-limit headers**, and
  **HTTP 503 = throttled** (502/504/500 also appear under load — the bulk endpoints are
  genuinely flaky). So we self-throttle (``min_interval``) and exponential-backoff on
  :data:`_RETRY_STATUS`.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

BASE = "https://nomad-lab.eu/prod/v1/api/v1"

# The three bulk-ingest external databases among VASP entries (verified: the ONLY
# external_db values, summing with direct uploads to the VASP total). Excluded so the
# harvest keeps only direct research uploads.
EXTERNAL_DBS = ["AFLOW", "OQMD", "Materials Project"]

# The `required` directive for a discover scan. NB: ``license`` is a DERIVED field (the
# same "not a doc quantity" case as aggregations) — it **422s** inside ``required.include``
# and an include restriction drops it entirely. So instead of an include allow-list we
# EXCLUDE only the one genuinely bulky field, ``quantities`` (the full metainfo-path list,
# ~15 KB/entry), which keeps ``license``, ``references``, ``datasets``, ``external_db``,
# ``results``, ``authors``, ``mainfile``, … (~24 KB/entry vs ~39 KB default). The keep-list
# is then slimmed to just the used fields by ``harvest.slim_candidate``, so the API
# response size does not dictate the manifest size.
CANDIDATE_REQUIRED = {"exclude": ["quantities"]}

# Retryable HTTP statuses. 503 is NOMAD's "you hit the rate limit"; 502/504 appear under
# load on the heavier endpoints; 500 is an occasional transient. No Retry-After header is
# sent, so we back off on a fixed exponential schedule.
_RETRY_STATUS = {429, 500, 502, 503, 504}


def direct_upload_vasp_query(elements: list[str] | None = None,
                             extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """The keep-query: direct-upload (non-external-db) VASP DFT entries.

    ``elements`` restricts to materials containing ALL of the given elements (a cheap way
    to scope a first harvest); ``extra`` AND-conjoins any further clause. The
    ``not external_db:any`` clause excludes AFLOW/OQMD/Materials Project.
    """
    clauses: list[dict[str, Any]] = [
        {"results.method.simulation.program_name": "VASP"},
        {"results.method.method_name": "DFT"},
        {"not": {"external_db:any": EXTERNAL_DBS}},
    ]
    if elements:
        clauses.append({"results.material.elements:all": list(elements)})
    if extra:
        clauses.append(extra)
    return {"and": clauses}


def _backoff_seconds(attempt: int, retry_after: str | None) -> float:
    """Exponential backoff (2,4,8,… capped 60 s), honouring a Retry-After if present."""
    if retry_after:
        try:
            return min(float(retry_after) + 1, 120)
        except ValueError:
            pass
    return min(2 ** (attempt + 1), 60)


class NomadClient:
    """Wrapper over the NOMAD v1 API. ``min_interval`` self-throttles under the ~30 req/s
    floor; every call retries :data:`_RETRY_STATUS` + transient network errors with
    exponential backoff."""

    def __init__(self, base: str = BASE, min_interval: float = 0.15,
                 timeout: float = 120, max_retries: int = 6,
                 session: requests.Session | None = None) -> None:
        self.base = base.rstrip("/")
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "materials-mlip-nomad/0.1")
        self._last = 0.0

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base}{path}"
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                r = self.session.post(url, json=body, timeout=self.timeout)
            except requests.RequestException as exc:
                self._last = time.monotonic()
                if attempt >= self.max_retries - 1:
                    raise
                logger.warning("NOMAD %s network error %s; retry %d", path,
                               type(exc).__name__, attempt + 1)
                time.sleep(_backoff_seconds(attempt, None))
                continue
            self._last = time.monotonic()
            if r.status_code in _RETRY_STATUS and attempt < self.max_retries - 1:
                wait = _backoff_seconds(attempt, r.headers.get("Retry-After"))
                logger.warning("NOMAD %s -> HTTP %d; backoff %.0fs (%d/%d)", path,
                               r.status_code, wait, attempt + 1, self.max_retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"NOMAD {path}: exhausted {self.max_retries} retries")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        """GET with the same retry/backoff policy; returns the streamed Response (caller
        must consume/close it — used for both JSON and file bytes)."""
        url = f"{self.base}{path}"
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                r = self.session.get(url, params=params, stream=True, timeout=self.timeout)
            except requests.RequestException:
                self._last = time.monotonic()
                if attempt >= self.max_retries - 1:
                    raise
                time.sleep(_backoff_seconds(attempt, None))
                continue
            self._last = time.monotonic()
            if r.status_code in _RETRY_STATUS and attempt < self.max_retries - 1:
                r.close()
                time.sleep(_backoff_seconds(attempt, r.headers.get("Retry-After")))
                continue
            r.raise_for_status()
            return r
        raise RuntimeError(f"NOMAD {path}: exhausted {self.max_retries} retries")

    # -- search ------------------------------------------------------------

    def count(self, query: dict[str, Any]) -> int:
        """Total entries matching ``query`` (cheap; page_size 0)."""
        d = self._post("/entries/query",
                       {"owner": "public", "query": query, "pagination": {"page_size": 0}})
        return int(d["pagination"]["total"])

    def query_page(self, query: dict[str, Any], required: dict[str, Any] | None = None,
                   page_size: int = 1000, page_after_value: str | None = None,
                   order_by: str = "entry_id") -> dict[str, Any]:
        pag: dict[str, Any] = {"page_size": page_size, "order_by": order_by, "order": "asc"}
        if page_after_value is not None:
            pag["page_after_value"] = page_after_value
        body: dict[str, Any] = {"owner": "public", "query": query, "pagination": pag}
        if required:
            body["required"] = required   # {"include":[…]} or {"exclude":[…]}
        return self._post("/entries/query", body)

    def iter_entries(self, query: dict[str, Any], required: dict[str, Any] | None = None,
                     page_size: int = 1000, max_entries: int | None = None
                     ) -> Iterator[dict[str, Any]]:
        """Keyset-paginate the FULL result set (no 10k offset ceiling).

        Feeds each page's ``next_page_after_value`` into the next request; stops when it
        is absent/empty or a page is empty. Ordered by ``entry_id`` (unique) for a stable
        scroll that never skips or repeats across pages.
        """
        after: str | None = None
        yielded = 0
        while True:
            page = self.query_page(query, required=required, page_size=page_size,
                                   page_after_value=after)
            hits = page.get("data", [])
            for rec in hits:
                yield rec
                yielded += 1
                if max_entries is not None and yielded >= max_entries:
                    return
            after = page.get("pagination", {}).get("next_page_after_value")
            if not after or not hits:
                return

    # -- raw files ---------------------------------------------------------

    def rawdir(self, entry_id: str) -> dict[str, Any]:
        """List an entry's raw files: ``{"mainfile", "files": [{"path","size"}, …]}``."""
        with self._get(f"/entries/{quote(entry_id, safe='')}/rawdir") as r:
            return r.json().get("data", {})

    def download_raw_file(self, entry_id: str, path: str, dest: Path,
                          decompress: bool = False,
                          on_chunk: "Callable[[int], None] | None" = None) -> int:
        """Stream one raw file (``GET /entries/{id}/raw/{path}``) to ``dest``.

        Returns bytes written. ``path`` is relative to the mainfile's directory (its
        internal ``/`` is preserved). ``decompress`` asks NOMAD to inflate ``.gz``/``.xz``
        server-side; default False keeps the compression extension so pymatgen's ``zopen``
        handles it locally (also covers ``.bz2``, which server-side decompress does not).
        Writes via a ``.part`` then renames, so an interrupted transfer never leaves a
        half-file under the final name.

        ``on_chunk(n_bytes)`` — if given — is invoked with each chunk's size *before* it is
        written; it may raise to abort the download (the disk-budget valve uses this to
        charge bytes as they land and stop cleanly at a limit). Its exception propagates
        after the partial ``.part`` is removed, so a budget-aborted transfer never leaves a
        stray file behind. Kept deliberately generic so the client has no budget dependency.
        """
        rel = f"/entries/{quote(entry_id, safe='')}/raw/{quote(path, safe='/')}"
        params = {"decompress": "true"} if decompress else None
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            with self._get(rel, params=params) as r:
                written = 0
                with tmp.open("wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        if on_chunk is not None:
                            on_chunk(len(chunk))
                        fh.write(chunk)
                        written += len(chunk)
            tmp.replace(dest)
            return written
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
