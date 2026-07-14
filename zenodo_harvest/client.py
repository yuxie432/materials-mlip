"""Throttled, retrying client for the Zenodo REST API.

Design notes grounded in observed API behaviour (2026-07):

* ``q`` is a full-text query over *metadata* (title/description/keywords), **not**
  file contents. Searching ``vasprun`` returns 0 hits even though thousands of
  records contain a ``vasprun.xml`` inside an archive. Discovery therefore works
  on metadata keywords; file-level filtering happens in triage.
* The search response already embeds each record's file listing (``files[].key``
  and ``size``), so we can triage on filenames without downloading anything.
* Rate limit for anonymous callers is ~30 requests/minute (``X-RateLimit-*``
  headers, ``Retry-After`` on 429). A personal access token raises this; pass it
  via ``ZENODO_TOKEN``.
* The search window is capped: ``page * size`` must be <= 10000, else HTTP 400.
  To exhaust a query with more than 10k hits we recursively bisect the ``created``
  date range (:meth:`ZenodoClient.iter_records`).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
from typing import Any, Iterable, Iterator

import requests

logger = logging.getLogger(__name__)

# Zenodo rejects page*size > 10000 with HTTP 400.
MAX_SEARCH_WINDOW = 10_000
# Zenodo caps page size at 25 on /api/records (size>=50 -> HTTP 400). With size=25
# the 10k window is reachable in 400 pages.
DEFAULT_PAGE_SIZE = 25


class ZenodoClient:
    """Minimal, polite wrapper over ``GET /api/records``.

    Parameters
    ----------
    base:
        API host. Use ``https://sandbox.zenodo.org`` for testing writes; reads
        work against the production host by default.
    token:
        Personal access token. Falls back to the ``ZENODO_TOKEN`` env var. Only
        needed to raise rate limits / reach restricted records; public search
        works anonymously.
    min_interval:
        Minimum seconds between requests. Defaults to ~2.1 s (≈28/min, under the
        30/min cap). NB: the ``/api/records`` search endpoint is capped at 30/min
        *even with a token* (verified 2026-07-13) — a token raises limits on other
        endpoints and hourly quotas, and grants access to restricted records, but
        does not speed up search. 429s are retried via ``Retry-After`` regardless.
    """

    def __init__(
        self,
        base: str = "https://zenodo.org",
        token: str | None = None,
        min_interval: float | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.base = base.rstrip("/")
        self.token = token or os.environ.get("ZENODO_TOKEN")
        # Search is 30/min regardless of auth, so don't speed up when tokened.
        self.min_interval = 2.1 if min_interval is None else min_interval
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "zenodo-harvest/0.1 (materials-mlip)"})
        if self.token:
            self.session.headers["Authorization"] = f"Bearer {self.token}"
        self._last_request = 0.0

    # -- low-level ---------------------------------------------------------

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)

    def _get(self, path: str, params: dict[str, Any] | None = None, max_retries: int = 5) -> dict:
        url = f"{self.base}{path}"
        attempt = 0
        while True:
            self._throttle()
            resp = self.session.get(url, params=params, timeout=60)
            self._last_request = time.monotonic()
            if resp.status_code == 429:
                # Throttling is expected, not a failure: honour Retry-After and
                # retry without spending the 5xx error budget.
                wait = int(resp.headers.get("Retry-After", 5)) + 1
                logger.warning("rate limited; sleeping %ss", wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 500 and attempt < max_retries:
                wait = 2 ** attempt
                attempt += 1
                logger.warning("server %s; retry %d/%d in %ss", resp.status_code, attempt, max_retries, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()  # raises on persistent 5xx / any other 4xx
            return resp.json()

    # -- search primitives -------------------------------------------------

    def count(self, query: str, extra: dict[str, Any] | None = None) -> int:
        """Total number of records matching ``query`` (cheap; size=1)."""
        params = {"q": query, "size": 1, **(extra or {})}
        return int(self._get("/api/records", params)["hits"]["total"])

    def search_page(
        self,
        query: str,
        page: int = 1,
        size: int = DEFAULT_PAGE_SIZE,
        sort: str = "newest",
        extra: dict[str, Any] | None = None,
    ) -> dict:
        params = {"q": query, "page": page, "size": size, "sort": sort, **(extra or {})}
        return self._get("/api/records", params)

    def iter_window(
        self,
        query: str,
        size: int = DEFAULT_PAGE_SIZE,
        sort: str = "newest",
        extra: dict[str, Any] | None = None,
    ) -> Iterator[dict]:
        """Yield records for ``query`` up to the 10k search window.

        Warns (and stops) if the query has more than 10k hits — use
        :meth:`iter_records` to exhaust those. Overall harvest caps
        (``max_records``) are applied by the caller at the dedup level.
        """
        first = self.search_page(query, page=1, size=size, sort=sort, extra=extra)
        total = int(first["hits"]["total"])
        if total > MAX_SEARCH_WINDOW:
            logger.warning(
                "query %r has %d hits (> %d window); results will be truncated. "
                "Use iter_records() to exhaust it.",
                query, total, MAX_SEARCH_WINDOW,
            )
        yielded = 0
        page = 1
        hits = first["hits"]["hits"]
        while hits:
            for rec in hits:
                yield rec
                yielded += 1
                if yielded >= MAX_SEARCH_WINDOW:
                    return
            if page * size >= min(total, MAX_SEARCH_WINDOW):
                return
            page += 1
            hits = self.search_page(query, page=page, size=size, sort=sort, extra=extra)["hits"]["hits"]

    def iter_records(
        self,
        query: str,
        start: date = date(2013, 1, 1),
        end: date | None = None,
        size: int = DEFAULT_PAGE_SIZE,
        extra: dict[str, Any] | None = None,
    ) -> Iterator[dict]:
        """Exhaustively yield every record for ``query`` across all versions/dates.

        Zenodo caps any single query at 10k retrievable results. To get past that
        we recursively bisect the ``created`` date interval until every sub-window
        has <= 10k hits, then page through each. This is the scalable path for a
        full harvest (safe to run on the cluster; resume by partitioning dates).
        """
        end = end or date.today()
        yield from self._iter_range(query, start, end, size, extra)

    def _iter_range(
        self, query: str, start: date, end: date, size: int, extra: dict[str, Any] | None
    ) -> Iterator[dict]:
        ranged = f"({query}) AND created:[{start.isoformat()} TO {end.isoformat()}]"
        total = self.count(ranged, extra)
        if total == 0:
            return
        if total <= MAX_SEARCH_WINDOW or start >= end:
            if total > MAX_SEARCH_WINDOW:
                logger.warning(
                    "single-day window %s still has %d hits; truncating at %d",
                    start, total, MAX_SEARCH_WINDOW,
                )
            logger.info("harvest %s..%s : %d records", start, end, total)
            yield from self.iter_window(ranged, size=size, extra=extra)
            return
        mid = start + (end - start) / 2
        yield from self._iter_range(query, start, mid, size, extra)
        yield from self._iter_range(query, mid + timedelta(days=1), end, size, extra)

    # -- single record -----------------------------------------------------

    def get_record(self, recid: str | int) -> dict:
        return self._get(f"/api/records/{recid}")


def iter_many(
    client: ZenodoClient,
    queries: Iterable[str],
    extra: dict[str, Any] | None = None,
    exhaustive: bool = False,
) -> Iterator[dict]:
    """Run several queries and yield records, tagging each with the query that hit it."""
    for q in queries:
        logger.info("query: %s", q)
        stream = client.iter_records(q, extra=extra) if exhaustive else client.iter_window(q, extra=extra)
        for rec in stream:
            rec.setdefault("_query", q)
            yield rec
