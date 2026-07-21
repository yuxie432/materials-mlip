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
  *datetime* range (:meth:`ZenodoClient.iter_records`) — down to sub-second if a
  single day is dense (a bulk upload day can exceed 10k; date-only bisection cannot
  split below a day and would silently truncate). Intervals are half-open
  ``[start TO end}`` so recursive splits are disjoint and gapless.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator

import requests

logger = logging.getLogger(__name__)

# Zenodo rejects page*size > 10000 with HTTP 400.
MAX_SEARCH_WINDOW = 10_000
# Zenodo caps page size at 25 on /api/records (size>=50 -> HTTP 400). With size=25
# the 10k window is reachable in 400 pages.
DEFAULT_PAGE_SIZE = 25
# Finest datetime-bisection granularity. If even a 1-second `created` window still
# has >10k hits we truncate (essentially impossible: >10k records ingested in the
# same second). Sub-second bisection would need `created` millisecond precision we
# don't rely on.
MIN_WINDOW = timedelta(seconds=1)


def _parse_retry_after(value: str | None, default: int = 5) -> int:
    """Seconds to wait from a ``Retry-After`` header value.

    RFC 7231 allows two forms: integer delta-seconds *or* an HTTP-date. Zenodo
    sends the delta form, but a stray HTTP-date (or garbage) must not crash the
    retry loop — so fall back to a small default rather than parsing dates (which
    we never need for this API).
    """
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


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
            try:
                resp = self.session.get(url, params=params, timeout=60)
            except requests.RequestException as exc:
                # Transient network failure (ConnectionError/ReadTimeout/…): retry
                # on the SAME budget as a 5xx (exponential backoff), then give up
                # and re-raise so the caller sees a real hard failure.
                self._last_request = time.monotonic()
                if attempt >= max_retries:
                    raise
                wait = 2 ** attempt
                attempt += 1
                logger.warning("request error %s; retry %d/%d in %ss",
                               type(exc).__name__, attempt, max_retries, wait)
                time.sleep(wait)
                continue
            self._last_request = time.monotonic()
            if resp.status_code == 429:
                # Throttling is expected, not a failure: honour Retry-After and
                # retry without spending the 5xx error budget.
                wait = _parse_retry_after(resp.headers.get("Retry-After")) + 1
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

    @staticmethod
    def _fmt(dt: datetime) -> str:
        """Format a datetime for a Zenodo ``created:`` range bound (no tz suffix)."""
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def iter_records(
        self,
        query: str,
        start: date = date(2013, 1, 1),
        end: date | None = None,
        size: int = DEFAULT_PAGE_SIZE,
        extra: dict[str, Any] | None = None,
        should_skip: Callable[[datetime, datetime], bool] | None = None,
        on_window_done: Callable[[datetime, datetime], None] | None = None,
    ) -> Iterator[dict]:
        """Exhaustively yield every record for ``query`` across all versions/dates.

        Zenodo caps any single query at 10k retrievable results. To get past that we
        recursively bisect the ``created`` **datetime** interval until every sub-window
        has <= 10k hits, then page through each. Bisection goes below one day when
        needed (down to :data:`MIN_WINDOW`), so a bulk-upload day with >10k records is
        split by hour/minute/second rather than silently truncated (the old date-only
        bisection could not split a single day). This is the scalable path for a full
        harvest (safe to run on the cluster; resume by partitioning dates).

        ``should_skip(start, end)`` and ``on_window_done(start, end)`` are optional
        checkpoint hooks invoked *only at leaf windows* (the ones actually paged), with
        timezone-aware ``datetime`` bounds: ``should_skip`` lets a resumed caller skip a
        window it already completed, and ``on_window_done`` is called once a leaf window
        has been fully yielded. NB: which windows are leaves depends on the live hit
        counts, so bisection leaves can shift between runs if counts changed — callers
        must match windows exactly (compare on the datetime isoformat).
        """
        start_dt = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
        end_date = end or date.today()
        # `end` is inclusive of the whole end-day; the recursion uses a half-open upper
        # bound, so set it to the next midnight (exclusive) to cover all of end-day.
        end_dt = (datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc)
                  + timedelta(days=1))
        yield from self._iter_range(query, start_dt, end_dt, size, extra,
                                    should_skip, on_window_done)

    def _iter_range(
        self, query: str, start: datetime, end: datetime, size: int,
        extra: dict[str, Any] | None,
        should_skip: Callable[[datetime, datetime], bool] | None = None,
        on_window_done: Callable[[datetime, datetime], None] | None = None,
    ) -> Iterator[dict]:
        # Half-open [start TO end}: inclusive start, EXCLUSIVE end. This makes the two
        # recursive halves [start,mid) and [mid,end) disjoint AND gapless — an inclusive
        # [A TO B] split would double-count any record created exactly at the midpoint.
        ranged = f"({query}) AND created:[{self._fmt(start)} TO {self._fmt(end)}}}"
        total = self.count(ranged, extra)
        if total == 0:
            return
        if total <= MAX_SEARCH_WINDOW or (end - start) <= MIN_WINDOW:  # leaf (paged directly)
            if should_skip is not None and should_skip(start, end):
                return  # resumed: this exact window was completed in a prior run
            if total > MAX_SEARCH_WINDOW:
                logger.warning(
                    "smallest window %s..%s still has %d hits (>%d); truncating — a single "
                    "%ds bucket exceeds the search window",
                    self._fmt(start), self._fmt(end), total, MAX_SEARCH_WINDOW,
                    int(MIN_WINDOW.total_seconds()),
                )
            logger.info("harvest %s..%s : %d records", self._fmt(start), self._fmt(end), total)
            yield from self.iter_window(ranged, size=size, extra=extra)
            if on_window_done is not None:
                on_window_done(start, end)
            return
        mid = start + (end - start) / 2
        yield from self._iter_range(query, start, mid, size, extra, should_skip, on_window_done)
        yield from self._iter_range(query, mid, end, size, extra, should_skip, on_window_done)

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
