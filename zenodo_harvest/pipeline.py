"""Overlapped harvest orchestration: run ``fetch`` (network-bound) for part *i+1* at
the same time as ``parse``+``purge-raw`` (CPU/disk-bound) for part *i*, so the network
is not idle during parsing and vice-versa.

The core :func:`run_pipeline` is deliberately I/O-agnostic — it takes plain callables,
so it is unit-testable without the network or pymatgen. The CLI (`cli.py pipeline`)
supplies the real callables: ``fetch_fn`` = stage-2 fetch of one part into that part's
own ``fetched.part-N.jsonl`` (never a shared file, so a background parse always reads a
*complete* manifest), ``process_fn`` = ``parse`` that part into the shared dataset dir
(parses serialize on the dataset's ``.parse.lock`` — ``after_workers=1`` keeps them
one-at-a-time) then ``purge-raw`` that part's staged archives.

Disk safety: while ``process_fn(i)`` runs, part *i*'s staged files are still on disk
(until its purge) *and* ``fetch_fn(i+1)`` is staging part *i+1* — so at most **two**
parts' staging coexist. Size ``fetch``'s ``--max-disk-bytes`` to ~0.4 * quota so the two
together stay under the quota.
"""

from __future__ import annotations

import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable

logger = logging.getLogger(__name__)


def run_pipeline(
    parts: list[Any],
    fetch_fn: Callable[[Any], Any],
    process_fn: Callable[[Any], Any],
    after_workers: int = 1,
) -> tuple[list[Any], list[tuple[Any, Exception]]]:
    """Fetch each part in the foreground; process it in the background, overlapping the
    processing of part *i* with the fetch of part *i+1*.

    ``after_workers`` bounds how many ``process_fn`` calls may run at once (default 1:
    one background parse+purge overlapping the current fetch — the safe choice when all
    parts parse into one dataset dir guarded by a single lock). Returns
    ``(done_parts, errors)`` where ``errors`` is ``[(part, exception), …]`` for any
    ``process_fn`` that raised (a fetch that raises propagates immediately — it is the
    foreground step and a failed download batch should stop the run, not be swallowed).
    """
    done: list[Any] = []
    errors: list[tuple[Any, Exception]] = []
    inflight: dict[Any, Any] = {}

    def _collect(fut: Any) -> None:
        part = inflight.pop(fut)
        try:
            fut.result()
            done.append(part)
        except Exception as exc:  # a background parse/purge failed; record, keep going
            logger.exception("pipeline processing failed for part %s", part)
            errors.append((part, exc))

    with ThreadPoolExecutor(max_workers=after_workers) as pool:
        for part in parts:
            fetch_fn(part)                              # foreground: download this part
            inflight[pool.submit(process_fn, part)] = part  # background: parse+purge it
            # Keep at most `after_workers` processing jobs outstanding so staging from
            # already-processed parts is reclaimed before we run too far ahead.
            if len(inflight) > after_workers:
                finished, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
                for f in finished:
                    _collect(f)
        for f in wait(list(inflight))[0]:               # drain the tail
            _collect(f)
    return done, errors
