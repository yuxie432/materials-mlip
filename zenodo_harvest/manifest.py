"""JSONL manifest helpers + rejection logging.

Every pipeline stage reads one JSONL manifest and writes another, so the whole
harvest is resumable (re-run a stage, skip what's done) and auditable (nothing is
dropped silently — rejections are logged with a machine-readable reason, per
docs/DESIGN.md §5).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def read_jsonl(path: str | Path) -> Iterator[dict]:
    """Yield one dict per JSONL line, tolerating a truncated *final* line.

    A crash / power loss / disk-full during an append (``flush`` is not ``fsync``)
    can leave the last line half-written. Since every stage reads its predecessor's
    manifest at the *start* of a resumed run, a hard ``json.loads`` there would
    abort the very resume the append-only design exists to enable. So a malformed
    **last** non-empty line is logged and skipped; a malformed non-final line still
    raises, since that signals genuine mid-file corruption rather than a torn tail.
    """
    prev: str | None = None
    with Path(path).open() as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            if prev is not None:
                yield json.loads(prev)  # prev was newline-terminated -> must be whole
            prev = line
    if prev is not None:
        try:
            yield json.loads(prev)
        except json.JSONDecodeError as exc:
            logger.warning("skipping truncated final line in %s: %s", path, exc)


def write_jsonl(path: str | Path, rows: list[dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


class JsonlWriter:
    """Append-only JSONL sink (flushes each line — crash-safe/resumable)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a")
        self.n = 0

    def write(self, row: dict[str, Any]) -> None:
        self._fh.write(json.dumps(row) + "\n")
        self._fh.flush()
        self.n += 1

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "JsonlWriter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class RejectionLogger:
    """Records dropped items with a reason so recall stays auditable."""

    def __init__(self, path: str | Path):
        self._w = JsonlWriter(path)

    def reject(self, stage: str, ident: str, reason: str, **detail: Any) -> None:
        self._w.write({"stage": stage, "id": ident, "reason": reason, **detail})

    @property
    def n(self) -> int:
        return self._w.n

    def close(self) -> None:
        self._w.close()

    def __enter__(self) -> "RejectionLogger":
        return self

    def __exit__(self, *exc: Any) -> None:
        # Context-managed so a raising parse/fetch still releases the log's file handle
        # (each line is already flushed, so no rejection is lost — this only stops an fd
        # leak on the error path, which matters in `pipeline` where a failed batch's
        # exception is caught and the process keeps running for the remaining batches).
        self.close()
