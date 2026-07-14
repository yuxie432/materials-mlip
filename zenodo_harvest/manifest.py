"""JSONL manifest helpers + rejection logging.

Every pipeline stage reads one JSONL manifest and writes another, so the whole
harvest is resumable (re-run a stage, skip what's done) and auditable (nothing is
dropped silently — rejections are logged with a machine-readable reason, per
docs/DESIGN.md §5).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator


def read_jsonl(path: str | Path) -> Iterator[dict]:
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


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
