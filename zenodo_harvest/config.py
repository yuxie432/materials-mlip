"""Configuration: .env loading and default filesystem layout.

Kept dependency-free (no python-dotenv) so it runs anywhere, including a bare
cluster login node. Paths are overridable via env vars / CLI so the same code
runs on WSL and on CSD3 scratch without edits.
"""

from __future__ import annotations

import os
from pathlib import Path


def _clean_value(val: str) -> str:
    """Unquote a value and strip a trailing ``# inline comment``.

    A quoted value is taken literally (any ``#`` inside is preserved); an
    unquoted value keeps a bare ``#`` (e.g. inside a token) but drops a comment
    introduced by whitespace + ``#`` — the usual dotenv convention.
    """
    val = val.strip()
    if val[:1] in ("'", '"'):
        end = val.find(val[0], 1)
        if end != -1:
            return val[1:end]
    for i, ch in enumerate(val):
        if ch == "#" and (i == 0 or val[i - 1].isspace()):
            return val[:i].strip()
    return val


def load_dotenv(path: str | Path = ".env") -> None:
    """Populate ``os.environ`` from a ``KEY=VALUE`` file (existing vars win).

    Minimal parser: ignores blanks and ``#`` comment lines, tolerates a leading
    ``export``, unquotes values, and drops inline comments (see ``_clean_value``).
    Never overwrites a variable already set in the real environment (so an
    ``export ZENODO_TOKEN=...`` on the cluster takes precedence over the file).
    """
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, _, val = line.partition("=")
        key = key.strip()
        if key:
            os.environ.setdefault(key, _clean_value(val))


# Filesystem layout. Override the root via ZENODO_HARVEST_DATA (e.g. point it at
# cluster scratch). Everything here is gitignored.
def _layout(root: str | None = None) -> tuple[Path, Path, Path, Path]:
    data_root = Path(root if root is not None
                     else os.environ.get("ZENODO_HARVEST_DATA", "data"))
    return (data_root, data_root / "manifests",  # JSONL pipeline manifests
            data_root / "raw",                    # downloaded + extracted VASP files (staging)
            data_root / "dataset")                # final extxyz.gz shards + metadata.jsonl


# Bound at import from the process environment. A ZENODO_HARVEST_DATA that arrives only
# via .env (loaded later, in the CLI) is picked up by refresh_paths() below.
DATA_ROOT, MANIFEST_DIR, RAW_DIR, DATASET_DIR = _layout()


def refresh_paths() -> None:
    """Recompute the data-dir layout from the current environment.

    The path constants are bound at import, but ``load_dotenv`` runs later (from the CLI),
    so a ``ZENODO_HARVEST_DATA`` living in ``.env`` would otherwise be ignored and the
    harvest would silently write under the default ``data/`` — on CSD3 that is ``/home``'s
    50 GB *backed-up* quota, not the 1 TB scratch, so a real harvest would fill it and fail.
    The CLI calls this once, right after ``load_dotenv`` and before argparse reads the
    path defaults, so ``.env`` and a real ``export`` behave identically. (Batch scripts
    still ``export`` it before Python starts, which works with or without this.)
    """
    global DATA_ROOT, MANIFEST_DIR, RAW_DIR, DATASET_DIR
    DATA_ROOT, MANIFEST_DIR, RAW_DIR, DATASET_DIR = _layout()


def ensure_dirs() -> None:
    for d in (MANIFEST_DIR, RAW_DIR, DATASET_DIR):
        d.mkdir(parents=True, exist_ok=True)
