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
# cluster scratch). Everything here is gitignored. NB: this is read once, at
# import — set it as a real env var / in your batch script (before Python starts),
# not in .env, which is loaded after this module is imported.
DATA_ROOT = Path(os.environ.get("ZENODO_HARVEST_DATA", "data"))
MANIFEST_DIR = DATA_ROOT / "manifests"   # JSONL pipeline manifests
RAW_DIR = DATA_ROOT / "raw"              # downloaded + extracted VASP files (staging)
DATASET_DIR = DATA_ROOT / "dataset"      # final extxyz.gz shards + metadata.jsonl


def ensure_dirs() -> None:
    for d in (MANIFEST_DIR, RAW_DIR, DATASET_DIR):
        d.mkdir(parents=True, exist_ok=True)
