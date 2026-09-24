"""AiiDA archive (``*.aiida``) reading for VASP outputs — both on-disk formats, pure stdlib.

aiida-core has written two archive formats, both met in the wild (Materials Cloud ships hundreds):

* **legacy** (aiida-core < 2.0, export_version 0.x) — a zip (or, from ``verdi export -F tar.gz``,
  a tarball) whose members keep their REAL names under
  ``nodes/<uuid[:2]>/<uuid[2:4]>/<uuid[4:]>/{path,raw_input}/…``, so the ordinary zip/tar
  extractors reach the VASP files directly.
* **sqlite_zip** (aiida-core >= 2.0) — a zip of content-addressed blobs ``repo/<sha256>`` plus a
  ``db.sqlite3``. A node's files exist only as its ``db_dbnode.repository_metadata`` JSON tree,
  ``{"o": {"vasprun.xml": {"k": "<blob key>"}, "sub": {"o": {…}}}}`` (a file is ``{"k": key}``, a
  directory ``{"o": {…}}``), so reading one means querying that database.

:func:`vasp_nodes` returns every node of a sqlite_zip database whose repository holds a VASP
*primary* (for aiida-vasp: the ``retrieved`` FolderData of a CalcJob) with its whole file tree;
the fetch extractor writes those files out under the LEGACY layout (:func:`legacy_node_dir`), so a
calc from either format gets the same calc-unit grouping, calc_id shape (the node UUID path) and
per-calc availability. What counts as a primary is the CALLER's rule (the shared fetch passes its
own ``_unit_role``), so this module never disagrees with what fetch would parse.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

_LEGACY_NODE_RE = re.compile(r"(?:^|/)nodes/[0-9a-f]{2}/[0-9a-f]{2}/[^/]+/(?:path|raw_input)/")
_SQLITE_REPO_RE = re.compile(r"(?:^|/)repo/[0-9a-f]{32,}$")
DB_NAME = "db.sqlite3"
# SQL prefilter on the repository JSON (case-insensitive LIKE): a node can only hold a VASP
# primary if one of these stems appears in its file tree; the JSON is then parsed and every name
# checked with the caller's exact rule.
_PRIMARY_STEMS = ("vasprun", "outcar", "vaspout")


def archive_format(names: Iterable[str]) -> str | None:
    """``"legacy"`` (real names under ``nodes/<shards>/<uuid>/path/``), ``"sqlite_zip"``
    (``repo/<sha256>`` blobs + ``db.sqlite3``), or None. Tolerates a top-level folder around the
    export (``export/nodes/…``)."""
    legacy = sqlite = False
    for n in names:
        base = n.rsplit("/", 1)[-1]
        if base == "data.json" or _LEGACY_NODE_RE.search(n):
            legacy = True
        elif base == DB_NAME or _SQLITE_REPO_RE.search(n):
            sqlite = True
    if legacy:
        return "legacy"
    if sqlite:
        return "sqlite_zip"
    return None


def db_member(names: Iterable[str]) -> str | None:
    """The member name of a sqlite_zip archive's database (the shallowest ``db.sqlite3``)."""
    hits = [n for n in names if n.rsplit("/", 1)[-1] == DB_NAME]
    return min(hits, key=lambda n: (n.count("/"), n)) if hits else None


def repo_prefix(db_name: str) -> str:
    """The folder the archive's ``repo/`` blobs sit under: that of its ``db.sqlite3``."""
    return db_name[: -len(DB_NAME)]


def iter_repository_files(meta: Any, prefix: str = "") -> Iterator[tuple[str, str]]:
    """``(relative path, blob key)`` for every FILE in a node's ``repository_metadata`` tree."""
    if not isinstance(meta, dict):
        return
    objs = meta.get("o")
    if not isinstance(objs, dict):
        return
    for name, entry in objs.items():
        if not isinstance(entry, dict) or not isinstance(name, str) or not name:
            continue
        rel = f"{prefix}{name}"
        key = entry.get("k")
        if isinstance(key, str) and key:
            yield rel, key
        elif isinstance(entry.get("o"), dict):
            yield from iter_repository_files(entry, rel + "/")


def legacy_node_dir(uuid: str) -> str:
    """Where a legacy export keeps a node's files: ``nodes/<uu>/<id>/<rest-of-uuid>/path``."""
    u = uuid.lower()
    return f"nodes/{u[:2]}/{u[2:4]}/{u[4:]}/path"


def vasp_nodes(db_path: str | Path, is_primary: Callable[[str], bool]) -> list[dict[str, Any]]:
    """Every node whose repository holds a VASP primary, with its full file tree.

    Returns ``[{"uuid", "node_type", "files": [(relpath, blob_key), …]}]`` sorted by uuid (a
    deterministic extraction order). ``is_primary(basename)`` decides what counts. Raises
    ``ValueError`` if the file is not a readable AiiDA sqlite database (callers treat that like any
    corrupt archive)."""
    clause = " OR ".join("repository_metadata LIKE ?" for _ in _PRIMARY_STEMS)
    params = [f"%{s}%" for s in _PRIMARY_STEMS]
    try:
        # read-only + immutable: no locking at all, so it works on filesystems without POSIX
        # locks (Lustre scratch) — the archive database is a static file
        con = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    except sqlite3.Error as exc:
        raise ValueError(f"cannot open AiiDA database: {exc}") from exc
    out: list[dict[str, Any]] = []
    try:
        rows = con.execute(f"SELECT uuid, node_type, repository_metadata FROM db_dbnode "
                           f"WHERE {clause}", params)
        for uuid, node_type, raw in rows:
            try:
                meta = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            except (TypeError, ValueError):
                continue
            files = sorted(iter_repository_files(meta))
            if uuid and any(is_primary(rel.rsplit("/", 1)[-1]) for rel, _ in files):
                out.append({"uuid": str(uuid), "node_type": node_type, "files": files})
    except sqlite3.Error as exc:
        raise ValueError(f"not an AiiDA sqlite database: {exc}") from exc
    finally:
        con.close()
    return sorted(out, key=lambda n: n["uuid"])
