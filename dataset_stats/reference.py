"""Reference MLIP datasets, measured with the same code as the harvested corpus.

* ``mptrj``          — Matbench Discovery's extxyz zip of MPtrj (1.58M frames, CHGNet/MACE-MP-0
                       training set), one member per MP material.
* ``omat24_val``     — OMat24's 1.03M-frame validation split (a random sample of its 101M train
                       set; same 11 sub-datasets), ``.aselmdb``.
* ``salex_val``      — sAlex's 0.55M-frame validation split (OMat24's subsample of the Alexandria
                       relaxation trajectories), ``.aselmdb``.
* ``mp_materials``   — MP relaxed structures as of 2023-02-07 (Matbench Discovery), the material
                       universe MPtrj was drawn from.
* ``alexandria_pbe`` — Alexandria PBE 3D materials (release 2025.07.02), the material universe of
                       sAlex and OMat24 (whose structures start from Alexandria ones).

``fetch_reference`` downloads into ``<refs>/<name>/`` (HTTP Range resume, md5 where published);
``scan_reference`` writes the same per-chunk ``.npz`` the shard scan writes (frames, calc records
with a ``g`` group = material id, aggregates), so the report treats a reference like a source.
Reading ``.aselmdb`` needs ``ase-db-backends`` (``pip install ase-db-backends``; no torch).
"""

from __future__ import annotations

import bz2
import gzip
import hashlib
import json
import logging
import math
import re
import tarfile
import time
import zipfile
from collections import Counter
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from . import structure
from .common import EV_A3_TO_GPA, FORCE_EDGES, atomic_write_npz, calc_key, jsonl_text, text_array
from .scan import (
    F_E_NONFINITE,
    F_F_NONFINITE,
    F_FORCES,
    F_NO_CELL,
    F_S_NONFINITE,
    F_STRESS,
    FRAME_DTYPE,
    MPTRJ_KEYS,
    _hash8,
    scan_text,
)

logger = logging.getLogger(__name__)

_FB = "https://dl.fbaipublicfiles.com/opencatalystproject/data/omat"
_OMAT_SUBSETS = ("rattled-1000", "rattled-1000-subsampled", "rattled-500",
                 "rattled-500-subsampled", "rattled-300", "rattled-300-subsampled",
                 "aimd-from-PBE-1000-npt", "aimd-from-PBE-1000-nvt", "aimd-from-PBE-3000-npt",
                 "aimd-from-PBE-3000-nvt", "rattled-relax")
ALEXANDRIA_PBE = "https://alexandria.icams.rub.de/data/pbe/2025.07.02/"

REFERENCES: dict[str, dict[str, Any]] = {
    "mptrj": {"kind": "extxyz_zip",
              "files": [("2024-09-03-mp-trj.extxyz.zip",
                         "https://api.figshare.com/v2/file/download/49034296",
                         "7f433171e4e5f2ef9304dccd42d5488f")],
              "label": "MPtrj (Matbench Discovery extxyz, 2024-09-03)"},
    "omat24_val": {"kind": "aselmdb",
                   "files": [(f"{s}.tar.gz", f"{_FB}/241220/omat/val/{s}.tar.gz", None)
                             for s in _OMAT_SUBSETS],
                   "label": "OMat24 validation split (1.03M frames)"},
    "salex_val": {"kind": "aselmdb",
                  "files": [("val.tar.gz", f"{_FB}/241018/sAlex/val.tar.gz", None)],
                  "label": "sAlex validation split (0.55M frames)"},
    "mp_materials": {"kind": "entries_json",
                     "files": [("2023-02-07-mp-computed-structure-entries.json.gz",
                                "https://api.figshare.com/v2/file/download/40344436",
                                "76fc748db6b175bb80de4c276d27c235")],
                     "label": "Materials Project relaxed structures (2023-02-07)"},
    "alexandria_pbe": {"kind": "entries_json", "files": "alexandria",
                       "label": "Alexandria PBE 3D (2025.07.02)"},
    # not scanned: the per-element reference energies for the formation-energy proxy (the same
    # file Matbench Discovery uses for MPtrj's formation energies)
    "mp_elemental_refs": {"kind": "elemental_refs",
                          "files": [("2023-02-07-mp-elemental-reference-entries.json.gz",
                                     "https://api.figshare.com/v2/file/download/40387775",
                                     "6e93b6f38d6e27d6c811d3cafb23a070")],
                          "label": "MP elemental reference entries (2023-02-07)"},
}


def _entries_anywhere(obj: Any) -> Iterator[dict]:
    """Every dict carrying ``composition`` + ``energy`` (a ComputedEntry), however nested."""
    if isinstance(obj, dict):
        if "composition" in obj and "energy" in obj:
            yield obj
            return
        for v in obj.values():
            yield from _entries_anywhere(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _entries_anywhere(v)


def load_elemental_refs(refs_root: str | Path) -> dict[str, float]:
    """``{element: eV/atom}`` — the lowest energy per atom of each elemental entry in MP's
    reference file (uncorrected GGA, as MPtrj's ``energy``); empty if not downloaded."""
    d = Path(refs_root) / "mp_elemental_refs"
    files = sorted(d.glob("*.json.gz")) if d.is_dir() else []
    mu: dict[str, float] = {}
    for f in files:
        with gzip.open(f, "rt") as fh:
            obj = json.load(fh)
        for e in _entries_anywhere(obj):
            comp = e["composition"]
            if isinstance(comp, dict) and len(comp) == 1:
                (el, amt), = comp.items()
                try:
                    v = float(e["energy"]) / float(amt)
                except (TypeError, ValueError, ZeroDivisionError):
                    continue
                el = structure.element_of(str(el))
                if el not in mu or v < mu[el]:
                    mu[el] = v
    return mu


# --- download ----------------------------------------------------------------------------------
def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, dest: Path, md5: str | None, session: Any, tries: int = 6) -> None:
    """Resumable download (HTTP Range) to ``dest``; verifies md5 when one is published."""
    if dest.is_file() and (md5 is None or _md5(dest) == md5):
        logger.info("have %s", dest.name)
        return
    part = dest.with_name(dest.name + ".part")
    for attempt in range(tries):
        have = part.stat().st_size if part.is_file() else 0
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            with session.get(url, headers=headers, stream=True, timeout=120,
                             allow_redirects=True) as r:
                if r.status_code == 416:  # already complete
                    break
                r.raise_for_status()
                mode = "ab" if have and r.status_code == 206 else "wb"
                with open(part, mode) as fh:
                    for chunk in r.iter_content(1 << 22):
                        fh.write(chunk)
            break
        except Exception as exc:  # noqa: BLE001 - retry transient network errors
            logger.warning("download %s failed (%s), retry %d", url, exc, attempt + 1)
            time.sleep(min(60, 5 * 2 ** attempt))
    else:
        raise RuntimeError(f"could not download {url}")
    if md5 is not None and _md5(part) != md5:
        part.unlink()
        raise RuntimeError(f"md5 mismatch for {dest.name}; deleted, re-run to retry")
    part.replace(dest)


def _alexandria_files(session: Any) -> list[tuple[str, str, None]]:
    html = session.get(ALEXANDRIA_PBE, timeout=60).text
    names = sorted(set(re.findall(r'href="(alexandria_\d+\.json\.bz2)"', html)))
    return [(n, ALEXANDRIA_PBE + n, None) for n in names]


def fetch_reference(name: str, refs_root: str | Path) -> dict:
    import requests
    spec = REFERENCES[name]
    out = Path(refs_root) / name
    out.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    s.headers["User-Agent"] = "dataset-stats/1.0 (research; python-requests)"
    files = _alexandria_files(s) if spec["files"] == "alexandria" else spec["files"]
    for fname, url, md5 in files:
        _download(url, out / fname, md5, s)
        if fname.endswith(".tar.gz") and spec["kind"] == "aselmdb":
            marker = out / (fname + ".extracted")
            if not marker.is_file():
                with tarfile.open(out / fname) as tf:
                    try:
                        tf.extractall(out / "extracted", filter="data")
                    except TypeError:  # Python < 3.11.4 has no extraction filters
                        tf.extractall(out / "extracted")  # noqa: S202 - trusted publisher
                marker.write_text("ok\n")
    return {"name": name, "dir": str(out), "files": len(files),
            "bytes": sum(p.stat().st_size for p in out.glob("*") if p.is_file())}


# --- scanning ----------------------------------------------------------------------------------
def _row_from_atoms(numbers: np.ndarray, positions: np.ndarray, cell: np.ndarray,
                    energy: float, forces: np.ndarray | None, stress: np.ndarray | None,
                    calc: int, step: int, symbols: list[str]) -> tuple[tuple, np.ndarray]:
    """One FRAME_DTYPE tuple (same fields as the shard scan) + the per-atom |F| (for the hist)."""
    nat = len(numbers)
    flags = 0
    fmax = fmean = frms = fnet = math.nan
    fn = np.zeros(0)
    if forces is not None and len(forces) == nat:
        flags |= F_FORCES
        fn = np.sqrt(np.einsum("ij,ij->i", forces, forces))
        ok = np.isfinite(fn)
        if not ok.all():
            flags |= F_F_NONFINITE
        if ok.any():
            f = fn[ok]
            fmax, fmean, frms = float(f.max()), float(f.mean()), float(np.sqrt((f * f).mean()))
            fnet = float(np.linalg.norm(forces[ok].sum(axis=0)))
        fn = fn[ok]
    pressure = smax = math.nan
    if stress is not None:
        st = np.asarray(stress, dtype=float).ravel()
        if st.size == 9:
            st = st[[0, 4, 8, 5, 2, 1]]
        if st.size == 6:
            flags |= F_STRESS
            if np.isfinite(st).all():
                pressure = float(-(st[0] + st[1] + st[2]) / 3.0 * EV_A3_TO_GPA)
                smax = float(np.abs(st).max() * EV_A3_TO_GPA)
            else:
                flags |= F_S_NONFINITE
    if not math.isfinite(energy):
        flags |= F_E_NONFINITE
    geo = structure.cell_geometry(cell)
    vol = math.nan if geo is None else geo[0]
    if geo is None:
        flags |= F_NO_CELL
    sp = ",".join(symbols).encode()
    sh = _hash8(sp, np.rint(cell * 1e4).astype(np.int64).tobytes(),
                np.rint(np.nan_to_num(positions) * 1e4).astype(np.int64).tobytes())
    e_b = (int(round(energy * 1e6)).to_bytes(8, "little", signed=True)
           if math.isfinite(energy) else b"nan")
    fh = _hash8(sh.to_bytes(8, "little"), e_b)
    return ((calc, step, nat, energy, math.nan, fmax, fmean, frms, fnet, pressure, smax, vol,
             math.nan, math.nan, -1, math.nan, flags, sh, fh), fn)


def _write_chunk(out: Path, rows: list[tuple], calcs: list[dict], fhist: np.ndarray,
                 name: str, t0: float, extra: dict | None = None) -> dict:
    arr = np.array(rows, dtype=FRAME_DTYPE) if rows else np.zeros(0, dtype=FRAME_DTYPE)
    agg = {"shard": name, "frames": len(rows), "atoms": int(arr["natoms"].sum()) if rows else 0,
           "truncated": False, "torn_tail": False, "bad_frames": 0, "fallback_runs": 0,
           "force_nonfinite_atoms": 0, "force_hist": fhist.tolist(), "props": {},
           "info_keys": {}, "pbc": {}, "elapsed_s": round(time.time() - t0, 3), **(extra or {})}
    atomic_write_npz(out, frames=arr, calcs=text_array(jsonl_text(calcs)),
                     agg=text_array(json.dumps(agg)))
    return {"chunk": out.name, "frames": len(rows), "calcs": len(calcs)}


def _describe_records(per_calc: dict[str, list[tuple[int, Any]]], groups: dict[str, str],
                      dmin: bool = True) -> list[dict]:
    """Calc records from ``{calc_id: [(step, (cell, pos, symbols)), ...]}`` (first + last)."""
    out = []
    for cid, items in per_calc.items():
        items.sort(key=lambda t: t[0])
        s0, (c0, p0, y0) = items[0]
        d0 = structure.describe(c0, p0, y0, dmin=dmin)
        d0["step"] = s0
        rec: dict[str, Any] = {"k": str(calc_key(cid)), "id": cid, "n": len(items),
                               "first": d0, "last": None, "g": groups.get(cid, "")}
        if len(items) > 1:
            s1, (c1, p1, y1) = items[-1]
            d1 = structure.describe(c1, p1, y1, dmin=dmin)
            d1["step"] = s1
            rec["last"] = d1
        out.append(rec)
    return out


def _sid_split(sid: str) -> tuple[str, int]:
    head, _, tail = sid.rpartition("_")
    return (head, int(tail)) if head and tail.isdigit() else (sid, 0)


def _scan_aselmdb_task(args: tuple[str, int, int, str]) -> dict:
    path, start, stop, out = args
    import ase_db_backends  # noqa: F401 - registers the 'aselmdb' backend with ase.db
    from ase.db import connect
    t0 = time.time()
    db = connect(path, type="aselmdb", readonly=True)
    rows: list[tuple] = []
    fhist = np.zeros(len(FORCE_EDGES) - 1, dtype=np.int64)
    per_calc: dict[str, list[tuple[int, Any]]] = {}
    groups: dict[str, str] = {}
    for row in db.select(offset=start, limit=stop - start):
        atoms = row.toatoms()
        res = atoms.calc.results if atoms.calc is not None else {}
        data = row.data or {}
        sid = str(data.get("sid") or f"{Path(path).parent.name}:{row.id}")
        cid, step = _sid_split(sid)
        groups.setdefault(cid, str(data.get("parent_id") or data.get("mat_id") or cid))
        symbols = atoms.get_chemical_symbols()
        cell = np.array(atoms.cell)
        pos = atoms.positions
        f = res.get("forces")
        tup, fn = _row_from_atoms(atoms.numbers, pos, cell, float(res.get("energy", math.nan)),
                                  None if f is None else np.asarray(f, dtype=float),
                                  res.get("stress"), calc_key(cid), step, symbols)
        rows.append(tup)
        if len(fn):
            fhist += np.histogram(fn, FORCE_EDGES)[0]
        per_calc.setdefault(cid, []).append((step, (cell, pos, symbols)))
    calcs = _describe_records(per_calc, groups)
    return _write_chunk(Path(out), rows, calcs, fhist, Path(out).name, t0)


def _scan_zip_task(args: tuple[str, list[str], str]) -> dict:
    path, members, out = args
    t0 = time.time()
    with zipfile.ZipFile(path) as zf:
        text = "".join(zf.read(m).decode("utf-8", "replace").rstrip("\n") + "\n"
                       for m in members)
    rows, calcs, agg = scan_text(text, name=Path(out).name, keys=MPTRJ_KEYS)
    atomic_write_npz(Path(out), frames=rows, calcs=text_array(jsonl_text(calcs)),
                     agg=text_array(json.dumps(agg)))
    return {"chunk": Path(out).name, "frames": len(rows), "calcs": len(calcs),
            "elapsed_s": round(time.time() - t0, 1)}


def _open_any(path: Path) -> Any:
    if path.suffix == ".bz2":
        return bz2.open(path, "rt")
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def iter_entry_dicts(obj: Any) -> Iterator[dict]:
    """ComputedStructureEntry dicts from the layouts seen in the wild: ``{"entries": [...]}``
    (Alexandria), a list, or a pandas column dict ``{"entry": {idx: {...}}}`` (Matbench
    Discovery's MP entries)."""
    if isinstance(obj, list):
        yield from (e for e in obj if isinstance(e, dict))
    elif isinstance(obj, dict):
        if "entries" in obj:
            yield from iter_entry_dicts(obj["entries"])
        elif "entry" in obj and isinstance(obj["entry"], dict):
            ids = obj.get("material_id") or {}
            for k, e in obj["entry"].items():
                if isinstance(e, dict):
                    e = dict(e)
                    e.setdefault("entry_id", ids.get(k) if isinstance(ids, dict) else k)
                    yield e
        elif "structure" in obj:
            yield obj


def _entry_structure(e: dict) -> tuple[np.ndarray, np.ndarray, list[str]] | None:
    st = e.get("structure") or {}
    lat = (st.get("lattice") or {}).get("matrix")
    sites = st.get("sites") or []
    if lat is None or not sites:
        return None
    cell = np.array(lat, dtype=float)
    pos, symbols = [], []
    for s in sites:
        xyz = s.get("xyz")
        if xyz is None:
            xyz = list(np.array(s["abc"], dtype=float) @ cell)
        pos.append(xyz)
        species = s.get("species") or [{"element": s.get("label", "X")}]
        symbols.append(str(max(species, key=lambda x: x.get("occu", 1)).get("element")))
    return cell, np.array(pos, dtype=float), symbols


def _scan_entries_task(args: tuple[str, str]) -> dict:
    path, out = args
    t0 = time.time()
    with _open_any(Path(path)) as fh:
        obj = json.load(fh)
    rows: list[tuple] = []
    per_calc: dict[str, list[tuple[int, Any]]] = {}
    groups: dict[str, str] = {}
    n = 0
    for e in iter_entry_dicts(obj):
        s = _entry_structure(e)
        if s is None:
            continue
        cell, pos, symbols = s
        data = e.get("data") or {}
        mid = str(e.get("entry_id") or data.get("mat_id") or data.get("material_id")
                  or f"{Path(path).name}:{n}")
        n += 1
        energy = float(e.get("energy", math.nan))
        tup, _fn = _row_from_atoms(np.zeros(len(symbols)), pos, cell, energy, None, None,
                                   calc_key(mid), 0, symbols)
        rows.append(tup)
        per_calc[mid] = [(0, (cell, pos, symbols))]
        groups[mid] = mid
    del obj
    calcs = _describe_records(per_calc, groups, dmin=False)
    return _write_chunk(Path(out), rows, calcs, np.zeros(len(FORCE_EDGES) - 1, dtype=np.int64),
                        Path(out).name, t0)


def _aselmdb_count(db_path: Path) -> int:
    """Row count without leaving an LMDB environment open in the parent: LMDB refuses a second
    open of the same file in one process, and forked workers inherit the parent's registry."""
    meta = db_path.with_name("metadata.npz")
    if meta.is_file():
        with np.load(meta) as z:
            return int(len(z["natoms"]))
    import ase_db_backends  # noqa: F401 - registers the backend
    from ase.db import connect
    db = connect(str(db_path), type="aselmdb", readonly=True)
    try:
        return int(db.count())
    finally:
        db.close()


def _tasks(name: str, ref_dir: Path, out_dir: Path, rows_per_task: int,
           members_per_task: int) -> list[tuple[Any, Any]]:
    kind = REFERENCES[name]["kind"]
    tasks: list[tuple[Any, Any]] = []
    if kind == "aselmdb":
        for i, db_path in enumerate(sorted((ref_dir / "extracted").rglob("*.aselmdb"))):
            n = _aselmdb_count(db_path)
            for j, start in enumerate(range(0, n, rows_per_task)):
                out = out_dir / f"shard-{i:03d}{j:04d}.npz"
                tasks.append((_scan_aselmdb_task,
                              (str(db_path), start, min(n, start + rows_per_task), str(out))))
    elif kind == "extxyz_zip":
        zpath = next(ref_dir.glob("*.zip"))
        with zipfile.ZipFile(zpath) as zf:
            names = sorted(n for n in zf.namelist() if n.endswith(".extxyz"))
        for j, start in enumerate(range(0, len(names), members_per_task)):
            out = out_dir / f"shard-{j:05d}.npz"
            tasks.append((_scan_zip_task,
                          (str(zpath), names[start:start + members_per_task], str(out))))
    elif kind == "entries_json":
        files = sorted(p for p in ref_dir.iterdir()
                       if p.name.endswith((".json.bz2", ".json.gz", ".json")))
        for j, p in enumerate(files):
            tasks.append((_scan_entries_task, (str(p), str(out_dir / f"shard-{j:05d}.npz"))))
    return tasks


def _run(task: tuple[Any, Any]) -> dict:
    fn, args = task
    try:
        return {"ok": True, **fn(args)}
    except Exception as exc:  # noqa: BLE001
        logger.exception("reference task failed: %s", args)
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "args": str(args)[:300]}


def scan_reference(name: str, refs_root: str | Path, stats_root: str | Path, *,
                   workers: int = 1, rows_per_task: int = 50_000, members_per_task: int = 1_500,
                   force: bool = False) -> dict:
    """Scan a fetched reference into ``<stats_root>/ref_<name>/scan`` (resumable per chunk)."""
    if REFERENCES[name]["kind"] == "elemental_refs":  # fetch-only: read by the report itself
        return {"name": name, "label": REFERENCES[name]["label"], "tasks": 0, "failed": [],
                "note": "fetch only; nothing to scan"}
    ref_dir = Path(refs_root) / name
    out_dir = Path(stats_root) / f"ref_{name}" / "scan"
    out_dir.mkdir(parents=True, exist_ok=True)
    tasks = _tasks(name, ref_dir, out_dir, rows_per_task, members_per_task)
    todo = [t for t in tasks if force or not Path(t[1][-1]).is_file()]
    t0 = time.time()
    if workers <= 1:
        results = [_run(t) for t in todo]
    else:
        with get_context("fork").Pool(workers, maxtasksperchild=20) as pool:
            results = list(pool.imap_unordered(_run, todo, chunksize=1))
    summary = {"name": name, "label": REFERENCES[name]["label"], "tasks": len(tasks),
               "done_now": sum(r["ok"] for r in results),
               "failed": [r for r in results if not r["ok"]][:20],
               "frames_now": sum(r.get("frames", 0) for r in results),
               "elapsed_s": round(time.time() - t0, 1)}
    (out_dir / "scan_summary.json").write_text(json.dumps(summary, indent=1))
    logger.info("reference %s: %s", name, {k: v for k, v in summary.items() if k != "failed"})
    return summary


def reference_counts(scan_dir: str | Path) -> Counter:
    """Frames per group (material) of a scanned reference (from its calc records)."""
    from .common import array_text
    c: Counter = Counter()
    for p in sorted(Path(scan_dir).glob("shard-*.npz")):
        with np.load(p, allow_pickle=False) as z:
            for line in array_text(z["calcs"]).splitlines():
                if line:
                    rec = json.loads(line)
                    c[rec.get("g", "")] += int(rec.get("n", 0))
    return c
