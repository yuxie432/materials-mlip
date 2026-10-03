"""Metadata pass: ``metadata.jsonl`` -> one compact row per calc (+ per-deposit provenance).

Streams the file in byte-range chunks (aligned to line starts) so N processes parse it at once —
NOMAD's is tens of GB — and never materialises the records (the ``verify``/``purge-raw`` OOM
lesson). Each chunk writes ``<out>/chunk-NNN.npz``:
  * ``calcs``    — :data:`CALC_DTYPE` rows; categorical columns hold codes into ``cats``;
  * ``cats``     — JSON ``{column: [value, ...]}`` (the chunk's own code tables; the report remaps);
  * ``deposits`` — JSON ``{deposit id: {title, doi, year, license, creators, ...}}``;
  * ``agg``      — JSON counters not kept per row (POTCAR symbols, U values, INCAR tags, ...).

A "deposit" is the unit a depositor published: the Zenodo / Materials Cloud concept record, the
NOMAD upload. The pass is cheap (minutes), so it is simply re-run rather than resumed.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from collections import Counter
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np

from .common import atomic_write_npz, calc_key, text_array
from .params import (
    as_float,
    as_int,
    calc_type,
    functional_label,
    hubbard_u,
    kpoint_summary,
    mp_compatible,
    omat24_compatible,
    potcar_elements,
    potcar_libraries,
    potcar_symbols,
    vdw_method,
    xc_family,
)

logger = logging.getLogger(__name__)

# Where a calc came from, read from its path. NOMAD's "direct uploads" include the Alexandria
# database's own relaxation runs, uploaded by its authors in 2024 (paths carry Alexandria ids:
# `xxx_02a-00_agm004014910_spg216/GEO1_vasprun.xml.bz2`) and the same group's 2017 cubic-perovskite
# high-throughput set (`perovskites/Li_LiF3Cs_xxx_02p-00_spg221b/`). Both are institutional
# high-throughput data already inside Alexandria / sAlex / OMat24, so the report separates them.
ORIGIN_LABELS = ("individual", "alexandria", "alexandria-group-ht")
_AGM_RE = re.compile(r"agm\d{6,}")
_HT_RE = re.compile(r"xxx_\d\d[a-z]?-\d\d")


def origin_of(rec: dict) -> int:
    prov = rec.get("provenance") or {}
    path = " ".join(str(prov.get(k) or "") for k in ("mainfile", "file_path")) + " " + \
        str(rec.get("calc_id") or "")
    if _AGM_RE.search(path):
        return 1
    if _HT_RE.search(path):
        return 2
    return 0


AVAIL_KEYS = ("charge_density", "wavefunction", "dos", "eigenvalues", "projected",
              "local_potential", "elf", "spin_density", "magnetization")

CAT_COLS = ("deposit", "parser", "run_type", "functional", "family", "label", "vdw", "pset",
            "pchemsys", "version", "prec", "lreal", "algo", "ctype", "kstyle", "mag_src",
            "charge_src", "license", "rtype")

CALC_DTYPE = np.dtype([
    ("calc", "u8"), ("n_frames", "i4"), ("n_atoms", "i4"), ("n_ionic", "i4"),
    # categorical codes (into the chunk's ``cats`` tables)
    ("deposit", "i4"), ("parser", "i4"), ("run_type", "i4"), ("functional", "i4"),
    ("family", "i4"), ("label", "i4"), ("vdw", "i4"), ("pset", "i4"), ("pchemsys", "i4"),
    ("version", "i4"), ("prec", "i4"), ("lreal", "i4"), ("algo", "i4"), ("ctype", "i4"),
    ("kstyle", "i4"), ("mag_src", "i4"), ("charge_src", "i4"), ("license", "i4"),
    ("rtype", "i4"),
    # flags / settings
    ("u", "i1"), ("mp", "i1"), ("omat", "i1"), ("potlib", "u1"), ("ispin", "i1"), ("ncl", "i1"),
    ("soc", "i1"), ("lasph", "i1"), ("ismear", "i2"), ("ibrion", "i2"), ("isif", "i2"),
    ("mdalgo", "i2"), ("ichg", "i2"), ("nsw", "i4"), ("kprod", "i4"), ("nkpts", "i4"),
    ("k1", "i2"), ("k2", "i2"), ("k3", "i2"),
    ("encut", "f4"), ("ediff", "f4"), ("ediffg", "f4"), ("sigma", "f4"), ("potim", "f4"),
    ("tebeg", "f4"), ("kspacing", "f4"),
    # quality
    ("econv", "i1"), ("iconv", "i1"), ("scf_dE", "f4"), ("dfree_max", "f4"), ("n_unconv", "i4"),
    ("n_forces", "i4"), ("n_stress", "i4"), ("n_dropped", "i4"),
    # electronic
    ("mag", "f4"), ("charge", "f4"),
    # availability bits (AVAIL_KEYS order) + provenance year + origin (ORIGIN_LABELS index)
    ("avail", "u2"), ("year", "i2"), ("origin", "i1"),
    # first / last shard index holding the calc's frames (a calc is written contiguously), so a
    # filtered scan can skip shards that hold none of the calcs it wants
    ("shard_lo", "i4"), ("shard_hi", "i4"),
])
_SHARD_IDX_RE = re.compile(r"(\d+)\.extxyz")


def shard_range(rec: dict) -> tuple[int, int]:
    idx = [int(m.group(1)) for s in rec.get("shards") or []
           if (m := _SHARD_IDX_RE.search(str(s)))]
    return (min(idx), max(idx)) if idx else (-1, -1)


class Cats:
    """Per-column value -> code tables (code order = first seen)."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, int]] = {c: {} for c in CAT_COLS}

    def code(self, col: str, value: Any) -> int:
        v = "null" if value is None else str(value)
        t = self.tables[col]
        c = t.get(v)
        if c is None:
            c = t[v] = len(t)
        return c

    def as_lists(self) -> dict[str, list[str]]:
        return {c: list(t) for c, t in self.tables.items()}


def _flag(v: Any) -> int:
    return -1 if v is None else int(bool(v))


def _f(v: Any) -> float:
    f = as_float(v)
    return math.nan if f is None else f


def _i(v: Any, default: int = -1) -> int:
    i = as_int(v)
    return default if i is None else i


def deposit_of(prov: dict) -> str:
    src = prov.get("source") or "?"
    if src == "nomad":
        return f"nomad:{prov.get('upload_id') or prov.get('record_id')}"
    return f"{src}:{prov.get('conceptrecid') or prov.get('record_id')}"


def _year(prov: dict) -> int:
    for key in ("publication_date", "upload_create_time", "created"):
        v = prov.get(key)
        if isinstance(v, str) and len(v) >= 4 and v[:4].isdigit():
            return int(v[:4])
    return -1


def _version(cp: dict) -> str:
    v = str(cp.get("code_version") or "")
    parts = v.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 and parts[0].isdigit() else (v or "null")


def _norm_prec(v: Any) -> str:
    s = re.sub(r"[^a-z]", "", str(v or "").lower())
    if not s:
        return "null"
    for full in ("accurate", "normal", "high", "low", "medium", "single"):
        if full.startswith(s) or s.startswith(full):
            return full
    return s


def calc_row(rec: dict, cats: Cats, agg: dict[str, Counter]) -> tuple:
    """One metadata record -> a CALC_DTYPE tuple (codes via ``cats``; extras into ``agg``)."""
    cp = rec.get("calc_parameters") or {}
    q = rec.get("quality") or {}
    el = rec.get("electronic") or {}
    av = rec.get("availability") or {}
    prov = rec.get("provenance") or {}
    n_frames = len(rec.get("frame_ids") or [])
    style, grid, ksp, nk = kpoint_summary(cp)
    kprod = None if grid is None else grid[0] * grid[1] * grid[2]
    u = hubbard_u(cp)
    for elem, val in u.items():
        agg["u_values"][f"{elem}:{val:g}"] += 1
    for sym in potcar_symbols(cp):
        agg["potcar_symbols"][sym] += 1
    incar = cp.get("incar") or {}
    for tag in incar:
        agg["incar_tags"][str(tag)] += 1
    pels = sorted(set(potcar_elements(cp)))
    avail = 0
    for i, key in enumerate(AVAIL_KEYS):
        if av.get(key):
            avail |= 1 << i
    tebeg = as_float((cp.get("incar") or {}).get("TEBEG"))
    return (
        calc_key(rec.get("calc_id", "")), n_frames, _i(q.get("n_atoms")),
        _i(q.get("n_ionic_steps")),
        cats.code("deposit", deposit_of(prov)), cats.code("parser", rec.get("parser")),
        cats.code("run_type", cp.get("run_type")), cats.code("functional", cp.get("functional")),
        cats.code("family", xc_family(cp)), cats.code("label", functional_label(cp)),
        cats.code("vdw", vdw_method(cp)), cats.code("pset", cp.get("potcar_set_hash")),
        cats.code("pchemsys", "-".join(pels) if pels else None),
        cats.code("version", _version(cp)),
        cats.code("prec", _norm_prec(cp.get("parameters", {}).get("PREC")
                                     if isinstance(cp.get("parameters"), dict) else None)),
        cats.code("lreal", str((cp.get("parameters") or {}).get("LREAL"))),
        cats.code("algo", str((cp.get("incar") or {}).get("ALGO", "default")).lower()),
        cats.code("ctype", calc_type(cp)), cats.code("kstyle", style),
        cats.code("mag_src", el.get("magnetization_source")),
        cats.code("charge_src", el.get("charge_source")),
        cats.code("license", prov.get("license")), cats.code("rtype", prov.get("resource_type")),
        1 if u else 0, mp_compatible(cp), omat24_compatible(cp), potcar_libraries(cp),
        _i((cp.get("parameters") or {}).get("ISPIN") or cp.get("ispin")),
        _flag((cp.get("parameters") or {}).get("LNONCOLLINEAR")),
        _flag((cp.get("parameters") or {}).get("LSORBIT")),
        _flag((cp.get("parameters") or {}).get("LASPH")),
        _i(cp.get("ismear") if cp.get("ismear") is not None
           else (cp.get("parameters") or {}).get("ISMEAR"), -99),
        _i((cp.get("parameters") or {}).get("IBRION"), -99),
        _i((cp.get("parameters") or {}).get("ISIF"), -99),
        _i(incar.get("MDALGO"), -1), _i(incar.get("ICHARG"), -99),
        _i((cp.get("parameters") or {}).get("NSW"), -1),
        -1 if kprod is None else kprod, -1 if nk is None else nk,
        *(grid if grid is not None else (-1, -1, -1)),
        _f(cp.get("encut") if cp.get("encut") is not None
           else (cp.get("parameters") or {}).get("ENCUT")),
        _f(cp.get("ediff") if cp.get("ediff") is not None
           else (cp.get("parameters") or {}).get("EDIFF")),
        _f((cp.get("parameters") or {}).get("EDIFFG")),
        _f(cp.get("sigma") if cp.get("sigma") is not None
           else (cp.get("parameters") or {}).get("SIGMA")),
        _f((cp.get("parameters") or {}).get("POTIM")),
        math.nan if tebeg is None else tebeg, math.nan if ksp is None else ksp,
        _flag(q.get("electronic_converged")), _flag(q.get("ionic_converged")),
        _f(q.get("scf_dE")), _f(q.get("max_abs_free_minus_e0_per_atom")),
        _i(q.get("n_frames_scf_unconverged"), 0), _i(q.get("n_frames_with_forces"), 0),
        _i(q.get("n_frames_with_stress"), 0), _i(q.get("n_frames_dropped_no_energy"), 0),
        _f(el.get("net_magnetization")), _f(el.get("net_charge")),
        avail, _year(prov), origin_of(rec), *shard_range(rec),
    )


def _deposit_info(prov: dict) -> dict:
    names = prov.get("creators") or prov.get("authors") or []
    return {"title": prov.get("title"), "doi": prov.get("conceptdoi") or prov.get("doi"),
            "url": prov.get("url"), "license": prov.get("license"), "year": _year(prov),
            "resource_type": prov.get("resource_type"),
            "creators": [str(n) for n in names if n][:200],
            "references": [str(r) for r in (prov.get("references") or [])][:20]}


def _line_bounds(path: Path, parts: int) -> list[tuple[int, int]]:
    size = path.stat().st_size
    cuts = [0]
    with open(path, "rb") as fh:
        for i in range(1, parts):
            fh.seek(size * i // parts)
            fh.readline()  # move to the next line start
            cuts.append(min(fh.tell(), size))
    cuts.append(size)
    return [(a, b) for a, b in zip(cuts, cuts[1:]) if b > a]


def _meta_task(args: tuple[str, int, int, str]) -> dict:
    path_s, start, end, out_s = args
    cats = Cats()
    agg: dict[str, Counter] = {"u_values": Counter(), "potcar_symbols": Counter(),
                               "incar_tags": Counter()}
    rows: list[tuple] = []
    deposits: dict[str, dict] = {}
    bad = 0
    with open(path_s, "rb") as fh:
        fh.seek(start)
        while fh.tell() < end:
            line = fh.readline()
            if not line:
                break
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                bad += 1
                continue
            rows.append(calc_row(rec, cats, agg))
            prov = rec.get("provenance") or {}
            dep = deposit_of(prov)
            info = deposits.get(dep)
            if info is None:
                deposits[dep] = _deposit_info(prov)
            elif prov.get("source") == "nomad":  # an upload's entries may list different authors
                seen = set(info["creators"])
                for n in prov.get("authors") or []:
                    if n and n not in seen and len(info["creators"]) < 200:
                        info["creators"].append(str(n))
                        seen.add(n)
    arr = np.array(rows, dtype=CALC_DTYPE) if rows else np.zeros(0, dtype=CALC_DTYPE)
    out_agg: dict[str, Any] = {k: dict(v) for k, v in agg.items()}
    out_agg["bad_lines"] = bad
    atomic_write_npz(Path(out_s), calcs=arr, cats=text_array(json.dumps(cats.as_lists())),
                     deposits=text_array(json.dumps(deposits)),
                     agg=text_array(json.dumps(out_agg)))
    return {"chunk": Path(out_s).name, "calcs": len(rows), "bad_lines": bad}


def meta_dataset(metadata: str | Path, out_dir: str | Path, *, workers: int = 1,
                 parts: int | None = None) -> dict:
    """Run the metadata pass over ``metadata`` into ``out_dir`` (old chunks are replaced)."""
    metadata, out_dir = Path(metadata), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("chunk-*.npz"):
        old.unlink()
    nparts = parts or max(1, workers * 4)
    bounds = _line_bounds(metadata, nparts)
    tasks = [(str(metadata), a, b, str(out_dir / f"chunk-{i:03d}.npz"))
             for i, (a, b) in enumerate(bounds)]
    t0 = time.time()
    if workers <= 1:
        results = [_meta_task(t) for t in tasks]
    else:
        with get_context("fork").Pool(workers) as pool:
            results = list(pool.imap_unordered(_meta_task, tasks, chunksize=1))
    summary = {"metadata": str(metadata), "out_dir": str(out_dir), "chunks": len(tasks),
               "calcs": sum(r["calcs"] for r in results),
               "bad_lines": sum(r["bad_lines"] for r in results),
               "bytes": metadata.stat().st_size, "elapsed_s": round(time.time() - t0, 1)}
    with open(out_dir / "meta_summary.json", "w") as fh:
        json.dump(summary, fh, indent=1)
    logger.info("meta %s: %d calcs in %.0f s", metadata, summary["calcs"], summary["elapsed_s"])
    return summary


def load_meta(out_dir: str | Path) -> tuple[np.ndarray, dict[str, list[str]], dict[str, dict],
                                            dict[str, Any]]:
    """Merge every chunk: (calc rows with GLOBAL codes, global code tables, deposits, agg)."""
    chunks = sorted(Path(out_dir).glob("chunk-*.npz"))
    tables: dict[str, dict[str, int]] = {c: {} for c in CAT_COLS}
    parts: list[np.ndarray] = []
    deposits: dict[str, dict] = {}
    agg: dict[str, Any] = {}
    for ch in chunks:
        with np.load(ch, allow_pickle=False) as z:
            arr = z["calcs"].copy()
            cats = json.loads(z["cats"].tobytes().decode())
            deps = json.loads(z["deposits"].tobytes().decode())
            a = json.loads(z["agg"].tobytes().decode())
        for col in CAT_COLS:
            local = cats.get(col, [])
            g = tables[col]
            remap = np.array([g.setdefault(v, len(g)) for v in local] or [0], dtype=np.int64)
            if len(arr):
                arr[col] = remap[arr[col]]
        parts.append(arr)
        for d, info in deps.items():
            if d not in deposits:
                deposits[d] = info
            else:
                have = deposits[d]["creators"]
                seen = set(have)
                for n in info.get("creators", []):
                    if n not in seen and len(have) < 200:
                        have.append(n)
                        seen.add(n)
        for k, v in a.items():
            if isinstance(v, dict):
                c = agg.setdefault(k, Counter())
                c.update(v)
            else:
                agg[k] = agg.get(k, 0) + v
    rows = np.concatenate(parts) if parts else np.zeros(0, dtype=CALC_DTYPE)
    return rows, {c: list(t) for c, t in tables.items()}, deposits, agg


INDIVIDUAL_FILTER = "individual-uploads"


def individual_include(meta_dir: str | Path) -> tuple[dict[int, frozenset[int]] | None, dict]:
    """Which shards (and which calcs in them) hold individual uploads, i.e. calcs with
    ``origin == 0`` — for a scan that skips the institutional high-throughput data.

    Returns ``(None, info)`` when the source has nothing to exclude (scan every shard), else
    ``({shard index: calc keys}, info)``; shards absent from the mapping hold no wanted calc."""
    rows, _tabs, _deps, _agg = load_meta(meta_dir)
    if rows.dtype.names is None or "shard_lo" not in rows.dtype.names:
        raise ValueError(f"{meta_dir} predates the shard ranges: re-run `meta` first")
    excl = rows["origin"] > 0
    info = {"name": INDIVIDUAL_FILTER, "excluded_origins": list(ORIGIN_LABELS[1:]),
            "calcs_excluded": int(excl.sum()), "calcs_kept": int((~excl).sum()),
            "frames_excluded": int(rows["n_frames"][excl].astype(np.int64).sum())}
    if not excl.any():
        return None, info
    inc: dict[int, set[int]] = {}
    keep = rows[~excl]
    for k, lo, hi in zip(keep["calc"].tolist(), keep["shard_lo"].tolist(),
                         keep["shard_hi"].tolist()):
        for s_idx in range(lo, hi + 1) if lo >= 0 else ():
            inc.setdefault(s_idx, set()).add(k)
    info["shards_with_kept_calcs"] = len(inc)
    return {s_idx: frozenset(v) for s_idx, v in inc.items()}, info

