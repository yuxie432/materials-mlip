"""Report: join the metadata rows, the frame rows and the per-calc structure rows of each source
and compute every statistic, per source and for the sources together.

``build_report`` returns a JSON-ready dict; :mod:`dataset_stats.render` turns it into Markdown.
Weights: "calcs" counts each calc once, "frames" weights a calc by its stored frames (what a
training run sees before any subsampling), "deposits" counts distinct deposits (a Zenodo /
Materials Cloud concept record, a NOMAD upload). Everything is vectorised over integer codes:
NOMAD alone is ~7.1M calcs / ~52M frames.
"""

from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from . import scan as S
from .common import FORCE_EDGES, concentration, counter_top, hist, hist_quantile, summary
from .meta import AVAIL_KEYS, CAT_COLS, CALC_DTYPE, load_meta
from .params import LIB_BITS
from .structure import VACUUM_GAP_A
from .tables import formula_counts, load_calc_structs, load_frames

logger = logging.getLogger(__name__)

TWO_PI = 2.0 * math.pi
NATOMS_EDGES = np.array([0.5, 1.5, 10.5, 50.5, 100.5, 200.5, 500.5, 1000.5, 1e9])
NATOMS_LABELS = ["1", "2-10", "11-50", "51-100", "101-200", "201-500", "501-1000", ">1000"]
FMAX_THRESHOLDS = (0.01, 0.03, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0)
CRYSTAL_SYSTEMS = ((1, 2, "triclinic"), (3, 15, "monoclinic"), (16, 74, "orthorhombic"),
                   (75, 142, "tetragonal"), (143, 167, "trigonal"), (168, 194, "hexagonal"),
                   (195, 230, "cubic"))
DIM_NAMES = ["unknown", "bulk (no vacuum)", "slab / 2D (1 vacuum axis)", "wire / 1D (2 axes)",
             "molecule / cluster (3 axes)"]
POTCAR_CLASSES = ["PBE-family", "LDA", "PW91", "US", "unknown-titel", "mixed-releases"]


def _share(n: float, d: float) -> float | None:
    return None if not d else round(float(n) / float(d), 6)


def _cat_table(codes: np.ndarray, table: list[str], weights: dict[str, np.ndarray],
               top: int = 40, sort_by: str = "frames") -> list[dict]:
    """``[{value, calcs, calcs_share, frames, frames_share}, ...]`` for a coded column."""
    n = max(len(table), int(codes.max()) + 1 if len(codes) else 0)
    cols = {k: np.bincount(codes, weights=w, minlength=n) for k, w in weights.items()}
    totals = {k: float(v.sum()) for k, v in cols.items()}
    key = sort_by if sort_by in cols else next(iter(cols))
    order = np.argsort(-cols[key], kind="stable")
    rows = []
    for i in order[:top]:
        if all(cols[k][i] == 0 for k in cols):
            continue
        row: dict[str, Any] = {"value": table[i] if i < len(table) else str(i)}
        for k in cols:
            row[k] = int(round(cols[k][i]))
            row[f"{k}_share"] = _share(cols[k][i], totals[k])
        rows.append(row)
    rest = order[top:]
    if len(rest) and any(cols[k][rest].sum() for k in cols):
        row = {"value": f"(other {int((cols[key][rest] > 0).sum())} values)"}
        for k in cols:
            row[k] = int(round(cols[k][rest].sum()))
            row[f"{k}_share"] = _share(row[k], totals[k])
        rows.append(row)
    return rows


def _value_table(values: np.ndarray, weights: dict[str, np.ndarray], top: int = 30,
                 fmt: Any = str) -> list[dict]:
    """:func:`_cat_table` for a raw numeric column (codes made with ``np.unique``)."""
    if len(values) == 0:
        return []
    uniq, inv = np.unique(values, return_inverse=True)
    return _cat_table(inv.ravel(), [fmt(u.item() if hasattr(u, "item") else u) for u in uniq],
                      weights, top=top)


def _ndeposits(dep: np.ndarray, mask: np.ndarray) -> int:
    return int(np.unique(dep[mask]).size)


def _code(tabs: dict, col: str, value: str) -> int:
    return tabs[col].index(value) if value in tabs[col] else -1


def _subset(W: dict[str, np.ndarray], m: np.ndarray) -> dict[str, np.ndarray]:
    return {k: v[m] for k, v in W.items()}


def _join(keys_sorted: np.ndarray, order: np.ndarray, keys: np.ndarray
          ) -> tuple[np.ndarray, np.ndarray]:
    """Row index into the unsorted table for each key, and whether it was found."""
    if len(keys_sorted) == 0:
        return np.zeros(len(keys), dtype=np.int64), np.zeros(len(keys), dtype=bool)
    pos = np.clip(np.searchsorted(keys_sorted, keys), 0, len(keys_sorted) - 1)
    return order[pos], keys_sorted[pos] == keys


# --------------------------------------------------------------------------------------------
def source_report(name: str, meta_dir: Path, scan_dir: Path, dataset_dir: Path | None = None,
                  mu: dict[str, float] | None = None
                  ) -> tuple[dict, dict, dict[str, tuple[dict, dict]]]:
    """(report, internal arrays, origin subsets) for one harvested source.

    When some calcs come from institutional high-throughput runs (``meta.ORIGIN_LABELS``: the
    Alexandria database's own runs inside NOMAD's direct uploads), the source is also analysed as
    two subsets, ``<source>[long-tail]`` and ``<source>[alexandria-group]``."""
    logger.info("[%s] loading metadata rows", name)
    meta, tabs, deposits, magg = load_meta(meta_dir)
    logger.info("[%s] %d calcs; loading frame rows", name, len(meta))
    frames, fagg = load_frames(scan_dir)  # empty when the source has not been scanned yet
    logger.info("[%s] %d frames; loading per-calc structures", name, len(frames))
    structs, formulas, systems, _groups = load_calc_structs(scan_dir)
    rep, internal = _analyse(name, meta, tabs, frames, fagg, structs, formulas, systems,
                             deposits=deposits, magg=magg, dataset_dir=dataset_dir,
                             reference=False, mu=mu)
    subsets: dict[str, tuple[dict, dict]] = {}
    if (meta["origin"] > 0).any():
        no_fhist = dict(fagg, force_hist=np.zeros_like(fagg["force_hist"]))
        for label, keep in (("long-tail", meta["origin"] == 0),
                            ("alexandria-group", meta["origin"] > 0)):
            sub_meta = meta[keep]
            keys = np.unique(sub_meta["calc"])
            sub_frames = frames[np.isin(frames["calc"], keys)]
            sub_structs = structs[np.isin(structs["calc"], keys)]
            sname = f"{name}[{label}]"
            logger.info("[%s] %d calcs, %d frames", sname, len(sub_meta), len(sub_frames))
            subsets[sname] = _analyse(sname, sub_meta, tabs, sub_frames, no_fhist, sub_structs,
                                      formulas, systems, deposits=deposits, magg=magg,
                                      dataset_dir=None, reference=False, mu=mu)
    return rep, internal, subsets


def reference_report(name: str, scan_dir: Path, mu: dict[str, float] | None = None
                     ) -> tuple[dict, dict]:
    """The same statistics for a reference dataset (no metadata.jsonl: calcs are trajectories,
    "deposits" are materials / parent structures)."""
    logger.info("[ref %s] loading", name)
    frames, fagg = load_frames(scan_dir)
    structs, formulas, systems, groups = load_calc_structs(scan_dir)
    meta = np.zeros(len(structs), dtype=CALC_DTYPE)
    for col in ("encut", "ediff", "ediffg", "sigma", "potim", "tebeg", "kspacing", "scf_dE",
                "dfree_max", "mag", "charge"):
        meta[col] = np.nan
    meta["calc"] = structs["calc"]
    meta["n_frames"] = structs["n_seen"]
    meta["n_atoms"] = structs["natoms"]
    meta["deposit"] = np.where(structs["group"] >= 0, structs["group"], 0)
    tabs = {c: ["reference"] for c in CAT_COLS}
    tabs["deposit"] = groups or ["?"]
    return _analyse(name, meta, tabs, frames, fagg, structs, formulas, systems, deposits={},
                    magg={}, dataset_dir=None, reference=True, mu=mu)


def _effective_per_calc(meta: np.ndarray, frames: np.ndarray, mk: np.ndarray,
                        morder: np.ndarray, thr_ev: float = 0.010) -> np.ndarray:
    """Frames each calc keeps under the sAlex rule (dE/atom > 10 meV from the last kept frame),
    aligned with ``meta`` rows (calcs without scanned frames keep their stored count)."""
    eff = meta["n_frames"].astype(np.float64).copy()
    order, starts, ends = _calc_groups(frames)
    if len(starts) == 0:
        return eff
    epa = frames["energy"].astype(float) / np.maximum(frames["natoms"], 1)
    kept = subsample_per_calc(epa[order], starts, ends, thr_ev)
    mi, ok = _join(mk, morder, frames["calc"][order][starts])
    eff[mi[ok]] = kept[ok]
    return eff


def _analyse(name: str, meta: np.ndarray, tabs: dict, frames: np.ndarray, fagg: dict,
             structs: np.ndarray, formulas: list[str], systems: list[str], *, deposits: dict,
             magg: dict, dataset_dir: Path | None, reference: bool,
             mu: dict[str, float] | None = None) -> tuple[dict, dict]:
    have_scan = len(frames) > 0
    rep: dict[str, Any] = {"source": name, "reference": reference, "have_scan": have_scan}
    order = np.argsort(meta["calc"], kind="stable")
    mk = meta["calc"][order]
    f_mi, f_ok = _join(mk, order, frames["calc"])
    s_mi, s_ok = _join(mk, order, structs["calc"])
    W = {"calcs": np.ones(len(meta)), "frames": meta["n_frames"].astype(np.float64)}
    rep["size"] = _size(meta, frames, fagg, f_ok, tabs, dataset_dir)
    rep["concentration"] = _concentration(meta, deposits, tabs)
    if not reference:
        rep["settings"] = _settings(meta, tabs, magg, W, structs, s_mi, s_ok)
        rep["quality"] = _quality(meta, tabs, W)
        rep["electronic"] = _electronic(meta, tabs, W)
        rep["availability"] = _availability(meta)
        rep["provenance"] = _provenance(meta, tabs, deposits, W)
        rep["compatibility"] = _compat(meta)
        rep["buckets"] = _buckets(meta, tabs, W)
    internal: dict[str, Any] = {"fhash": np.zeros(0, "u8"), "shash": np.zeros(0, "u8")}
    if have_scan:
        logger.info("[%s] chemistry / structure / labels", name)
        eff = _effective_per_calc(meta, frames, mk, order)
        rep["chemistry"], chem_int = _chemistry(meta, tabs, structs, s_mi, s_ok, formulas,
                                                systems, eff)
        rep["structure"] = _structure(meta, structs, s_mi, s_ok, frames)
        rep["labels"] = _labels(meta, tabs, frames, f_mi, f_ok, fagg)
        if mu:
            rep["labels"]["formation_energy_vs_mp_elements"] = _formation_proxy(
                meta, frames, f_mi, f_ok, structs, formulas, mu, reference)
        logger.info("[%s] redundancy", name)
        rep["redundancy"] = _redundancy(meta, tabs, frames, structs, s_mi, s_ok, mk, order)
        if not reference:
            rep["curation"] = _curation(meta, tabs, frames, f_mi, f_ok, structs, s_ok)
        rep["scan_integrity"] = _scan_integrity(fagg, frames, f_ok, meta, mk)
        internal.update(chem_int)
        internal["fhash"] = np.unique(frames["fhash"])
        internal["shash"] = np.unique(frames["shash"])
    return rep, internal


# --------------------------------------------------------------------------------------------
def _size(meta: np.ndarray, frames: np.ndarray, fagg: dict, f_ok: np.ndarray, tabs: dict,
          dataset_dir: Path | None) -> dict:
    fl = frames["flags"]
    has_f = (fl & S.F_FORCES) > 0
    out: dict[str, Any] = {
        "deposits": int(np.unique(meta["deposit"]).size),
        "calcs": int(len(meta)),
        "frames_metadata": int(meta["n_frames"].astype(np.int64).sum()),
        "frames_scanned": int(len(frames)),
        "atoms_in_frames": int(frames["natoms"].astype(np.int64).sum()),
        "force_labels_atoms": int(frames["natoms"][has_f].astype(np.int64).sum()),
        "frames_with_forces": int(has_f.sum()),
        "frames_with_stress": int(((fl & S.F_STRESS) > 0).sum()),
        "frames_with_free_energy": int(((fl & S.F_EFREE) > 0).sum()),
        "frames_without_metadata": int((~f_ok).sum()),
        "frames_per_calc": summary(meta["n_frames"]),
        "shards": fagg["shards"],
    }
    if len(frames):
        out["atoms_per_frame"] = summary(frames["natoms"])
    # the same totals from the metadata alone (what a report without a scan can show)
    nf = meta["n_forces"].astype(np.int64)
    out["metadata_frames_with_forces"] = int(nf.sum())
    out["metadata_frames_with_stress"] = int(meta["n_stress"].astype(np.int64).sum())
    out["metadata_force_labels_atoms"] = int((nf * np.maximum(meta["n_atoms"], 0)).sum())
    dep_calcs = np.bincount(meta["deposit"], minlength=len(tabs["deposit"]))
    out["calcs_per_deposit"] = summary(dep_calcs[dep_calcs > 0])
    if dataset_dir is not None and Path(dataset_dir).is_dir():
        shards = list(Path(dataset_dir).glob("shard-*.extxyz.gz"))
        out["dataset_shard_bytes"] = int(sum(p.stat().st_size for p in shards))
        mp = Path(dataset_dir) / "metadata.jsonl"
        out["metadata_bytes"] = int(mp.stat().st_size) if mp.is_file() else None
    return out


def _concentration(meta: np.ndarray, deposits: dict, tabs: dict) -> dict:
    nd = len(tabs["deposit"])
    fr = np.bincount(meta["deposit"], weights=meta["n_frames"].astype(float), minlength=nd)
    ca = np.bincount(meta["deposit"], minlength=nd).astype(float)
    rows = []
    for i in np.argsort(-fr)[:20]:
        if fr[i] <= 0:
            continue
        dep = tabs["deposit"][i]
        info = deposits.get(dep, {})
        rows.append({"deposit": dep, "title": (info.get("title") or "")[:120],
                     "frames": int(fr[i]), "frames_share": _share(fr[i], fr.sum()),
                     "calcs": int(ca[i]), "calcs_share": _share(ca[i], ca.sum()),
                     "license": info.get("license"), "year": info.get("year")})
    return {"frames_over_deposits": concentration(fr), "calcs_over_deposits": concentration(ca),
            "frames_over_calcs": concentration(meta["n_frames"].astype(float)),
            "top_deposits_by_frames": rows}


def _settings(meta: np.ndarray, tabs: dict, magg: dict, W: dict, structs: np.ndarray,
              s_mi: np.ndarray, s_ok: np.ndarray) -> dict:
    out: dict[str, Any] = {}
    for col in ("parser", "family", "label", "run_type", "vdw", "version", "prec", "lreal",
                "algo", "ctype", "kstyle"):
        out[col] = _cat_table(meta[col], tabs[col], W, top=15 if col == "algo" else 40)
    u = meta["u"] == 1
    out["hubbard_u"] = {"calcs": int(u.sum()), "frames": int(meta["n_frames"][u].sum()),
                        "top_values_calcs": counter_top(dict(magg.get("u_values", {})), 30)}
    enc_edges = np.array([0, 300, 400, 450, 500, 519.5, 520.5, 550, 600, 700, 800, 1000, 1e6])
    out["encut"] = {"summary_calcs": summary(meta["encut"]),
                    "summary_frames": summary(meta["encut"], W["frames"]),
                    "hist_calcs": hist(meta["encut"], enc_edges),
                    "hist_frames": hist(meta["encut"], enc_edges, W["frames"])}
    ed = np.log10(np.where(meta["ediff"] > 0, meta["ediff"], np.nan))
    out["ediff_log10"] = {"summary_frames": summary(ed, W["frames"]),
                          "hist_frames": hist(ed, np.arange(-10.5, -0.49, 1.0), W["frames"])}
    sig = np.where(np.isfinite(meta["sigma"]),
                   np.round(meta["sigma"].astype(float) * 1000).clip(0, 999_998), 999_999)
    combo = (meta["ismear"].astype(np.int64) + 100) * 1_000_000 + sig.astype(np.int64)

    def _smear(v: int) -> str:
        ism, sc = v // 1_000_000 - 100, v % 1_000_000
        return (f"ISMEAR={'?' if ism == -99 else ism} "
                f"SIGMA={'?' if sc == 999_999 else f'{sc / 1000:g}'}")
    out["smearing"] = _value_table(combo, W, top=20, fmt=_smear)
    for col, lab in (("ispin", "ISPIN"), ("ncl", "LNONCOLLINEAR"), ("soc", "LSORBIT"),
                     ("lasph", "LASPH")):
        out[lab] = _value_table(meta[col], W)
    kp = meta["kprod"] > 0
    kppra = np.where(kp & (meta["n_atoms"] > 0),
                     meta["kprod"].astype(float) * meta["n_atoms"].astype(float), np.nan)
    out["kppra"] = {"calcs_with_grid": int(kp.sum()), "summary_calcs": summary(kppra),
                    "summary_frames": summary(kppra, W["frames"]),
                    "hist_calcs": hist(kppra, np.array([0, 1.5, 50, 100, 200, 500, 1000, 2000,
                                                        5000, 1e4, 1e9]))}
    out["kspacing_user"] = summary(meta["kspacing"])
    if len(structs):
        st = structs[s_ok]
        mi = s_mi[s_ok]
        gaps = np.stack([st["gap0"], st["gap1"], st["gap2"]], axis=1)
        bulk = (gaps < VACUUM_GAP_A).all(axis=1)
        g = np.stack([meta["k1"][mi], meta["k2"][mi], meta["k3"][mi]], axis=1).astype(float)
        rl = np.stack([st["rlen0"], st["rlen1"], st["rlen2"]], axis=1).astype(float)
        okg = bulk & (g > 0).all(axis=1) & np.isfinite(rl).all(axis=1)
        eff = np.full(len(st), np.nan)
        eff[okg] = np.max(TWO_PI * rl[okg] / g[okg], axis=1)
        wf = meta["n_frames"][mi].astype(float)
        out["kspacing_effective_bulk"] = {
            "calcs": int(okg.sum()), "summary_calcs": summary(eff),
            "summary_frames": summary(eff, wf),
            "hist_calcs": hist(eff, np.array([0, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0,
                                              1.5, 100]))}
    lib = {}
    for lab, bit in LIB_BITS.items():
        m = (meta["potlib"] & bit) > 0
        lib[lab] = {"calcs": int(m.sum()), "frames": int(meta["n_frames"][m].sum())}
    out["potcar_releases_containing_all_titels"] = lib
    null_code = _code(tabs, "pset", "null")
    nonnull = meta["pset"] != null_code
    out["potcar_sets"] = {"distinct": int(np.unique(meta["pset"][nonnull]).size),
                          "calcs_without": int((~nonnull).sum()),
                          "top_symbols_calcs": counter_top(dict(magg.get("potcar_symbols", {})),
                                                           60)}
    md = meta["ctype"] == _code(tabs, "ctype", "md")
    out["md"] = {"calcs": int(md.sum()), "frames": int(meta["n_frames"][md].sum()),
                 "tebeg_frames": summary(meta["tebeg"][md], W["frames"][md]),
                 "tebeg_hist_frames": hist(meta["tebeg"][md],
                                           np.array([0, 100, 200, 300, 400, 500, 600, 800, 1000,
                                                     1500, 2000, 3000, 5000, 1e5]),
                                           W["frames"][md]),
                 "potim_fs_frames": summary(meta["potim"][md], W["frames"][md]),
                 "mdalgo": _value_table(meta["mdalgo"][md], _subset(W, md)),
                 "isif": _value_table(meta["isif"][md], _subset(W, md))}
    rl_ = np.isin(meta["ctype"], [_code(tabs, "ctype", "relax"),
                                  _code(tabs, "ctype", "relax-cell")])
    out["relax"] = {"calcs": int(rl_.sum()), "frames": int(meta["n_frames"][rl_].sum()),
                    "frames_per_calc": summary(meta["n_frames"][rl_]),
                    "isif": _value_table(meta["isif"][rl_], _subset(W, rl_)),
                    "ediffg": summary(meta["ediffg"][rl_])}
    out["incar_tags_top"] = counter_top(dict(magg.get("incar_tags", {})), 80)
    return out


def _quality(meta: np.ndarray, tabs: dict, W: dict) -> dict:
    rel = np.isin(meta["ctype"], [_code(tabs, "ctype", "relax"),
                                  _code(tabs, "ctype", "relax-cell")])
    return {
        "calc_electronic_converged": _value_table(meta["econv"], W),
        "calc_ionic_converged_relaxations": _value_table(meta["iconv"][rel], _subset(W, rel)),
        "frames_scf_unconverged": int(meta["n_unconv"].astype(np.int64).sum()),
        "frames_dropped_no_energy": int(meta["n_dropped"].astype(np.int64).sum()),
        "frames_without_forces": int((meta["n_frames"] - meta["n_forces"]).clip(0).sum()),
        "frames_without_stress": int((meta["n_frames"] - meta["n_stress"]).clip(0).sum()),
        "max_abs_free_minus_e0_per_atom": summary(meta["dfree_max"]),
        "calc_final_scf_dE_abs": summary(np.abs(meta["scf_dE"])),
    }


def _electronic(meta: np.ndarray, tabs: dict, W: dict) -> dict:
    mag = meta["mag"].astype(float)
    ch = meta["charge"].astype(float)
    nat = np.where(meta["n_atoms"] > 0, meta["n_atoms"], np.nan).astype(float)
    km, kc = np.isfinite(mag), np.isfinite(ch)
    rch = np.where(kc, np.round(ch, 2), 1e9)
    return {
        "magnetization_source": _cat_table(meta["mag_src"], tabs["mag_src"], W),
        "charge_source": _cat_table(meta["charge_src"], tabs["charge_src"], W),
        "net_moment_known": {k: int(w[km].sum()) for k, w in W.items()},
        "net_moment_abs_gt_0.01": {k: int(w[km & (np.abs(mag) > 0.01)].sum())
                                   for k, w in W.items()},
        "net_moment_abs_gt_0.5": {k: int(w[km & (np.abs(mag) > 0.5)].sum())
                                  for k, w in W.items()},
        "net_moment_abs_calcs": summary(np.abs(mag)),
        "net_moment_per_atom_abs_calcs": summary(np.abs(mag) / nat),
        "net_moment_abs_hist_calcs": hist(np.abs(mag), np.array([0, 1e-3, 0.01, 0.1, 0.5, 1, 2,
                                                                 5, 10, 20, 50, 100, 1e6])),
        "net_charge_known": {k: int(w[kc].sum()) for k, w in W.items()},
        "net_charge_nonzero": {k: int(w[kc & (np.abs(ch) > 0.01)].sum())
                               for k, w in W.items()},
        "net_charge_values_top": _value_table(rch, W, top=25,
                                              fmt=lambda v: "unknown" if v > 1e8 else f"{v:g}"),
    }


def _availability(meta: np.ndarray) -> dict:
    out = {}
    for i, key in enumerate(AVAIL_KEYS):
        m = (meta["avail"] & (1 << i)) > 0
        out[key] = {"calcs": int(m.sum()), "frames": int(meta["n_frames"][m].sum()),
                    "deposits": _ndeposits(meta["deposit"], m),
                    "calcs_share": _share(int(m.sum()), len(meta))}
    heavy = (meta["avail"] & 0b11) > 0
    out["charge_density_or_wavefunction"] = {"calcs": int(heavy.sum()),
                                             "deposits": _ndeposits(meta["deposit"], heavy)}
    return out


def _norm_name(n: str) -> str:
    return " ".join(str(n).lower().replace(",", " ").split())


def _provenance(meta: np.ndarray, tabs: dict, deposits: dict, W: dict) -> dict:
    present = [tabs["deposit"][i] for i in np.unique(meta["deposit"])]
    creators: set[str] = set()
    years: Counter = Counter()
    lic: Counter = Counter()
    rtype: Counter = Counter()
    with_doi = with_refs = 0
    for d in present:
        info = deposits.get(d, {})
        for n in info.get("creators") or []:
            creators.add(_norm_name(n))
        years[str(info.get("year"))] += 1
        lic[str(info.get("license"))] += 1
        rtype[str(info.get("resource_type"))] += 1
        with_doi += bool(info.get("doi"))
        with_refs += bool(info.get("references"))
    # independent groups: frames / calcs per FIRST creator of the deposit (a NOMAD upload lists its
    # uploader first), the closest proxy for "how many research groups" the data comes from
    first = [(_norm_name((deposits.get(d, {}).get("creators") or ["?"])[0])) for d in tabs["deposit"]]
    names = sorted(set(first))
    fcode = {n: i for i, n in enumerate(names)}
    dep_author = np.array([fcode[n] for n in first], dtype=np.int64)
    a_frames = np.bincount(dep_author[meta["deposit"]], weights=W["frames"], minlength=len(fcode))
    a_calcs = np.bincount(dep_author[meta["deposit"]], minlength=len(fcode)).astype(float)
    top_auth = [{"value": names[i], "frames": int(a_frames[i]), "calcs": int(a_calcs[i]),
                 "frames_share": _share(a_frames[i], a_frames.sum()),
                 "calcs_share": _share(a_calcs[i], a_calcs.sum())}
                for i in np.argsort(-a_frames)[:15] if a_frames[i] > 0]
    out_auth = {"first_authors": int((a_calcs > 0).sum()),
                "frames_over_first_authors": concentration(a_frames),
                "calcs_over_first_authors": concentration(a_calcs),
                "top_first_authors": top_auth}
    return {**out_auth, "deposits": len(present), "distinct_creator_names": len(creators),
            "deposits_with_doi": with_doi, "deposits_with_references": with_refs,
            "deposits_by_year": dict(sorted(years.items())),
            "deposits_by_license": dict(lic.most_common()),
            "deposits_by_resource_type": dict(rtype.most_common()),
            "license": _cat_table(meta["license"], tabs["license"], W),
            "resource_type": _cat_table(meta["rtype"], tabs["rtype"], W),
            "year": _value_table(meta["year"].astype(int), W, top=40)}


def _compat(meta: np.ndarray) -> dict:
    out = {}
    enc_ok = meta["encut"] >= 519.5
    for col, lab in (("mp", "materials_project_gga_u"), ("omat", "omat24_pbe54")):
        m = meta[col] == 1
        out[lab] = {"calcs": int(m.sum()), "frames": int(meta["n_frames"][m].sum()),
                    "deposits": _ndeposits(meta["deposit"], m),
                    "calcs_encut_ge_520": int((m & enc_ok).sum()),
                    "frames_encut_ge_520": int(meta["n_frames"][m & enc_ok].sum()),
                    "undeterminable_calcs": int((meta[col] == -1).sum())}
    return out


def _potcar_class(bits: np.ndarray) -> np.ndarray:
    pbe = LIB_BITS["PBE"] | LIB_BITS["PBE_52"] | LIB_BITS["PBE_54"] | LIB_BITS["PBE_64"]
    out = np.full(len(bits), POTCAR_CLASSES.index("mixed-releases"), dtype=np.int64)
    for name, bit in (("US", LIB_BITS["US"]), ("PW91", LIB_BITS["PW91*"]),
                      ("LDA", LIB_BITS["LDA*"]), ("PBE-family", pbe)):
        out[(bits & bit) > 0] = POTCAR_CLASSES.index(name)
    out[(bits & LIB_BITS["unknown_titel"]) > 0] = POTCAR_CLASSES.index("unknown-titel")
    return out


def _buckets(meta: np.ndarray, tabs: dict, W: dict) -> dict:
    """Consistency buckets: XC label x POTCAR release class, and exact POTCAR sets."""
    cls = _potcar_class(meta["potlib"].astype(np.int64))
    key = meta["label"].astype(np.int64) * len(POTCAR_CLASSES) + cls
    uniq, inv = np.unique(key, return_inverse=True)
    names = [f"{tabs['label'][k // len(POTCAR_CLASSES)]} | {POTCAR_CLASSES[k % len(POTCAR_CLASSES)]}"
             for k in uniq.tolist()]
    fr = np.bincount(inv.ravel(), weights=W["frames"])
    return {"label_x_potcar_class": _cat_table(inv.ravel(), names, W, top=30),
            "n_label_x_potcar_class": int(len(uniq)),
            "largest_bucket_frames_share": _share(fr.max() if len(fr) else 0, fr.sum()),
            "top_potcar_sets": _cat_table(meta["pset"], tabs["pset"], W, top=15)}


# --------------------------------------------------------------------------------------------
def _chemistry(meta: np.ndarray, tabs: dict, structs: np.ndarray, s_mi: np.ndarray,
               s_ok: np.ndarray, formulas: list[str], systems: list[str], eff: np.ndarray
               ) -> tuple[dict, dict]:
    st = structs[s_ok]
    mi = s_mi[s_ok]
    nfr = meta["n_frames"][mi].astype(np.int64)
    dep = meta["deposit"][mi].astype(np.int64)
    nat = st["natoms"].astype(np.int64)
    nf = len(formulas)
    calcs_f = np.bincount(st["formula"], minlength=nf)
    frames_f = np.bincount(st["formula"], weights=nfr, minlength=nf)
    atomsfr_f = np.bincount(st["formula"], weights=nat * nfr, minlength=nf)
    comp_of = [formula_counts(f) for f in formulas]
    el_calcs: Counter = Counter()
    el_frames: Counter = Counter()
    el_atoms: Counter = Counter()
    el_systems: Counter = Counter()
    for code in np.flatnonzero(calcs_f):
        comp = comp_of[code]
        fu = sum(comp.values()) or 1
        for el, c in comp.items():
            el_calcs[el] += int(calcs_f[code])
            el_frames[el] += int(frames_f[code])
            el_atoms[el] += int(round(atomsfr_f[code] * c / fu))
    ndep = int(dep.max()) + 1 if len(dep) else 1
    el_deps: dict[str, set] = defaultdict(set)
    for p in np.unique(st["formula"].astype(np.int64) * ndep + dep).tolist():
        code, d = divmod(p, ndep)
        for el in comp_of[code]:
            el_deps[el].add(d)
    sys_calcs = np.bincount(st["chemsys"], minlength=len(systems))
    pairs: set[tuple[str, str]] = set()
    for code in np.flatnonzero(sys_calcs):
        els = systems[code].split("-")
        for el in els:
            el_systems[el] += 1
        for i in range(len(els)):
            for j in range(i + 1, len(els)):
                pairs.add((els[i], els[j]))
    elements = sorted(el_calcs, key=lambda e: -el_frames[e])
    w = {"calcs": np.ones(len(st)), "frames": nfr.astype(float)}
    out = {
        "n_elements": len(elements),
        "elements": {el: {"frames": el_frames[el], "calcs": el_calcs[el], "atoms": el_atoms[el],
                          "deposits": len(el_deps[el]), "chemical_systems": el_systems[el]}
                     for el in elements},
        "n_reduced_formulas": int(np.count_nonzero(calcs_f)),
        "n_chemical_systems": int(np.count_nonzero(sys_calcs)),
        "n_element_pairs_cooccurring": len(pairs),
        "n_ary": _value_table(np.clip(st["nel"].astype(int), 0, 8), w),
        "top_formulas_by_frames": _cat_table(st["formula"], formulas, w, top=30),
        "top_formulas_by_calcs": _cat_table(st["formula"], formulas, w, top=30, sort_by="calcs"),
        "top_chemsys_by_calcs": _cat_table(st["chemsys"], systems, w, top=30, sort_by="calcs"),
        "calcs_without_structure_row": int(len(meta) - int(s_ok.sum())),
    }
    if tabs["pchemsys"] != ["reference"]:
        out["structure_vs_potcar_elements"] = _potcar_mismatch(meta, tabs, st, mi, systems)
    # per-calc arrays for the novelty section (prototype = formula x space group of a bulk cell
    # whose symmetry is not P1, so AIMD snapshots and rattled cells do not count as prototypes)
    gaps = np.stack([st["gap0"], st["gap1"], st["gap2"]], axis=1).astype(float)
    bulk = np.isfinite(gaps).all(axis=1) & (gaps < VACUUM_GAP_A).all(axis=1)
    spg = np.where(st["l_spg"] > 0, st["l_spg"], st["spg"]).astype(np.int64)
    has_proto = bulk & (spg > 1)
    proto_key = np.where(has_proto, st["formula"].astype(np.int64) * 256 + spg, -1)
    uniq_p, inv_p = np.unique(proto_key, return_inverse=True)
    proto_names = [f"{formulas[k // 256]}|{k % 256}" if k >= 0 else "" for k in uniq_p.tolist()]
    internal = {
        "elements": set(elements),
        "systems": {systems[c] for c in np.flatnonzero(sys_calcs)},
        "formulas": {formulas[c] for c in np.flatnonzero(calcs_f)},
        "prototypes": {n for n in proto_names if n},
        "formula_names": formulas, "system_names": systems, "proto_names": proto_names,
        "calc_formula": st["formula"].astype(np.int64), "calc_system": st["chemsys"].astype(np.int64),
        "calc_proto": inv_p.ravel().astype(np.int64), "calc_has_proto": has_proto,
        "calc_frames": nfr.astype(np.float64), "calc_eff": eff[mi].astype(np.float64),
        "calc_dep": dep, "calc_bulk": bulk,
    }
    return out, internal


def _potcar_mismatch(meta: np.ndarray, tabs: dict, st: np.ndarray, mi: np.ndarray,
                     systems: list[str]) -> dict:
    """Calcs whose structure element set differs from the POTCAR element set (pseudo-H, dummy
    species, a mislabelled POTCAR list) — a provenance-consistency check."""
    pcodes = meta["pchemsys"][mi]
    ptab = np.array(tabs["pchemsys"], dtype=object)
    stab = np.array(systems, dtype=object)
    null = _code(tabs, "pchemsys", "null")
    known = pcodes != null
    # compare via a code map: pchemsys string -> systems code (or -1)
    smap = {s: i for i, s in enumerate(systems)}
    pmap = np.array([smap.get(p, -1) for p in tabs["pchemsys"]], dtype=np.int64)
    mism = known & (pmap[pcodes] != st["chemsys"])
    idx = np.flatnonzero(mism)[:10]
    return {"calcs_compared": int(known.sum()), "calcs_mismatched": int(mism.sum()),
            "examples_potcar_vs_structure": [(str(ptab[pcodes[i]]), str(stab[st["chemsys"][i]]))
                                             for i in idx]}


def _structure(meta: np.ndarray, structs: np.ndarray, s_mi: np.ndarray, s_ok: np.ndarray,
               frames: np.ndarray) -> dict:
    st = structs[s_ok]
    mi = s_mi[s_ok]
    w = {"calcs": np.ones(len(st)), "frames": meta["n_frames"][mi].astype(float)}
    nat_bin = np.clip(np.digitize(st["natoms"], NATOMS_EDGES) - 1, 0, len(NATOMS_LABELS) - 1)
    natoms_rows = _cat_table(nat_bin, NATOMS_LABELS, w, top=20)
    natoms_rows.sort(key=lambda r: NATOMS_LABELS.index(r["value"])
                     if r["value"] in NATOMS_LABELS else 99)
    gaps = np.stack([st["gap0"], st["gap1"], st["gap2"]], axis=1).astype(float)
    finite = np.isfinite(gaps).all(axis=1)
    dims = {}
    for thr in (4.5, VACUUM_GAP_A, 8.0):
        code = np.where(finite, (gaps >= thr).sum(axis=1) + 1, 0)
        rows = _cat_table(code, DIM_NAMES, w)
        rows.sort(key=lambda r: DIM_NAMES.index(r["value"]) if r["value"] in DIM_NAMES else 9)
        dims[f"threshold_{thr:g}A"] = rows
    nvac = np.where(finite, (gaps >= VACUUM_GAP_A).sum(axis=1), -1)
    bulk = nvac == 0
    slab = nvac == 1
    vac_width = np.where(slab, np.nanmax(np.where(finite[:, None], gaps, np.nan), axis=1), np.nan)
    spg_f = st["spg"].astype(int)
    spg_l = st["l_spg"].astype(int)
    spg = np.where(spg_l > 0, spg_l, spg_f)
    cs_code = np.zeros(len(st), dtype=np.int64)
    cs_names = ["unknown"] + [c[2] for c in CRYSTAL_SYSTEMS]
    for i, (lo, hi, _n) in enumerate(CRYSTAL_SYSTEMS, start=1):
        cs_code[(spg >= lo) & (spg <= hi)] = i
    has_spg = bulk & (spg > 0)
    non_p1 = bulk & (spg > 1)  # prototypes: rattled / AIMD cells (P1) are not counted
    proto = np.unique(st["formula"][non_p1].astype(np.int64) * 256 + spg[non_p1])
    dmin = st["dmin"].astype(float)
    natf = frames["natoms"].astype(float)
    vpa = frames["volume"].astype(float) / np.where(natf > 0, natf, np.nan)
    bulk_frames = np.isin(frames["calc"], st["calc"][bulk])
    return {
        "natoms_bins": natoms_rows,
        "natoms_calcs": summary(st["natoms"]),
        "natoms_frames": summary(frames["natoms"]),
        "natoms_hist_frames": hist(frames["natoms"], np.array([0.5, 1.5, 2.5, 4.5, 8.5, 16.5,
                                                               32.5, 64.5, 128.5, 256.5, 512.5,
                                                               1024.5, 2048.5, 1e7])),
        "dimensionality": dims,
        "slab_vacuum_width_A": summary(vac_width),
        "volume_per_atom_bulk_frames": summary(vpa[bulk_frames]),
        "volume_per_atom_hist_bulk_frames": hist(vpa[bulk_frames],
                                                 np.array([0, 5, 8, 10, 12, 15, 20, 25, 30, 40,
                                                           60, 100, 1e9])),
        "density_bulk_calcs": summary(st["rho"][bulk]),
        "min_distance_calcs": summary(dmin),
        "min_distance_counts": {"lt_0.5A": int((dmin < 0.5).sum()),
                                "lt_0.7A": int((dmin < 0.7).sum()),
                                "lt_1.0A": int((dmin < 1.0).sum()),
                                "none_within_3A": int((~np.isfinite(dmin)).sum())},
        "bulk_crystal_systems": _cat_table(cs_code[bulk], cs_names, _subset(w, bulk)),
        "bulk_space_groups_distinct": int(np.unique(spg[has_spg]).size),
        "bulk_p1_share_calcs": _share(int((spg[has_spg] == 1).sum()), int(has_spg.sum())),
        "bulk_prototypes_formula_x_spg": int(len(proto)),
        "symmetry_changed_during_run_calcs": int(((spg_f > 0) & (spg_l > 0)
                                                  & (spg_f != spg_l)).sum()),
    }


def _labels(meta: np.ndarray, tabs: dict, frames: np.ndarray, f_mi: np.ndarray,
            f_ok: np.ndarray, fagg: dict) -> dict:
    nat = frames["natoms"].astype(float)
    natd = np.where(nat > 0, nat, np.nan)
    e = frames["energy"].astype(float)
    epa = e / natd
    fam = np.where(f_ok, meta["family"][f_mi], -1)
    ctype = np.where(f_ok, meta["ctype"][f_mi], -1)
    fmax = frames["fmax"].astype(float)
    fnet_pa = frames["fnet"].astype(float) / natd
    by_family = {}
    for code, n in Counter(fam.tolist()).most_common(12):
        m = fam == code
        name = tabs["family"][code] if code >= 0 else "no-metadata"
        by_family[name] = {"frames": int(n), "summary": summary(epa[m]),
                           "hist": hist(epa[m], np.arange(-15.0, 5.01, 0.25))}
    by_type = {}
    for code, n in Counter(ctype.tolist()).most_common():
        m = ctype == code
        name = tabs["ctype"][code] if code >= 0 else "no-metadata"
        by_type[name] = {"frames": int(n), "fmax": summary(fmax[m]),
                         "fnet_per_atom": summary(fnet_pa[m])}
    has_f = np.isfinite(fmax)
    fh = fagg["force_hist"]
    pr = frames["pressure"].astype(float)
    smax = frames["smax"].astype(float)
    dfa = np.abs(frames["dfree"].astype(float)) / natd
    fl = frames["flags"]
    return {
        "energy_per_atom": summary(epa),
        "energy_per_atom_by_family": by_family,
        "energy_positive_frames": int((e > 0).sum()),
        "fmax": summary(fmax),
        "fmax_hist": hist(fmax, FORCE_EDGES),
        "fmax_share_below": {f"{t:g}": _share(int((fmax[has_f] < t).sum()), int(has_f.sum()))
                             for t in FMAX_THRESHOLDS},
        "fmax_by_calc_type": by_type,
        "fmean": summary(frames["fmean"]),
        "frms": summary(frames["frms"]),
        "per_atom_force": {
            "atoms": int(fh.sum()),
            "quantiles": {f"p{q * 100:g}": hist_quantile(FORCE_EDGES, fh, q)
                          for q in (0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999)},
            "share_gt": {f"{t:g}": _share(int(fh[FORCE_EDGES[1:] > t].sum()), int(fh.sum()))
                         for t in (0.1, 0.5, 1.0, 5.0, 10.0, 50.0)},
            "hist": {"edges": [float(x) for x in FORCE_EDGES], "counts": fh.tolist()}},
        "fnet_per_atom": summary(fnet_pa),
        "pressure_gpa": summary(pr),
        "pressure_hist_gpa": hist(pr, np.array([-1e6, -100, -50, -20, -10, -5, -2, -1, -0.5, 0.5,
                                                1, 2, 5, 10, 20, 50, 100, 1e6])),
        "max_abs_stress_gpa": summary(smax),
        "max_abs_stress_share_gt": {f"{t:g}": _share(int((smax > t).sum()),
                                                     int(np.isfinite(smax).sum()))
                                    for t in (1, 5, 10, 50, 80)},
        "free_minus_e0_per_atom_abs": summary(dfa),
        "free_minus_e0_per_atom_share_gt": {f"{t:g}meV": _share(int((dfa > t / 1000).sum()),
                                                                int(np.isfinite(dfa).sum()))
                                            for t in (1, 10, 50)},
        "frame_scf": {"converged": int((frames["econv"] == 1).sum()),
                      "unconverged": int((frames["econv"] == 0).sum()),
                      "unknown": int((frames["econv"] == -1).sum()),
                      "scf_dE_abs": summary(np.abs(frames["scf_dE"].astype(float)))},
        "nonfinite_frames": {"energy": int(((fl & S.F_E_NONFINITE) > 0).sum()),
                             "forces": int(((fl & S.F_F_NONFINITE) > 0).sum()),
                             "stress": int(((fl & S.F_S_NONFINITE) > 0).sum()),
                             "positions": int(((fl & S.F_POS_NONFINITE) > 0).sum()),
                             "unreadable": int(((fl & S.F_BAD) > 0).sum())},
    }


_MP_U_ELEMENTS = frozenset({"Co", "Cr", "Fe", "Mn", "Mo", "Ni", "V", "W"})


def _formation_proxy(meta: np.ndarray, frames: np.ndarray, f_mi: np.ndarray, f_ok: np.ndarray,
                     structs: np.ndarray, formulas: list[str], mu: dict[str, float],
                     reference: bool) -> dict:
    """E/atom minus the composition-weighted MP elemental reference energy (uncorrected GGA): a
    formation-energy proxy on ONE scale for every dataset. Only frames whose energies share MP's
    GGA reference: harvested calcs that follow the MP recipe without +U (``mp == 1``, ``u == 0``);
    references in full. Compositions MP would treat with +U (O/F with Co, Cr, Fe, Mn, Mo, Ni, V,
    W) and elements without a reference are left out."""
    mu_f = np.full(max(len(formulas), 1), np.nan)
    for i, f in enumerate(formulas):
        c = formula_counts(f)
        if ("O" in c or "F" in c) and _MP_U_ELEMENTS & set(c):
            continue
        if c and all(el in mu for el in c):
            mu_f[i] = sum(mu[el] * n for el, n in c.items()) / sum(c.values())
    if len(structs) == 0 or len(frames) == 0:
        return {"frames_eligible": 0}
    sk = np.argsort(structs["calc"], kind="stable")
    sc = structs["calc"][sk]
    pos = np.clip(np.searchsorted(sc, frames["calc"]), 0, len(sc) - 1)
    ok = sc[pos] == frames["calc"]
    fcode = np.where(ok, structs["formula"][sk][pos], 0)
    sel = ok.copy()
    if not reference:
        sel &= f_ok & (meta["mp"][f_mi] == 1) & (meta["u"][f_mi] == 0)
    epa = frames["energy"].astype(float) / np.maximum(frames["natoms"], 1)
    ef = np.where(sel, epa - mu_f[fcode], np.nan)
    fin = np.isfinite(ef)
    return {"frames_eligible": int(fin.sum()), "summary": summary(ef),
            "hist": hist(ef, np.arange(-6.0, 6.01, 0.1)),
            "share_gt": {f"{t:g}eV": _share(int((ef[fin] > t).sum()), int(fin.sum()))
                         for t in (0.0, 0.1, 0.5, 1.0, 2.0)}}


def _calc_groups(frames: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Frames sorted by (calc, step): (order, group starts, group ends)."""
    if len(frames) == 0:
        z = np.zeros(0, dtype=np.int64)
        return z, z, z
    order = np.lexsort((frames["step"], frames["calc"]))
    ck = frames["calc"][order]
    starts = np.flatnonzero(np.r_[True, ck[1:] != ck[:-1]])
    ends = np.append(starts[1:], len(ck)) - 1
    return order, starts, ends


def subsample_per_calc(epa_sorted: np.ndarray, starts: np.ndarray, ends: np.ndarray,
                       thr_ev: float) -> np.ndarray:
    """Frames each calc keeps under the sAlex-style rule: in step order, keep a frame only if its
    energy per atom differs by more than ``thr_ev`` from the last KEPT frame (the first frame is
    always kept). ``epa_sorted`` is E/atom with frames sorted by (calc, step)."""
    kept = np.ones(len(starts), dtype=np.int64)
    for g in np.flatnonzero(ends > starts).tolist():
        seq = epa_sorted[starts[g]: ends[g] + 1].tolist()
        last = seq[0]
        k = 1
        for v in seq[1:]:
            if abs(v - last) > thr_ev:
                k += 1
                last = v
        kept[g] = k
    return kept


def subsample_counts(epa_sorted: np.ndarray, starts: np.ndarray, ends: np.ndarray,
                     thresholds_mev: tuple[float, ...] = (1.0, 10.0, 50.0)) -> dict:
    """Total frames kept by :func:`subsample_per_calc` at each threshold, plus endpoints-only."""
    out = {f"dE_gt_{t:g}meV_per_atom": int(subsample_per_calc(epa_sorted, starts, ends,
                                                              t / 1000.0).sum())
           for t in thresholds_mev}
    out["endpoints_only"] = int(np.where(ends > starts, 2, 1).sum())
    return out


def _redundancy(meta: np.ndarray, tabs: dict, frames: np.ndarray, structs: np.ndarray,
                s_mi: np.ndarray, s_ok: np.ndarray, mk: np.ndarray, morder: np.ndarray) -> dict:
    out: dict[str, Any] = {"frames": int(len(frames))}
    uf, inv_f, cnt_f = np.unique(frames["fhash"], return_inverse=True, return_counts=True)
    inv_f = inv_f.ravel()
    out["unique_frames_structure_plus_energy"] = int(len(uf))
    out["unique_structures"] = int(np.unique(frames["shash"]).size)
    out["duplicate_frames"] = int(len(frames) - len(uf))
    # duplicates spanning different calcs (vs a step repeated inside one calc)
    o = np.lexsort((frames["calc"], inv_f))
    gi, ck = inv_f[o], frames["calc"][o]
    first_of_pair = np.r_[True, (gi[1:] != gi[:-1]) | (ck[1:] != ck[:-1])]
    ncalc = np.bincount(gi[first_of_pair], minlength=len(uf))
    out["duplicate_groups_spanning_calcs"] = int((ncalc > 1).sum())
    out["frames_in_cross_calc_duplicate_groups"] = int(cnt_f[ncalc > 1].sum())
    order, starts, ends = _calc_groups(frames)
    if len(starts):
        fh = frames["fhash"][order]
        lens = (ends - starts + 1).astype(np.uint64)
        fp = np.stack([fh[starts], fh[ends], lens], axis=1)
        _u, inv_c, cnt_c = np.unique(fp, axis=0, return_inverse=True, return_counts=True)
        inv_c = inv_c.ravel()
        keys = frames["calc"][order][starts]
        mi, ok = _join(mk, morder, keys)
        dep = np.where(ok, meta["deposit"][mi], -1).astype(np.int64)
        og = np.argsort(inv_c, kind="stable")
        gs = inv_c[og]
        gstart = np.flatnonzero(np.r_[True, gs[1:] != gs[:-1]])
        dmin = np.minimum.reduceat(dep[og], gstart)
        dmax = np.maximum.reduceat(dep[og], gstart)
        gsize = np.diff(np.append(gstart, len(og)))
        out["exact_duplicate_calcs"] = {
            "groups": int((cnt_c > 1).sum()),
            "calcs_in_groups": int(cnt_c[cnt_c > 1].sum()),
            "redundant_calcs": int((cnt_c[cnt_c > 1] - 1).sum()),
            "groups_spanning_deposits": int(((gsize > 1) & (dmin != dmax)).sum())}
        epa = frames["energy"].astype(float) / np.maximum(frames["natoms"], 1)
        out["subsampling_effective_frames"] = subsample_counts(epa[order], starts, ends)
    st = structs[s_ok]
    if len(st):
        lab = meta["label"][s_mi[s_ok]].astype(np.int64)
        _u2, inv2, cnt2 = np.unique(st["shash"], return_inverse=True, return_counts=True)
        inv2 = inv2.ravel()
        o2 = np.lexsort((lab, inv2))
        g2, l2 = inv2[o2], lab[o2]
        firstpair = np.r_[True, (g2[1:] != g2[:-1]) | (l2[1:] != l2[:-1])]
        nlab = np.bincount(g2[firstpair], minlength=len(cnt2))
        out["shared_initial_structure"] = {
            "calcs_sharing": int((cnt2[inv2] > 1).sum()), "groups": int((cnt2 > 1).sum()),
            "groups_with_several_functional_labels": int((nlab > 1).sum()),
            "calcs_in_multi_functional_groups": int((nlab[inv2] > 1).sum())}
    return out


def _curation(meta: np.ndarray, tabs: dict, frames: np.ndarray, f_mi: np.ndarray,
              f_ok: np.ndarray, structs: np.ndarray, s_ok: np.ndarray) -> dict:
    """Frames each default training-time filter would remove (filters overlap), and what passes.

    The filters encode the issues recorded in docs/FURTHER_WORK.md B3 and the result docs: SCF not
    converged, NEB images (under VTST the stored force is the projected NEB force), non-self-
    consistent runs (ICHARG >= 10 / line-mode k-points), VASP MLFF runs, non-finite labels,
    |F - E0| > 50 meV/atom, sAlex-style extremes (E > 0, max |F| > 50 eV/A, max |stress| > 80 GPa)
    and atoms closer than 0.5 A."""
    nat = np.maximum(frames["natoms"].astype(float), 1)
    ctype = np.where(f_ok, meta["ctype"][f_mi], -1)
    fl = frames["flags"]
    bad_calcs = structs["calc"][s_ok & (structs["dmin"] < 0.5)]
    filters = {
        "scf_unconverged": frames["econv"] == 0,
        "neb_image": ctype == _code(tabs, "ctype", "neb"),
        "non_self_consistent": ctype == _code(tabs, "ctype", "nscf"),
        "vasp_mlff_run": ctype == _code(tabs, "ctype", "mlff"),
        "nonfinite_or_unreadable": (fl & (S.F_E_NONFINITE | S.F_F_NONFINITE | S.F_S_NONFINITE
                                          | S.F_POS_NONFINITE | S.F_BAD)) > 0,
        "free_minus_e0_gt_50meV_per_atom": np.abs(frames["dfree"].astype(float)) / nat > 0.05,
        "energy_positive": frames["energy"] > 0,
        "fmax_gt_50": frames["fmax"] > 50,
        "max_stress_gt_80GPa": frames["smax"] > 80,
        "atoms_closer_than_0.5A": np.isin(frames["calc"], bad_calcs),
        "no_forces": (fl & S.F_FORCES) == 0,
        "no_metadata": ~f_ok,
    }
    out: dict[str, Any] = {k: int(v.sum()) for k, v in filters.items()}
    anyf = np.zeros(len(frames), dtype=bool)
    for v in filters.values():
        anyf |= v
    out["frames"] = int(len(frames))
    out["frames_passing_all"] = int((~anyf).sum())
    out["share_passing_all"] = _share(int((~anyf).sum()), len(frames))
    return out


def _scan_integrity(fagg: dict, frames: np.ndarray, f_ok: np.ndarray, meta: np.ndarray,
                    mk: np.ndarray) -> dict:
    has = np.zeros(len(mk), dtype=bool)
    if len(mk) and f_ok.any():
        has[np.searchsorted(mk, np.unique(frames["calc"][f_ok]))] = True
    return {"shards": fagg["shards"], "truncated_shards": fagg["truncated"][:20],
            "torn_shards": fagg["torn"][:20], "bad_frames": fagg["bad_frames"],
            "fallback_runs": fagg["fallback_runs"],
            "force_nonfinite_atoms": fagg["force_nonfinite_atoms"],
            "properties_layouts": dict(Counter(fagg["props"]).most_common(10)),
            "info_keys": dict(Counter(fagg["info_keys"]).most_common(40)),
            "pbc_patterns": dict(fagg["pbc"]),
            "frames_scanned_vs_metadata": [int(len(frames)),
                                           int(meta["n_frames"].astype(np.int64).sum())],
            "calcs_with_frames_vs_metadata": [int(has.sum()), int(len(meta))],
            "scan_cpu_hours": round(fagg["scan_cpu_s"] / 3600.0, 2)}


# --------------------------------------------------------------------------------------------
def cross_source(internals: dict[str, dict], reports: dict[str, dict]) -> dict:
    """Overlap of elements, chemical systems, formulas, prototypes and identical frames between
    the harvested sources."""
    out: dict[str, Any] = {"sources": list(reports)}
    tot = {"deposits": 0, "calcs": 0, "frames": 0, "force_labels_atoms": 0}
    for r in reports.values():
        tot["deposits"] += int(r["size"]["deposits"])
        tot["calcs"] += int(r["size"]["calcs"])
        tot["frames"] += int(r["size"]["frames_metadata"])
        tot["force_labels_atoms"] += int(r["size"].get("force_labels_atoms", 0))
    out["totals"] = tot
    names = [n for n in internals if "systems" in internals[n]]
    for key in ("elements", "systems", "formulas", "prototypes"):
        sets = {n: internals[n][key] for n in names}
        if not sets:
            continue
        union = set().union(*sets.values())
        out[key] = {"union": len(union), "per_source": {n: len(v) for n, v in sets.items()},
                    "unique_to_source": {n: len(v - set().union(*(sets[m] for m in names
                                                                   if m != n)))
                                         for n, v in sets.items()},
                    "pairwise_intersection": {f"{a}&{b}": len(sets[a] & sets[b])
                                              for i, a in enumerate(names)
                                              for b in names[i + 1:]}}
    for key in ("fhash", "shash"):
        out[f"shared_{key}"] = {f"{a}&{b}": int(np.intersect1d(internals[a][key],
                                                                internals[b][key],
                                                                assume_unique=True).size)
                                for i, a in enumerate(names) for b in names[i + 1:]}
    return out


def _novel_masks(internal: dict, ref: dict) -> dict[str, np.ndarray]:
    """Per-calc booleans: does the calc's element set / chemical system / formula / prototype lie
    OUTSIDE the reference's set?"""
    els = ref["elements"]
    f_new_el = np.array([any(e not in els for e in formula_counts(f))
                         for f in internal["formula_names"]] or [False])
    s_new = np.array([s not in ref["systems"] for s in internal["system_names"]] or [False])
    f_new = np.array([f not in ref["formulas"] for f in internal["formula_names"]] or [False])
    p_new = np.array([bool(p) and p not in ref["prototypes"] for p in internal["proto_names"]]
                     or [False])
    cf, cs, cp = internal["calc_formula"], internal["calc_system"], internal["calc_proto"]
    return {"element": f_new_el[cf] if len(cf) else np.zeros(0, bool),
            "chemical_system": s_new[cs] if len(cs) else np.zeros(0, bool),
            "formula": f_new[cf] if len(cf) else np.zeros(0, bool),
            "prototype": (p_new[cp] & internal["calc_has_proto"]) if len(cp)
            else np.zeros(0, bool)}


def novelty(internals: dict[str, dict], refs: dict[str, dict],
            subsets: dict[str, dict] | None = None) -> dict:
    """For every harvested source (and all together) against every reference (and the union of
    the material universes MP + Alexandria): the share of calcs / frames / effective frames
    (sAlex 10 meV rule) / deposits whose element set, chemical system, reduced formula or bulk
    prototype (formula x space group, non-P1) is absent from the reference."""
    out: dict[str, Any] = {}
    ref_sets = {n: r for n, r in refs.items() if "systems" in r}
    universe = [n for n in ("mp_materials", "alexandria_pbe") if n in ref_sets]
    if len(universe) == 2:
        ref_sets["mp+alexandria"] = {k: ref_sets[universe[0]][k] | ref_sets[universe[1]][k]
                                     for k in ("elements", "systems", "formulas", "prototypes")}
    for rname, ref in ref_sets.items():
        per_src: dict[str, Any] = {}
        totals: dict[str, dict[str, float]] = {}
        distinct_all: dict[str, set] = defaultdict(set)
        for sname, internal in list(internals.items()) + list((subsets or {}).items()):
            if "calc_formula" not in internal:
                continue
            is_subset = sname not in internals
            masks = _novel_masks(internal, ref)
            fr, ef, dep = internal["calc_frames"], internal["calc_eff"], internal["calc_dep"]
            row: dict[str, Any] = {}
            for level, m in masks.items():
                denom_c = len(m) if level != "prototype" else int(internal["calc_has_proto"].sum())
                base = internal["calc_has_proto"] if level == "prototype" else np.ones(len(m), bool)
                row[level] = {
                    "calcs": int(m.sum()), "calcs_share": _share(int(m.sum()), denom_c),
                    "frames": int(fr[m].sum()), "frames_share": _share(fr[m].sum(), fr[base].sum()),
                    "effective_frames": int(ef[m].sum()),
                    "effective_frames_share": _share(ef[m].sum(), ef[base].sum()),
                    "deposits_with_any": int(np.unique(dep[m]).size),
                    "deposits_share": _share(int(np.unique(dep[m]).size),
                                             int(np.unique(dep[base]).size))}
                if is_subset:
                    continue
                t = totals.setdefault(level, {"calcs": 0, "frames": 0, "eff": 0, "deps": 0,
                                              "c0": 0, "f0": 0, "e0": 0, "d0": 0})
                t["calcs"] += int(m.sum())
                t["frames"] += float(fr[m].sum())
                t["eff"] += float(ef[m].sum())
                t["deps"] += int(np.unique(dep[m]).size)
                t["c0"] += denom_c
                t["f0"] += float(fr[base].sum())
                t["e0"] += float(ef[base].sum())
                t["d0"] += int(np.unique(dep[base]).size)
            for key, names_key in (("systems", "system_names"), ("formulas", "formula_names"),
                                   ("prototypes", "proto_names")):
                own = internal[key]
                row[f"distinct_{key}_absent"] = len(own - ref[key])
                row[f"distinct_{key}"] = len(own)
                if not is_subset:
                    distinct_all[key] |= own
            row["distinct_elements_absent"] = sorted(internal["elements"] - ref["elements"])
            per_src[sname] = row
        combined: dict[str, Any] = {level: {"calcs_share": _share(t["calcs"], t["c0"]),
                            "frames_share": _share(t["frames"], t["f0"]),
                            "effective_frames_share": _share(t["eff"], t["e0"]),
                            "deposits_share": _share(t["deps"], t["d0"]),
                            "calcs": t["calcs"], "frames": int(t["frames"]),
                            "effective_frames": int(t["eff"]), "deposits": t["deps"]}
                    for level, t in totals.items()}
        for key in ("systems", "formulas", "prototypes"):
            combined[f"distinct_{key}_absent"] = len(distinct_all[key] - ref[key])
            combined[f"distinct_{key}"] = len(distinct_all[key])
        out[rname] = {"per_source": per_src, "all_sources": combined,
                      "reference_sizes": {k: len(ref[k]) for k in ("elements", "systems",
                                                                   "formulas", "prototypes")}}
    return out


def build_report(stats_root: str | Path, sources: list[str],
                 dataset_dirs: dict[str, Path] | None = None,
                 references: list[str] | None = None,
                 refs_root: str | Path | None = None) -> tuple[dict, dict]:
    """(report, internals) over ``<stats_root>/<source>/{meta,scan}`` for each harvested source
    and ``<stats_root>/ref_<name>/scan`` for each reference. MP's elemental reference energies,
    if downloaded under ``refs_root``, add the formation-energy proxy to every label section."""
    from .reference import load_elemental_refs
    stats_root = Path(stats_root)
    mu = load_elemental_refs(Path(refs_root) if refs_root else stats_root / "refs") or None
    reports: dict[str, dict] = {}
    internals: dict[str, dict] = {}
    sub_reports: dict[str, dict] = {}
    sub_internals: dict[str, dict] = {}
    for src in sources:
        base = stats_root / src
        if not (base / "meta").is_dir():
            logger.warning("no metadata pass for %s under %s — skipped", src, base)
            continue
        r, internal, subs = source_report(src, base / "meta", base / "scan",
                                          (dataset_dirs or {}).get(src), mu=mu)
        reports[src] = r
        internals[src] = internal
        for sname, (sr, si) in subs.items():
            sub_reports[sname] = sr
            sub_internals[sname] = si
    ref_reports: dict[str, dict] = {}
    ref_internals: dict[str, dict] = {}
    for ref in references or []:
        sdir = stats_root / f"ref_{ref}" / "scan"
        if not any(sdir.glob("shard-*.npz")):
            logger.warning("reference %s not scanned (%s) — skipped", ref, sdir)
            continue
        r, internal = reference_report(ref, sdir, mu=mu)
        ref_reports[ref] = r
        ref_internals[ref] = internal
    report = {"sources": reports, "subsets": sub_reports, "references": ref_reports,
              "cross_source": cross_source(internals, reports),
              "novelty_vs_references": novelty(internals, ref_internals, sub_internals),
              "vacuum_gap_threshold_A": VACUUM_GAP_A,
              "elemental_references": {"source": "MP 2023-02-07 elemental entries",
                                       "elements": len(mu or {})}}
    return report, {"sources": internals, "subsets": sub_internals, "references": ref_internals}
