"""Markdown rendering of :func:`dataset_stats.report.build_report` output."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

SOURCE_NAMES = {"zenodo": "Zenodo", "nomad": "NOMAD", "materials_cloud": "Materials Cloud",
                "mptrj": "MPtrj", "omat24_val": "OMat24 (val)", "salex_val": "sAlex (val)",
                "mp_materials": "MP materials", "alexandria_pbe": "Alexandria PBE",
                "mp+alexandria": "MP ∪ Alexandria"}


def _n(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        if v != v:  # NaN
            return "—"
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:.3g}" if abs(v) < 100 else f"{v:.0f}"
        return f"{v:.3g}"
    return str(v)


def _pct(v: Any) -> str:
    return "—" if v is None else f"{100 * float(v):.1f}%"


def _table(rows: list[dict], cols: list[tuple[str, str]], limit: int = 25) -> str:
    if not rows:
        return "_(none)_\n"
    head = "| " + " | ".join(h for _, h in cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    body = []
    for r in rows[:limit]:
        cells = []
        for key, _h in cols:
            v = r.get(key)
            cells.append(_pct(v) if key.endswith("_share") else str(v) if key == "year"
                         else _n(v))
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([head, sep, *body]) + "\n"


_CF = [("value", "value"), ("calcs", "calcs"), ("calcs_share", "% calcs"),
       ("frames", "frames"), ("frames_share", "% frames")]


def _q(s: dict | None, keys: tuple[str, ...] = ("min", "p5", "p50", "p95", "max"),
       unit: str = "") -> str:
    if not s or not s.get("n"):
        return "—"
    parts = [f"{k} {_n(s.get(k))}" for k in keys if s.get(k) is not None]
    return f"{', '.join(parts)}{(' ' + unit) if unit else ''} (n={s['n']:,})"


def _ordered(rep: dict) -> dict[str, dict]:
    """Sources, each followed by its origin subsets (e.g. NOMAD[long-tail])."""
    out: dict[str, dict] = {}
    subs = rep.get("subsets", {})
    for n, r in rep["sources"].items():
        out[n] = r
        for sn, sr in subs.items():
            if sn.startswith(f"{n}["):
                out[sn] = sr
    return out


def _label(n: str) -> str:
    base, _, rest = n.partition("[")
    return SOURCE_NAMES.get(base, base) + (f" [{rest}" if rest else "")


def _glance(rep: dict) -> str:
    srcs = _ordered(rep)
    names = list(srcs)
    base_names = list(rep["sources"])
    rows: list[tuple[str, list[Any]]] = []

    def g(path: list[str], src: dict) -> Any:
        cur: Any = src
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return None
            cur = cur[p]
        return cur

    scanned = any(_get(r, "size", "frames_scanned") for r in srcs.values())
    spec = [
        ("deposits (records / uploads)", ["size", "deposits"]),
        ("calculations", ["size", "calcs"]),
        ("frames (ionic steps)", ["size", "frames_metadata"]),
        ("atoms with a force label",
         ["size", "force_labels_atoms" if scanned else "metadata_force_labels_atoms"]),
        ("frames with stress",
         ["size", "frames_with_stress" if scanned else "metadata_frames_with_stress"]),
        ("elements", ["chemistry", "n_elements"]),
        ("reduced formulas", ["chemistry", "n_reduced_formulas"]),
        ("chemical systems", ["chemistry", "n_chemical_systems"]),
        ("bulk prototypes (formula x space group)", ["structure", "bulk_prototypes_formula_x_spg"]),
        ("unique structures", ["redundancy", "unique_structures"]),
        ("exact duplicate frames", ["redundancy", "duplicate_frames"]),
        ("frames after dE > 10 meV/atom subsampling",
         ["redundancy", "subsampling_effective_frames", "dE_gt_10meV_per_atom"]),
        ("frames passing default curation filters", ["curation", "frames_passing_all"]),
        ("frames MP-compatible (GGA/GGA+U recipe)",
         ["compatibility", "materials_project_gga_u", "frames"]),
        ("distinct POTCAR sets", ["settings", "potcar_sets", "distinct"]),
        ("distinct creators / authors", ["provenance", "distinct_creator_names"]),
    ]
    for label, path in spec:
        vals = [g(path, srcs[n]) for n in names]
        rows.append((label, vals))
    head = "| | " + " | ".join(_label(n) for n in names) + " | all |"
    sep = "|---|" + "|".join("---:" for _ in names) + "|---:|"
    lines = [head, sep]
    additive = {"deposits (records / uploads)", "calculations", "frames (ionic steps)",
                "atoms with a force label", "frames with stress", "exact duplicate frames",
                "frames after dE > 10 meV/atom subsampling",
                "frames passing default curation filters",
                "frames MP-compatible (GGA/GGA+U recipe)"}
    cs = rep.get("cross_source", {})
    union = {"elements": (cs.get("elements") or {}).get("union"),
             "reduced formulas": (cs.get("formulas") or {}).get("union"),
             "chemical systems": (cs.get("systems") or {}).get("union")}
    for label, vals in rows:
        base_vals = [v for n, v in zip(names, vals) if n in base_names]
        if label in additive and all(isinstance(v, int) for v in base_vals):
            tot: Any = sum(base_vals)
        else:
            tot = union.get(label)
        lines.append(f"| {label} | " + " | ".join(_n(v) for v in vals) + f" | {_n(tot)} |")
    return "\n".join(lines) + "\n"


def _source_md(r: dict) -> str:
    name = _label(r["source"])
    out = [f"## {name}\n"]
    sz = r["size"]
    scanned = bool(sz.get("frames_scanned"))
    fl = sz.get("force_labels_atoms") if scanned else sz.get("metadata_force_labels_atoms")
    fs = sz.get("frames_with_stress") if scanned else sz.get("metadata_frames_with_stress")
    out.append(f"- **{sz['deposits']:,} deposits, {sz['calcs']:,} calcs, "
               f"{sz['frames_metadata']:,} frames** "
               f"({(fl or 0):,} atoms with force labels; {(fs or 0):,} frames with stress).")
    out.append(f"- Frames per calc: {_q(sz['frames_per_calc'])}.")
    if sz.get("atoms_per_frame"):
        out.append(f"- Atoms per frame: {_q(sz['atoms_per_frame'])}.")
    if sz.get("dataset_shard_bytes"):
        out.append(f"- On disk: {sz['dataset_shard_bytes'] / 2**30:.1f} GiB of shards "
                   f"({sz['shards']:,}), metadata {sz.get('metadata_bytes', 0) / 2**30:.1f} GiB.")
    c = r["concentration"]
    fo = c["frames_over_deposits"]
    out.append(f"- Concentration: top-1 deposit {_pct(fo.get('top1_share'))} of frames, top-10 "
               f"{_pct(fo.get('top10_share'))}; Gini {fo.get('gini')}; effective number of "
               f"deposits (1/HHI) {fo.get('effective_n_inverse_hhi')} of {fo.get('groups')}.\n")
    out.append("### Largest deposits (by frames)\n")
    out.append(_table(c["top_deposits_by_frames"],
                      [("deposit", "deposit"), ("title", "title"), ("frames", "frames"),
                       ("frames_share", "% frames"), ("calcs", "calcs"), ("year", "year"),
                       ("license", "licence")], limit=12))
    if "chemistry" in r:
        ch = r["chemistry"]
        out.append("### Chemistry\n")
        out.append(f"{ch['n_elements']} elements, {ch['n_reduced_formulas']:,} reduced formulas, "
                   f"{ch['n_chemical_systems']:,} chemical systems, "
                   f"{ch['n_element_pairs_cooccurring']:,} element pairs co-occurring.\n")
        out.append(_table(ch["n_ary"], _CF))
        els = sorted(ch["elements"].items(), key=lambda kv: -kv[1]["frames"])
        rows = [{"value": el, **v} for el, v in els]
        out.append("\nElements (by frames containing them):\n")
        out.append(_table(rows, [("value", "element"), ("frames", "frames"), ("calcs", "calcs"),
                                 ("deposits", "deposits"), ("chemical_systems", "systems"),
                                 ("atoms", "atoms x frames")], limit=120))
        out.append("\nTop formulas by calcs:\n")
        out.append(_table(ch["top_formulas_by_calcs"], _CF, limit=20))
        mm = ch.get("structure_vs_potcar_elements")
        if mm:
            out.append(f"\nStructure vs POTCAR element sets: {mm['calcs_mismatched']:,} of "
                       f"{mm['calcs_compared']:,} calcs differ "
                       f"(e.g. {mm['examples_potcar_vs_structure'][:3]}).\n")
    if "structure" in r:
        s = r["structure"]
        out.append("### Structure types\n")
        out.append(_table(s["natoms_bins"], [("value", "atoms per cell"), *_CF[1:]]))
        out.append(f"\nAtoms per frame: {_q(s['natoms_frames'])}.\n")
        out.append("\nDimensionality from vacuum gaps (6 Å threshold):\n")
        out.append(_table(s["dimensionality"]["threshold_6A"], _CF))
        out.append(f"\nSlab vacuum width: {_q(s['slab_vacuum_width_A'], unit='Å')}. "
                   f"Bulk volume/atom: {_q(s['volume_per_atom_bulk_frames'], unit='Å³')}. "
                   f"Shortest interatomic distance (first frame): "
                   f"{_q(s['min_distance_calcs'], unit='Å')}; "
                   f"{s['min_distance_counts']}.\n")
        out.append(f"\nBulk: {s['bulk_space_groups_distinct']} space groups, "
                   f"{s['bulk_prototypes_formula_x_spg']:,} formula x space-group prototypes, "
                   f"P1 share {_pct(s['bulk_p1_share_calcs'])}.\n")
        out.append(_table(s["bulk_crystal_systems"], _CF))
    if "labels" in r:
        lb = r["labels"]
        out.append("### Labels\n")
        out.append(f"- Max force per frame: {_q(lb['fmax'], unit='eV/Å')}.")
        sb = lb["fmax_share_below"]
        out.append("- Share of frames with max |F| below: "
                   + ", ".join(f"{k} eV/Å {_pct(v)}" for k, v in sb.items()) + ".")
        pa = lb["per_atom_force"]
        out.append(f"- Per-atom |F| ({pa['atoms']:,} atoms): "
                   + ", ".join(f"{k} {_n(v)}" for k, v in pa["quantiles"].items()) + " eV/Å.")
        out.append(f"- Pressure: {_q(lb['pressure_gpa'], unit='GPa')}; max |stress| > 10 GPa in "
                   f"{_pct(lb['max_abs_stress_share_gt'].get('10'))} of frames with stress.")
        out.append(f"- |E_free − E0|/atom: {_q(lb['free_minus_e0_per_atom_abs'], unit='eV')}.")
        out.append(f"- Energy/atom: {_q(lb['energy_per_atom'], unit='eV')}; "
                   f"{lb['energy_positive_frames']:,} frames with E > 0.")
        out.append(f"- Frame SCF: {lb['frame_scf']['converged']:,} converged, "
                   f"{lb['frame_scf']['unconverged']:,} unconverged, "
                   f"{lb['frame_scf']['unknown']:,} unknown. Non-finite: {lb['nonfinite_frames']}.\n")
        rows = [{"value": k, "frames": v["frames"], "p50": v["fmax"].get("p50"),
                 "p95": v["fmax"].get("p95"), "fnet": v["fnet_per_atom"].get("p50")}
                for k, v in lb["fmax_by_calc_type"].items()]
        out.append(_table(rows, [("value", "calc type"), ("frames", "frames"),
                                 ("p50", "median max|F|"), ("p95", "p95 max|F|"),
                                 ("fnet", "median |ΣF|/atom")]))
        rows = [{"value": k, "frames": v["frames"], "p5": v["summary"].get("p5"),
                 "p50": v["summary"].get("p50"), "p95": v["summary"].get("p95")}
                for k, v in lb["energy_per_atom_by_family"].items()]
        out.append("\nEnergy per atom by XC family (eV/atom):\n")
        out.append(_table(rows, [("value", "family"), ("frames", "frames"), ("p5", "p5"),
                                 ("p50", "median"), ("p95", "p95")]))
    e = r["electronic"]
    out.append("### Net magnetic moment and charge\n")
    out.append(f"- Net moment known for {e['net_moment_known']['calcs']:,} calcs; |m| > 0.5 μB in "
               f"{e['net_moment_abs_gt_0.5']['calcs']:,} calcs "
               f"({e['net_moment_abs_gt_0.5']['frames']:,} frames).")
    out.append(f"- Net charge known for {e['net_charge_known']['calcs']:,} calcs; non-zero in "
               f"{e['net_charge_nonzero']['calcs']:,} calcs "
               f"({e['net_charge_nonzero']['frames']:,} frames).\n")
    st = r["settings"]
    out.append("### Calculation settings\n")
    for key, title in (("ctype", "calculation type"), ("family", "XC family"),
                       ("label", "XC + U + dispersion (top 20)"), ("version", "VASP version"),
                       ("prec", "PREC"), ("parser", "parser")):
        out.append(f"\n{title}:\n")
        out.append(_table(st[key], _CF, limit=20))
    out.append(f"\n+U: {st['hubbard_u']['calcs']:,} calcs / {st['hubbard_u']['frames']:,} frames. "
               f"ENCUT (frames): {_q(st['encut']['summary_frames'], unit='eV')}. "
               f"KPPRA (calcs): {_q(st['kppra']['summary_calcs'])}. ")
    if "kspacing_effective_bulk" in st:
        out.append(f"Effective k-spacing of bulk cells (2π/Å): "
                   f"{_q(st['kspacing_effective_bulk']['summary_calcs'])}. ")
    out.append(f"Distinct POTCAR sets: {st['potcar_sets']['distinct']:,}.\n")
    md = st["md"]
    out.append(f"\nMD: {md['calcs']:,} calcs / {md['frames']:,} frames; TEBEG (frames) "
               f"{_q(md['tebeg_frames'], unit='K')}.\n")
    out.append("\nSmearing (top):\n")
    out.append(_table(st["smearing"], _CF, limit=10))
    out.append("\nPOTCAR releases containing every titel of a calc:\n")
    out.append(_table([{"value": k, **v} for k, v in
                       st["potcar_releases_containing_all_titels"].items()],
                      [("value", "release"), ("calcs", "calcs"), ("frames", "frames")]))
    q = r["quality"]
    out.append("### Quality, compatibility, curation\n")
    out.append(f"- SCF-unconverged frames: {q['frames_scf_unconverged']:,}; frames without "
               f"forces: {q['frames_without_forces']:,}; dropped (no energy): "
               f"{q['frames_dropped_no_energy']:,}.")
    cp = r["compatibility"]
    for k, v in cp.items():
        out.append(f"- {k}: {v['calcs']:,} calcs / {v['frames']:,} frames / {v['deposits']:,} "
                   f"deposits (ENCUT ≥ 520 eV: {v['frames_encut_ge_520']:,} frames).")
    if "curation" in r:
        cu = r["curation"]
        out.append("\nDefault training-time filters (frames removed; filters overlap):\n")
        rows = [{"value": k, "frames": v} for k, v in cu.items()
                if k not in ("frames", "frames_passing_all", "share_passing_all")]
        out.append(_table(rows, [("value", "filter"), ("frames", "frames")]))
        out.append(f"\nPassing all filters: {cu['frames_passing_all']:,} of {cu['frames']:,} "
                   f"frames ({_pct(cu['share_passing_all'])}).\n")
    av = r["availability"]
    out.append("### Availability of heavy outputs (recorded, not stored)\n")
    out.append(_table([{"value": k, **v} for k, v in av.items() if "calcs_share" in v],
                      [("value", "output"), ("calcs", "calcs"), ("calcs_share", "% calcs"),
                       ("frames", "frames"), ("deposits", "deposits")]))
    pv = r["provenance"]
    out.append("### Provenance\n")
    out.append(f"{pv['deposits']:,} deposits, {pv['distinct_creator_names']:,} distinct creator "
               f"names, {pv['deposits_with_doi']:,} with a DOI. Deposits by year: "
               f"{pv['deposits_by_year']}.\n")
    fa = pv.get("frames_over_first_authors")
    if fa:
        out.append(f"First authors (independent groups proxy): {pv['first_authors']:,}; top-1 holds "
                   f"{_pct(fa.get('top1_share'))} of frames, top-10 {_pct(fa.get('top10_share'))}; "
                   f"effective number (1/HHI) {fa.get('effective_n_inverse_hhi')}.\n")
        out.append(_table(pv["top_first_authors"], [("value", "first author"),
                                                     ("frames", "frames"),
                                                     ("frames_share", "% frames"),
                                                     ("calcs", "calcs"),
                                                     ("calcs_share", "% calcs")], limit=10))
    out.append(_table(pv["license"], _CF, limit=12))
    if "redundancy" in r:
        rd = r["redundancy"]
        out.append("### Redundancy and effective size\n")
        out.append(f"- {rd['unique_structures']:,} unique structures and "
                   f"{rd['unique_frames_structure_plus_energy']:,} unique (structure, energy) "
                   f"frames of {rd['frames']:,}; {rd['duplicate_frames']:,} exact duplicate frames "
                   f"({rd['frames_in_cross_calc_duplicate_groups']:,} frames in groups spanning "
                   f"several calcs).")
        if "exact_duplicate_calcs" in rd:
            dc = rd["exact_duplicate_calcs"]
            out.append(f"- Exact duplicate calcs: {dc['redundant_calcs']:,} redundant in "
                       f"{dc['groups']:,} groups ({dc['groups_spanning_deposits']:,} groups span "
                       f"deposits).")
            ss = rd["subsampling_effective_frames"]
            out.append("- Effective frames after per-trajectory subsampling: "
                       + ", ".join(f"{k}: {v:,}" for k, v in ss.items()) + ".")
        if "shared_initial_structure" in rd:
            si = rd["shared_initial_structure"]
            out.append(f"- Calcs starting from an identical structure: {si['calcs_sharing']:,} in "
                       f"{si['groups']:,} groups; {si['groups_with_several_functional_labels']:,} "
                       f"groups mix XC labels (same structure, several functionals).\n")
    b = r["buckets"]
    out.append("### Consistency buckets (XC label x POTCAR release family)\n")
    out.append(f"{b['n_label_x_potcar_class']:,} buckets; the largest holds "
               f"{_pct(b['largest_bucket_frames_share'])} of frames.\n")
    out.append(_table(b["label_x_potcar_class"], _CF, limit=15))
    return "\n".join(out) + "\n"


def _cross_md(cs: dict) -> str:
    out = ["## Across sources\n"]
    t = cs.get("totals", {})
    out.append(f"Totals: {t.get('deposits', 0):,} deposits, {t.get('calcs', 0):,} calcs, "
               f"{t.get('frames', 0):,} frames, {t.get('force_labels_atoms', 0):,} atoms with "
               f"force labels.\n")
    for key, title in (("elements", "Elements"), ("systems", "Chemical systems"),
                       ("formulas", "Reduced formulas")):
        if key in cs:
            v = cs[key]
            out.append(f"- {title}: union {v['union']:,}; per source {v['per_source']}; unique to "
                       f"one source {v['unique_to_source']}; pairwise shared "
                       f"{v['pairwise_intersection']}.")
    for key, title in (("shared_fhash", "identical frames (structure + energy)"),
                       ("shared_shash", "identical structures")):
        if cs.get(key):
            out.append(f"- Shared {title}: {cs[key]}.")
    return "\n".join(out) + "\n"


def _get(d: Any, *path: str) -> Any:
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return None
        d = d[p]
    return d


def _dim_share(r: dict, label: str) -> Any:
    rows = _get(r, "structure", "dimensionality", "threshold_6A") or []
    for row in rows:
        if row["value"] == label:
            return row["frames_share"]
    return 0.0 if rows else None


def _comparison_md(rep: dict) -> str:
    """Side-by-side table: every harvested source next to every reference, same definitions."""
    cols = list(_ordered(rep).items()) + list(rep.get("references", {}).items())
    if not cols:
        return ""
    spec: list[tuple[str, Any]] = [
        ("frames", lambda r: _get(r, "size", "frames_scanned")),
        ("calcs / trajectories", lambda r: _get(r, "size", "calcs")),
        ("deposits / materials", lambda r: _get(r, "size", "deposits")),
        ("atoms with force labels", lambda r: _get(r, "size", "force_labels_atoms")),
        ("elements", lambda r: _get(r, "chemistry", "n_elements")),
        ("chemical systems", lambda r: _get(r, "chemistry", "n_chemical_systems")),
        ("reduced formulas", lambda r: _get(r, "chemistry", "n_reduced_formulas")),
        ("bulk prototypes (non-P1)",
         lambda r: _get(r, "structure", "bulk_prototypes_formula_x_spg")),
        ("median atoms / frame", lambda r: _get(r, "structure", "natoms_frames", "p50")),
        ("p95 atoms / frame", lambda r: _get(r, "structure", "natoms_frames", "p95")),
        ("% frames bulk", lambda r: _dim_share(r, "bulk (no vacuum)")),
        ("% frames slab / 2D", lambda r: _dim_share(r, "slab / 2D (1 vacuum axis)")),
        ("% frames molecule / cluster", lambda r: _dim_share(r, "molecule / cluster (3 axes)")),
        ("median max|F| (eV/Å)", lambda r: _get(r, "labels", "fmax", "p50")),
        ("p95 max|F| (eV/Å)", lambda r: _get(r, "labels", "fmax", "p95")),
        ("% frames max|F| < 0.05", lambda r: _get(r, "labels", "fmax_share_below", "0.05")),
        ("% frames max|F| > 1 eV/Å",
         lambda r: (None if _get(r, "labels", "fmax_share_below", "1") is None
                    else 1 - _get(r, "labels", "fmax_share_below", "1"))),
        ("median per-atom |F| (eV/Å)",
         lambda r: _get(r, "labels", "per_atom_force", "quantiles", "p50")),
        ("median pressure (GPa)", lambda r: _get(r, "labels", "pressure_gpa", "p50")),
        ("% frames max|σ| > 10 GPa", lambda r: _get(r, "labels", "max_abs_stress_share_gt", "10")),
        ("formation-energy proxy: median (eV/atom)",
         lambda r: _get(r, "labels", "formation_energy_vs_mp_elements", "summary", "p50")),
        ("% eligible frames E_f > 0.5 eV/atom",
         lambda r: _get(r, "labels", "formation_energy_vs_mp_elements", "share_gt", "0.5eV")),
        ("unique structures", lambda r: _get(r, "redundancy", "unique_structures")),
        ("frames after dE>10 meV/atom",
         lambda r: _get(r, "redundancy", "subsampling_effective_frames", "dE_gt_10meV_per_atom")),
    ]
    head = "| | " + " | ".join(_label(n) for n, _ in cols) + " |"
    sep = "|---|" + "|".join("---:" for _ in cols) + "|"
    lines = [head, sep]
    for label, fn in spec:
        vals = []
        for _name, r in cols:
            v = fn(r)
            vals.append(_pct(v) if label.startswith("%") and v is not None else _n(v))
        lines.append(f"| {label} | " + " | ".join(vals) + " |")
    return ("## Side by side with the reference datasets\n\nSame code and definitions for every "
            "column (reference columns from their own files; OMat24/sAlex are their published "
            "validation splits, i.e. random samples; MP/Alexandria materials are relaxed "
            "structures without forces).\n\n" + "\n".join(lines) + "\n")


def _novelty_md(nov: dict) -> str:
    if not nov:
        return ""
    out = ["## What the harvested data adds beyond the reference datasets\n",
           "Share of each source's calcs / frames / effective frames (sAlex rule) / deposits "
           "whose chemistry lies OUTSIDE the reference: element set (any element absent), "
           "chemical system, reduced formula, or bulk prototype (formula × space group, "
           "non-P1 bulk cells only).\n"]
    for rname, block in nov.items():
        out.append(f"### vs {SOURCE_NAMES.get(rname, rname)} "
                   f"(reference: {block['reference_sizes']})\n")
        rows = []
        for sname, row in list(block["per_source"].items()) + [("all", block["all_sources"])]:
            for level in ("element", "chemical_system", "formula", "prototype"):
                v = row.get(level)
                if not v:
                    continue
                rows.append({"value": f"{_label(sname)} — {level}",
                             "calcs": v["calcs"], "calcs_share": v["calcs_share"],
                             "frames": v["frames"], "frames_share": v["frames_share"],
                             "eff": v["effective_frames"],
                             "effective_frames_share": v["effective_frames_share"],
                             "deposits_share": v["deposits_share"]})
        out.append(_table(rows, [("value", "source — level"), ("calcs", "calcs"),
                                 ("calcs_share", "% calcs"), ("frames_share", "% frames"),
                                 ("effective_frames_share", "% eff. frames"),
                                 ("deposits_share", "% deposits with any")], limit=40))
        al = block["all_sources"]
        out.append(f"\nDistinct items absent from the reference (all sources): chemical systems "
                   f"{al.get('distinct_systems_absent', 0):,} of {al.get('distinct_systems', 0):,}, "
                   f"formulas {al.get('distinct_formulas_absent', 0):,} of "
                   f"{al.get('distinct_formulas', 0):,}, prototypes "
                   f"{al.get('distinct_prototypes_absent', 0):,} of "
                   f"{al.get('distinct_prototypes', 0):,}.\n")
    return "\n".join(out) + "\n"


def render_markdown(rep: dict) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    parts = [f"# Dataset statistics\n\nGenerated {stamp} by `python -m dataset_stats.cli "
             f"report`. Weights: *calcs* count each calculation once; *frames* weight it by its "
             f"stored ionic steps. Vacuum threshold {rep.get('vacuum_gap_threshold_A')} Å.\n"]
    excl = {n: r["filter"] for n, r in rep["sources"].items()
            if (r.get("filter") or {}).get("individual_only")}
    if excl:
        dropped = ", ".join(f"{_label(n)}: {f['calcs_excluded']:,} calcs / "
                            f"{f['frames_excluded']:,} frames"
                            for n, f in excl.items() if f["calcs_excluded"])
        parts.append("**Individual uploads only.** Calcs of institutional high-throughput origin "
                     "(the Alexandria group's runs inside NOMAD's direct uploads, identified by "
                     "Alexandria ids / naming in their paths) are excluded"
                     + (f" ({dropped})" if dropped else "") + ".\n")
    parts += ["## At a glance\n", _glance(rep)]
    parts.append(_comparison_md(rep))
    parts.append(_novelty_md(rep.get("novelty_vs_references", {})))
    for r in _ordered(rep).values():
        parts.append(_source_md(r))
    parts.append(_cross_md(rep.get("cross_source", {})))
    return "\n".join(parts)
