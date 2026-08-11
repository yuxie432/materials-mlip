# Zenodo harvest — dataset evaluation (Aug 2026)

Quality evaluation of the completed first Zenodo harvest, complementing
`docs/HARVEST_RESULT.md` (the result summary). This document assesses **completeness,
accuracy, and coverage** of the assembled dataset + metadata, proves the pipeline funnels
reconcile exactly, and scopes the deferred recovery work.

**Inputs used** (all offline, no shards touched):
- `verify` integrity + curation JSON (job `zh-verify-33447316`, exit 0)
- `dataset/metadata.jsonl` (176,739 calc records, streamed once)
- `dataset/rejections.jsonl` (parse stage) + `manifests/rejections.jsonl` (fetch stage)
- `sacct` job history (`logs/sacct_harvest.txt`), the `logs/*.err|out`, and the session
  memories ([[zenodo-harvest-stall-debug-log]], [[zenodo-harvest-finalization-log]],
  [[zenodo-harvest-run-findings]]) + the user's dated run notes.

Reproduce with `scripts/estimate/*` and the two audit scripts noted at the end.

---

## 1. Verdict / scorecard

| axis | result |
|---|---|
| **Storage integrity** | ✅ perfect metadata↔shard bijection (11,870,529 frames, 0 missing/dup/orphan/truncated) |
| **Pipeline completeness** | ✅ both record and calc funnels reconcile to the unit (proof §3) — no unaccounted data |
| **Field completeness** | 🟡 forces 100%, stress 41%; run_type/functional null on the 25.6% parsed from OUTCAR-only |
| **Label quality** | ✅ SCF-unconverged 0.03%, dropped-no-energy 0, |F−E0|>0.05 eV/atom only 0.05% |
| **Provenance/licensing** | ✅ 293/293 records carry DOI + license; 98.3% `cc-by-4.0`, 100% open |
| **Coverage / diversity** | 🔴 severe frame concentration — 1 record = 47%, top-10 = 83%; deep but narrow |
| **Data-value accuracy (shard-level)** | ⏳ not yet run — physical sanity sweep over shards is the outstanding check (§8) |

**One-line assessment:** the harvest is *provably complete and internally consistent*; the
only real quality caveat is **diversity**, not correctness. Data-value spot-checks over the
actual shards remain to be run on CSD3.

---

## 2. Final dataset

293 records · 176,739 calc-units · **11,870,529 frames** · 1,212 shards · 39.5 GiB.
Parser mix (frame-weighted): `pymatgen.Vasprun` 73.60%, `ase.OUTCAR` 25.62%,
`pymatgen.Vaspout` 0.78% (**92,457 frames — recovered by the h5py fix**; was ~0 before).

---

## 3. Funnel reconciliation (completeness backbone)

Both funnels close **exactly** — every unit is either stored or has a logged terminal reason.
This is the strongest completeness statement available: nothing silently vanished.

**Record funnel** (keep-list → outcome):
```
keep-list                 1,352
  = fetched (≥1 VASP calc)  311   ─┬─ in dataset            293
  + fetch-rejected        1,041    └─ all calc-units failed  18
  (untouched                  0)
fetch-rejected 1,041 = no_vasp_files_fetched 810
                     + no_calc_units_after_extract 230
                     + manually_excluded 1   (record 18012696)
```

**Calc-unit funnel** (fetched → stored):
```
fetched calc-units      195,233
  = in dataset          176,739
  + parse-rejected       18,494   (distinct calc_ids)
parse-rejected 18,494 = outcar_parse_error 17,037
                      + no_frames             779
                      + vasprun_parse_error   665
                      + primary_too_large      88
                      + parse_timeout          14
                      + vaspout_parse_error     3
```
176,739 + 18,494 = 195,233 ✔  ·  311 + 1,041 = 1,352 ✔  ·  293 + 18 = 311 ✔

**Fetch-stage per-member events** (non-terminal / informational, not record rejections):
`extract_error` 1,715 (97 records; `8087871`:945 dominates), `disk_budget_deferred` 29
(all `20196565`, the inode hog), `archive_multipart_unsupported` 24, `http_504` 10,
`fetch_failed_transient` 9, `ChunkedEncodingError` 6, `archive_nesting_too_deep` 2.

---

## 4. Field completeness (metadata, frame-weighted)

| field | coverage | note |
|---|---|---|
| `REF_forces` | **100.00%** | every frame |
| `REF_stress` | **41.11%** (4,879,418) | only the vasprun/vaspout path emits stress; OUTCAR path varies |
| `run_type` | null on **25.62%** | exactly the `ase.OUTCAR` frames — pymatgen-only field |
| `functional` | null on **25.62%** | same cause |
| `electronic_converged` | null on **26.40%** | OUTCAR path + a few vasprun edge cases |
| `scf_dE` / SCF verdict | present where parseable | **0.03%** of frames SCF-unconverged (3,968) — very clean |
| `dropped_no_energy` | **0** | no energy-less frames leaked in |
| site charges / magmoms | 15,224 / 10,418 calcs | final-frame only, as designed |

**The one structural gap:** ~3.04M frames (all `parser=="ase.OUTCAR"`) lack
`run_type`/`functional`/`electronic_converged`. This is *by construction* (those come from
pymatgen `Vasprun.run_type`/`converged_electronic`, which don't run on the OUTCAR fallback),
**not corruption** — and it is back-fillable later by classifying from the OUTCAR's INCAR echo.
Filter these with `parser == "ase.OUTCAR"`.

---

## 5. Coverage & diversity (the weak axis)

**Frame concentration is severe** — the headline caveat for any downstream training:

| measure | value |
|---|---|
| top **1** record | **46.84%** of all frames (`5720009`, revPBE+Padé AIMD) |
| top **3** records | 67.35% |
| top **10** records | 83.15% |
| top **1** calc-unit | 0.25% |
| calc-unit frames | median **1**, mean 67, **max 30,000** (single AIMD run) |

So although the frame count (11.9M) exceeds MPtrj (1.6M), the number of *independent*
configurations is far smaller — trajectory frames within a calc are highly correlated and
a handful of AIMD/MC deposits dominate. **Downstream: de-correlate/subsample per trajectory
and weight by deposit**, or the model over-fits a few chemistries.

**Nominal diversity is actually decent** once you look past frame weighting:
- **3,795** distinct `potcar_set_hash` (pseudopotential sets) · **20** functionals ·
  **89** elements present.
- Functionals span GGA/PBE/PBEsol/revPBE, meta-GGA (SCAN/R2SCAN), hybrids
  (HSE06/B3LYP/PBE0), +U and many vdW variants — **heterogeneous, so a consistency filter
  by (functional + potcar_set_hash + settings) is mandatory before training** (absolute
  energies are only comparable within one such group).
- Elements: O (9.71M) and H (7.49M) dominate (oxides/hydrides/aqueous); transition metals
  well represented (Ag, Au, Pt, Cu ~1–1.8M); rare earths sparse (Dy 9, Lu 2, Pm 7).
- Cell sizes: 77% of frames have 51–200 atoms, 13.5% 11–50, 9% >200, 0.5% ≤10; range 1–1536.
- Heavy-output availability recorded (not stored): DOS 107k calcs, eigenvalues 106k,
  charge-density 73k, wavefunction 67k — re-fetchable pointers for future property work.

**Provenance & licensing (clean):** 293 distinct records = 293 concepts = 293 DOIs; every
frame carries source DOI + license. `cc-by-4.0` 98.3%, remainder all open (cc0/cc-by-sa/
mit/apache/bsd/gpl); `resource_type` dataset 88.6% / publication 10.7% / software 0.7%.

---

## 6. Label accuracy — what metadata already proves, what shards must confirm

Confirmed from metadata (§4): near-zero SCF non-convergence, no energy-less frames, and
`max_abs_free_minus_e0_per_atom` ≤ 0.37 eV/atom with only 0.05% of frames above 0.05 — so
the E0 label is force/stress-consistent for essentially the whole set.

**Not yet verified (needs the shards, §8):** that stored energies/forces/stress are finite
and physical (no NaN/inf, no Fortran `********` overflow), that per-atom energies sit in a
sane window, that `REF_*` keys land in `atoms.info`/`atoms.arrays` on read-back, and that the
`REF_stress` Voigt sign/units are correct on a known case.

---

## 7. Loss ledger & recovery pool (deferred work)

126 records have ≥1 parse-rejected calc: **18 wholly-failed** (0 calcs stored, 2,266
calc-units) + **108 partially-lost**. Recoverability by reason:

| reason | calcs | recoverable? |
|---|---|---|
| `outcar_parse_error` | 17,037 | **Yes** — lenient chunk-by-chunk OUTCAR parser (keep completed ionic steps, drop only the bad trailing one). ⚠️ NEB caveat: OUTCAR forces may be spring/tangent-modified, not raw DFT — verify before ingesting. |
| `no_frames` | 779 | mostly no — genuinely energy-less |
| `vasprun_parse_error` | 665 | hard — truncated/corrupt vaspruns |
| `primary_too_large` | 88 | **Yes, cheap** — higher-RAM re-parse (`--max-primary-bytes` up on `icelake-himem`) |
| `parse_timeout` | 14 | **Yes** — longer `--parse-timeout` / bigger RAM |
| `vaspout_parse_error` | 3 | residual only (h5py fix already recovered the rest) |

Biggest wholly-failed targets: `7267564` (1,744 OUTCAR), `3527985` (384), `16400603` (77:
55 OUTCAR + 22 too-large). The `outcar_parse_error` bulk is concentrated — 88% from 5 records
(`31044`, `20053085`, `15741825`, `7267564`, `7023990`) — so a lenient OUTCAR parser is the
single highest-yield recovery.

**Permanently un-harvestable (2 records only):** `20196565` and `18012696` — both unpack to
millions of tiny files, exceeding the CSD3 `/rds` 1M-inode limit. Un-stageable on this
filesystem; re-fetchable from Zenodo onto a different FS if ever needed.

---

## 8. Outstanding check — shard-level data accuracy (run on CSD3)

The only axis not yet evaluated is the *content* of the frames (metadata says they exist and
are self-consistent; this confirms the numbers are physical). Run on CSD3 where the shards
live (sample, don't sweep all 11.9M):

```bash
# Physical-sanity sweep over a random sample of frames (needs ase in the venv)
python - <<'PY'
import gzip, glob, random, numpy as np
from ase.io import read
shards = sorted(glob.glob("/rds/user/$USER/hpc-work/zenodo/dataset/shard-*.extxyz.gz"))
random.seed(0); sample = random.sample(shards, 40)          # ~3% of 1,212 shards
bad = []
for s in sample:
    for a in read(s, index=":"):
        e = a.info.get("REF_energy"); f = a.arrays.get("REF_forces")
        st = a.info.get("REF_stress")
        na = len(a)
        if e is None or not np.isfinite(e): bad.append((s,"energy",e)); continue
        if f is None or not np.isfinite(f).all(): bad.append((s,"forces",None)); continue
        epa = e/na
        if not (-15 < epa < 8): bad.append((s,"epa",epa))
        if np.abs(f).max() > 100: bad.append((s,"fmax",float(np.abs(f).max())))
        if a.get_volume() <= 0: bad.append((s,"vol",a.get_volume()))
        if st is not None and not np.isfinite(st).all(): bad.append((s,"stress",None))
print("checked sample; anomalies:", len(bad))
for b in bad[:50]: print(" ", b)
PY
```
Also worth a one-shot: confirm `REF_energy`/`REF_forces`/`REF_stress` survive an ASE
read→write→read round-trip in `atoms.info`/`atoms.arrays` (not absorbed into a calculator),
and eyeball one `REF_stress` against a known VASP kBar tensor for sign/units.

---

## 9. Corrections to prior docs

- `HARVEST_RESULT.md` lists `14773462` as "manually excluded (source of the 221
  FileNotFoundErrors)". It **partially recovered** after the parse-timeout/skip fixes and
  **is now in the dataset** (293-record set), with 160 residual `vasprun_parse_error` calcs.
  Likewise `20053085` (once manually excluded on 8/4) is now partially in the dataset
  (3,478 residual `outcar_parse_error`, rest parsed). Only `18012696` carries the terminal
  `manually_excluded` flag in the final manifest.
- The 7/31 nested-tarball bug (bare `.gz/.bz2/.xz/.zst` treated as tar → 158k spurious
  `extract_error` from record `15307432`) is fully healed: `15307432` is now in the dataset
  with **zero** residual rejections.

---

## 10. Operational timeline (cross-ref `logs/` + `sacct`)

~30 `zh-pipeline` jobs across Jul 31–Aug 10 on the self-resubmit chain (`RESUBMIT=1`, 12 h
SL3, `MAX_ATTEMPTS=8`), peak `MaxRSS ≈ 27.7 GB` (icelake-himem). Stall→fix beats:

| window | job(s) | signature (`.err`) | cause → fix |
|---|---|---|---|
| 7/31 | 32485773 (CANCELLED, 27.7 GB) | 158k `extract_error` | bare-compressed-in-nest treated as tar → extractor fix (record `15307432` fully recovered) |
| 8/2–3 | 32601595/32625824/32669564 | `DatasetLockError` ×26–30 | stale cross-host `.parse.lock` → lock clears at startup + SLURM-aware reclaim; `20196565` inode hog excluded |
| 8/3 | — | hung OUTCAR (100% CPU) | truncated `14773462` vasprun → OUTCAR loop → `--parse-timeout` (forkserver child) |
| 8/4 | 32733353/32794878 | — | `20053085` incomplete OUTCARs → skip-previously-rejected on resume |
| 8/6–9 | 32944819…33252591 | `LZMAError` + `PasswordRequired` ×2 | uncaught extractor exceptions re-attempted for days → caught → terminal `extract_error` |
| 8/10 | 33384294 (zh-finish) | TIMEOUT @6h in `purge-raw` | nested MC tarball `8005679` (+37,054 frames) parsed & committed **before** purge → data safe, purge is housekeeping |
| 8/10 | 33416453 (verify) | `OOM`/Killed (3.4 GB, icelake) | verify held full frame-id multiset → moved to icelake-himem |
| 8/11 | 33418925 (verify) | TIMEOUT, D-state Lustre | per-frame `Atoms` build + degraded OST → text-parse verify (no `Atoms`) |
| 8/11 | 33447316 (verify) | exit 0, 25 min | **PASSED** — the integrity JSON this eval is built on |

User run-note "283 records @ 8/10" predates the finalization: the `zh-finish` run (`8005679`)
+ mop-up parse (`zh-parse-33207951`) lifted it 283 → **293**.

---

## 11. Recommendations

1. **Ship the dataset as-is for a first MLIP baseline** — it is complete, integrity-verified,
   and cleanly licensed. Nothing blocks training.
2. **Apply a consistency filter before training**: group by `(functional, potcar_set_hash)`,
   pick the dominant consistent subset, and **subsample correlated trajectory frames**
   (per-calc stride) so the top-10 deposits don't swamp the loss. Report the *effective*
   distinct-composition count alongside the 11.9M.
3. **Cheap recovery wins** (when you return to it): higher-RAM re-parse of the 88+14
   `primary_too_large`/`parse_timeout` calcs, then a lenient OUTCAR parser for the 17k
   `outcar_parse_error` bulk (biggest yield; mind the NEB force caveat).
4. **Run the §8 shard sanity sweep** to close the last (data-value) evaluation axis before
   the dataset is used as a training label source.
5. **Diversity, not depth, is the argument for NOMAD** ([[nomad-harvest-plan]],
   `docs/NOMAD_HARVEST.md`) — plan dedup against this set's 293 DOIs / `potcar_set_hash`es.
