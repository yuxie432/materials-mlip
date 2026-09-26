# Running the Zenodo census on CSD3

Batch templates for **FURTHER_WORK part A**: find the VASP data on Zenodo that keyword discovery
cannot see, and feed it into the production Zenodo dataset through the ordinary pipeline. Design,
measurements and decisions: `docs/ZENODO_CENSUS.md`. Conventions as for the other harvests — set
the account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before `sbatch`,
`mkdir -p logs_census` before the first submit.

```
scripts/csd3/census/10_census.sh   # stage 0' : census of every archive-bearing record (~3.5 h) + link pulls
scripts/csd3/census/20_score.sh    # stage 0'': links top-up, version resolve, OpenAlex lookups, tiers
scripts/csd3/census/30_triage.sh   # stage 1' : zip / tar-head peeks -> census_keep.jsonl (RESUBMIT=1 chain)
scripts/csd3/20_pipeline.sh        # stages 2-4, UNCHANGED: IN=census_keep.jsonl RAW_DIR=raw_census
```

## Where things live

Everything the census writes is under `$ZENODO_CENSUS_DATA` (default
`$ZENODO_HARVEST_DATA/census`, i.e. `/rds/user/$USER/hpc-work/zenodo/census`); the Zenodo dataset,
its manifests and (for seed names) the Materials Cloud dataset are only read until step 4.

```
zenodo/census/
├── census.jsonl (+ .windows.jsonl)     one slimmed record per archive-bearing Zenodo record (~1.5 GB)
├── links/datacite_refs.jsonl (+ .cursor)   Crossref paper -> Zenodo references (DataCite events)
├── links/epmc_mentions.jsonl           Zenodo ids named in Europe PMC full texts of VASP papers
├── links/zenodo_versions.jsonl         cited version ids -> concept ids
├── links/openalex.jsonl                per-DOI verdict: cites the VASP papers? field?
├── scored.jsonl + score_report.json    tier per record + the report to review
├── peeks.jsonl                         every zip / tar-head peek verdict (shared by all triage runs)
├── census_keep.jsonl                   THE keep-list for 20_pipeline.sh
├── census_keep.report.json             triage summary (samples, yield per signal) + per-record table
├── census_keep.licence_review.jsonl    VASP-evidenced records with ND / no licence (for approval)
└── census_keep.rejections.jsonl        what triage did not keep, and why
logs_census/                            SLURM .out/.err + per-step JSON summaries (repo-relative)
```

## Order of operations (from the repo root)

```bash
cd ~/materials-mlip && git pull
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo
export ZENODO_CENSUS_DATA=$ZENODO_HARVEST_DATA/census
mkdir -p logs_census

# 0. (~30 s, login node) live smoke of the census stage on one day, isolated dir:
ZENODO_CENSUS_DATA=$HOME/zc_smoke python -m zenodo_census.cli -v census \
    --start 2024-10-02 --end 2024-10-02 && rm -rf $HOME/zc_smoke   # expect ~199 written, page_size 100

# 1+2. census, then links/resolve/OpenAlex/score, chained:
C1=$(sbatch --parsable scripts/csd3/census/10_census.sh)
C2=$(sbatch --parsable --dependency=afterok:$C1 scripts/csd3/census/20_score.sh)
python -m zenodo_census.cli status            # any time (read-only)
#    If 10_census.sh FAILS (its .err ends in a traceback; 20_score.sh is then cancelled by the
#    dependency): fix the cause, `git pull`, and submit the same two lines again — finished windows
#    are skipped, the unfinished one is re-paged (duplicate lines are harmless), finished link pulls
#    return at once. Records Zenodo's JSON serializer cannot return are handled in-run and listed in
#    $ZENODO_CENSUS_DATA/census.jsonl.poison.jsonl; a long Zenodo outage stops the job cleanly
#    ("ZenodoOutage ... resubmit to resume").
#    -> REVIEW / send $ZENODO_CENSUS_DATA/score_report.json: tier sizes, the peek workload,
#       the known-miss probes, the Europe PMC coverage.

# 3. triage (peeks only — a few MB per archive, nothing staged), after the review:
RESUBMIT=1 sbatch scripts/csd3/census/30_triage.sh
#    -> REVIEW / send census_keep.report.json ("summary") + census_keep.licence_review.jsonl.
#       If the residual sample's STRICT rate (summary.samples.residual_sample.rate_primary — a
#       VASP output actually seen, not just INCAR/POSCAR in a partial head) is ~1 in 2,000 or
#       better, run the whole non-software T3 tier as a second keep-list (~2-3 days of peeks):
#       TIERS=T3 RESIDUAL_SAMPLE=0 NEGATIVE_SAMPLE=0 OUT=$ZENODO_CENSUS_DATA/census_keep_t3.jsonl \
#         MAX_ATTEMPTS=12 RESUBMIT=1 sbatch scripts/csd3/census/30_triage.sh

# 4. fetch + parse straight into the production dataset (back up the metadata first; check
#    `quota` — the valve below leaves room for the NOMAD / MC datasets on the same 1 TB / 1M quota):
quota
cp $ZENODO_HARVEST_DATA/dataset/metadata.jsonl $ZENODO_HARVEST_DATA/dataset/metadata.jsonl.bak.pre_census
IN=$ZENODO_CENSUS_DATA/census_keep.jsonl RAW_DIR=$ZENODO_HARVEST_DATA/raw_census \
  MAX_DISK_BYTES=700000000000 MAX_DISK_FILES=700000 RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh
python -m zenodo_harvest.cli status --keep $ZENODO_CENSUS_DATA/census_keep.jsonl \
    --manifests-dir $ZENODO_CENSUS_DATA --raw-dir $ZENODO_HARVEST_DATA/raw_census \
    --dataset-dir $ZENODO_HARVEST_DATA/dataset      # progress (read-only; finds the part manifests)
```

## What bounds each step

| step | cost | bound |
|---|---|---|
| census | ~5.8k search pages of 100 ≈ 3.6 h | Zenodo search: 30 req/min even with a token (paced on request starts, 2.2 s) |
| links | DataCite ~47 pages (~10 min); Europe PMC ~600 full texts (~10 min) | other hosts — runs beside the census |
| resolve | ≤ ~250 batched searches (~9 min) | Zenodo search, 30 req/min |
| openalex | one free singleton lookup per paper DOI of a T2/T3 record (~50k?) at ≤ 8/s | OpenAlex (list calls are metered since 2026; singletons are free) |
| triage | 1-3 Range reads per zip, 1 per tar (8 MB head) — ~20-30k files ≈ 5-8 h | Zenodo 100/min + 5,000/h documented → `INTERVAL=0.8` s (4.5k/h) |
| pipeline | whatever the keep-list holds | as the original harvest (bandwidth, disk valve) |

Tuning: `INTERVAL` (seconds between triage request starts — raise it if the log shows repeated
429s), `PEEK_WORKERS`, `RESIDUAL_SAMPLE` / `NEGATIVE_SAMPLE`, `TIERS` / `TYPES`, `OUT`.
