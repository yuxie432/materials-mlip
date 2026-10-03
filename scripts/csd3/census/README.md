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
scripts/csd3/census/40_recover_t1.sh  # targeted re-parse (+ optional re-fetch) of fixable rejections (T1; T2 via KEEP=)
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

# 3. triage (peeks only — a few MB per archive, nothing staged), after the review. In stages:
#    T1 first (done 2026-09-28: census_keep_t1.*). Deep peeks (default on) then take a second look
#    at every archive the standard peeks left unresolved — for a T1 fail-safe record a proof of
#    "no VASP" saves its whole download; what stays unresolved is still downloaded whole (no cap).
#    Re-running T1 reuses every cached standard peek, so only the deep peeks cost requests (~3-5 h).
#    It rewrites census_keep_t1.*, so keep the first run's files (its report holds the residual-sample
#    result) — in a SUBDIRECTORY: any census_keep_*.jsonl beside them is read as an earlier keep-list
#    and its records are skipped:
mkdir -p $ZENODO_CENSUS_DATA/t1_first_run && cp -p $ZENODO_CENSUS_DATA/census_keep_t1.* $ZENODO_CENSUS_DATA/t1_first_run/
TIERS=T1 RESIDUAL_SAMPLE=0 NEGATIVE_SAMPLE=0 OUT=$ZENODO_CENSUS_DATA/census_keep_t1.jsonl \
  RESUBMIT=1 sbatch scripts/csd3/census/30_triage.sh
#    T2 (~33k records; no fail-safe, so deep peeks are its only way into such archives; ~1.5-2 days):
#    chain it after the T1 re-run, at INTERVAL 1.2 to leave Zenodo request budget for the pipeline
#    that runs at the same time, and DEEP_MAX_REQUESTS 100 (it only needs to FIND VASP):
TIERS=T2 RESIDUAL_SAMPLE=0 NEGATIVE_SAMPLE=0 INTERVAL=1.2 DEEP_MAX_REQUESTS=100 \
  OUT=$ZENODO_CENSUS_DATA/census_keep_t2.jsonl RESUBMIT=1 \
  sbatch --dependency=afterok:<T1 re-run job> scripts/csd3/census/30_triage.sh
#    -> REVIEW / send <out>.report.json ("summary" + per-record table) + <out>.licence_review.jsonl
#       (no-licence and ND records are listed there, NOT in the keep-list). summary.kept.bytes_blind
#       = what the T1 fail-safe will download whole; summary.deep_peeks = what the deep peeks did.
#       Residual T3: the 2026-09-28 sample found 0 in 3,000 -> no full T3 run (user decision).

# 4. fetch + parse straight into the production dataset (back up the metadata first; check `quota`).
#    ONE pipeline at a time (they write the same dataset): census_keep_t1.jsonl, then
#    census_keep_t2.jsonl. The shape below is sized for the census keep-lists: 8 icelake-himem
#    cores (~53 GiB) -> 4 parse workers under a 42 GiB RAM budget, cap 3.8 GB per primary (the
#    keyword harvest had 8 of 182k primaries above 2 GB), 4 fetch workers; the 800 GB / 800k
#    staging valve (the script's defaults) fits the 1 TB / 1M quota at ~120 GB / 7k files used.
quota
cp $ZENODO_HARVEST_DATA/dataset/metadata.jsonl $ZENODO_HARVEST_DATA/dataset/metadata.jsonl.bak.pre_census
IN=$ZENODO_CENSUS_DATA/census_keep_t1.jsonl RAW_DIR=$ZENODO_HARVEST_DATA/raw_census \
  RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh     # 40 batches (default): lower staging peaks
#    Progress (read-only, safe beside the job): --scope-to-keep counts only this keep-list's records
#    (calcs, frames, rejections) instead of the whole production dataset; the fetch rejections are
#    read from beside --raw-dir ($ZENODO_HARVEST_DATA/manifests/rejections.jsonl, shared with the
#    keyword harvest); the part manifests sit in <keep-list>.pipeline_parts/:
python -m zenodo_harvest.cli status --keep $ZENODO_CENSUS_DATA/census_keep_t1.jsonl --scope-to-keep \
    --manifests-dir $ZENODO_CENSUS_DATA/census_keep_t1.pipeline_parts \
    --raw-dir $ZENODO_HARVEST_DATA/raw_census --dataset-dir $ZENODO_HARVEST_DATA/dataset \
    --max-disk-bytes 800000000000 --max-disk-files 800000   # add --no-staging-walk if raw/ is big
#    Live log: tail -f logs/zh-pipeline-<jobid>.err (one "parsing <calc_id>" line per calc); memory:
#    sacct -j <jobid> --format=JobID,State,Elapsed,MaxRSS; the JSON summary ends the .out.
#    Afterwards (T1: DONE 2026-10-01, job 37029347 — +280 calcs / +400k frames, §11 of the doc):
#    recover what the run rejected for a FIXED or resource-only
#    reason — their files are still staged (purge-raw never deletes an unparsed unit's files). One
#    20-core himem job re-parses them with --retry-rejected (cap ~10.9 GB; parsed calcs are skipped,
#    so it is safe to re-run), verifies, and purges what is now parsed. Defaults: 7506565 (numeric
#    ALGO, fixed in parse.py), 13843222 + 22084774 (primary_too_large); and it RE-FETCHES 14809725 +
#    3359829, two zips the fixed fetch can now extract (FETCH_RECIDS; "" = none).
#    No other job may write the dataset meanwhile (the T2 triage may run alongside):
git pull                     # parse.py numeric-ALGO guard + fetch zip-member recovery
sbatch scripts/csd3/census/40_recover_t1.sh
#    -> logs/zc-recover-<id>.{out,err}; "verify ok=True" ends the .out.

# 5. T2 pipeline. T2 triaged 2026-10-01 (103 evidence records / 210 GB; no fail-safe); reviewed
#    2026-10-02: 12792088 excluded, the 6,217 unresolved records not fetched (doc §6, decisions 9-13).
#    Same script and staging dir. raw_census holds only the files of T1 calcs rejected for good —
#    clear it first (housekeeping; nothing reads them again). Take a fresh metadata backup, exclude
#    12792088 with a manually_excluded line in the FETCH rejection log (the keep-list and its
#    round-robin parts stay as triage wrote them), and run 10 batches (103 records: fewer full
#    metadata scans than the default 40; staging stays far below the valve):
du -sh $ZENODO_HARVEST_DATA/raw_census; find $ZENODO_HARVEST_DATA/raw_census -type f | wc -l
rm -rf $ZENODO_HARVEST_DATA/raw_census
cp $ZENODO_HARVEST_DATA/dataset/metadata.jsonl $ZENODO_HARVEST_DATA/dataset/metadata.jsonl.bak.pre_census_t2
echo '{"stage": "fetch", "id": "12792088", "reason": "manually_excluded", "transient": null, "detail": "census T2: software-engineering paper artifact (ISSTA 2024 Sleuth); its only hint is one heavy-output-named file in a source tree; 13.4 GB not worth fetching (user decision 2026-10-02)"}' \
  >> $ZENODO_HARVEST_DATA/manifests/rejections.jsonl
IN=$ZENODO_CENSUS_DATA/census_keep_t2.jsonl RAW_DIR=$ZENODO_HARVEST_DATA/raw_census PARTS=10 \
  RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh
python -m zenodo_harvest.cli status --keep $ZENODO_CENSUS_DATA/census_keep_t2.jsonl --scope-to-keep \
    --manifests-dir $ZENODO_CENSUS_DATA/census_keep_t2.pipeline_parts \
    --raw-dir $ZENODO_HARVEST_DATA/raw_census --dataset-dir $ZENODO_HARVEST_DATA/dataset \
    --max-disk-bytes 800000000000 --max-disk-files 800000
#    T2: DONE 2026-10-01, job 37089987 (+86 records / +28,014 calcs / +450k frames, doc §11).
#    Afterwards, the fixable rejections are re-parsed by the recovery script pointed at T2 (RECIDS
#    must be given — empty means the T1 defaults; FETCH_RECIDS="" = no re-fetch). raw_census must
#    still hold the run's leftovers (the rejected units' files), so clear it only AFTER this job.
#    2026-10-02: 11234637 (OUTCARs with non-UTF-8 bytes) + 20403107/17254051 (VASPsol LAMBDA_D_K=****)
#    — both read by parse.py since 2026-10-02, so `git pull` first —, 21316138 (3 OUTCARs past the
#    20 min timeout; the script allows 2 h) and a re-fetch of 18706703 (HTTP 429):
git pull
KEEP=$ZENODO_CENSUS_DATA/census_keep_t2.jsonl WORK=$ZENODO_CENSUS_DATA/recover_t2 \
  RECIDS="11234637 20403107 17254051 21316138" FETCH_RECIDS="18706703" \
  sbatch scripts/csd3/census/40_recover_t1.sh
#    -> logs/zc-recover-<id>.{out,err}; "verify ok=True" ends the .out. Then raw_census can go.
#    T2 recovery: DONE 2026-10-02, job 37127828 (+435 calcs / +4,957 frames / +1 record).
#    PART A COMPLETE: +317 records / +201,738 calcs / +6,124,397 frames (doc §11 "Census outcome").
#
# 6. Seed snowball (doc §6 decisions 14-16, scoping in §11): re-score with all ~620 dataset records as
#    identity seeds, then triage ONLY the records whose tier rose — 541 expected (148 T3->T1, 55 T3->T2,
#    338 T2->T1; ~200 never peeked). NOT a plain 30_triage.sh re-run: deep-peek verdicts are cached
#    per read budget, so re-triaging the T2 tier at the default 300 would deep-peek its 6,217
#    unresolved records again (~90k requests). T1 movers get the T1 rule (fail-safe, no cap).
#    NB re-running 10_census.sh adds no newer records (fixed created range, doc §10).
git pull
mkdir -p $ZENODO_CENSUS_DATA/score_pre_snowball
cp -p $ZENODO_CENSUS_DATA/scored.jsonl $ZENODO_CENSUS_DATA/score_report.json $ZENODO_CENSUS_DATA/score_pre_snowball/
sbatch scripts/csd3/census/20_score.sh       # ~40 min (links cached); REWRITES scored.jsonl + score_report.json
#    after it (seconds; fine on a login node) — prints the moves, ~541 expected:
python -m zenodo_census.cli select-moved --old $ZENODO_CENSUS_DATA/score_pre_snowball/scored.jsonl \
    --out $ZENODO_CENSUS_DATA/scored_snowball.jsonl
SCORED=$ZENODO_CENSUS_DATA/scored_snowball.jsonl RESIDUAL_SAMPLE=0 NEGATIVE_SAMPLE=0 \
  OUT=$ZENODO_CENSUS_DATA/census_keep_snowball.jsonl RESUBMIT=1 sbatch scripts/csd3/census/30_triage.sh
#    ~2-3k requests ≈ 1 h. -> REVIEW / send census_keep_snowball.report.json + .licence_review.jsonl.
#    Then the pipeline as in step 5 (fresh metadata backup; ~200-300 GB of fail-safe downloads):
cp $ZENODO_HARVEST_DATA/dataset/metadata.jsonl $ZENODO_HARVEST_DATA/dataset/metadata.jsonl.bak.pre_census_snowball
IN=$ZENODO_CENSUS_DATA/census_keep_snowball.jsonl RAW_DIR=$ZENODO_HARVEST_DATA/raw_census PARTS=8 \
  RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh
#    status: the step-4 command with census_keep_snowball; fixable rejections: 40_recover_t1.sh with
#    KEEP=$ZENODO_CENSUS_DATA/census_keep_snowball.jsonl WORK=$ZENODO_CENSUS_DATA/recover_snowball RECIDS="…".
#    Seed snowball: DONE 2026-10-02 (triage 23 min; pipeline job 37197208, verify OK): +9 records /
#    +2,576 calcs / +30,571 frames; no recovery needed (doc §11). raw_census holds only rejected leftovers.
```

## What bounds each step

| step | cost | bound |
|---|---|---|
| census | ~5.8k search pages of 100 ≈ 3.6 h | Zenodo search: 30 req/min even with a token (paced on request starts, 2.2 s) |
| links | DataCite ~47 pages (~10 min); Europe PMC ~600 full texts (~10 min) | other hosts — runs beside the census |
| resolve | ≤ ~250 batched searches (~9 min) | Zenodo search, 30 req/min |
| openalex | one free singleton lookup per paper DOI of a T2/T3 record (~50k?) at ≤ 8/s | OpenAlex (list calls are metered since 2026; singletons are free) |
| triage | 1-3 Range reads per zip, 1 per tar (8 MB head); deep peeks of unresolved archives ≤ 300 reads / 256 MiB each. Measured (2026-09-28): T1 + samples 14.8k files in 3 h 26 min; T2 ~85k files ≈ 80-95k requests + deep peeks ≈ 1.5-2 days at `INTERVAL=1.2` | Zenodo 100/min + 5,000/h documented → `INTERVAL=0.8` s (4.5k/h) alone, 1.2 s beside a pipeline |
| pipeline | whatever the keep-list holds | as the original harvest (bandwidth, disk valve) |

Tuning: `INTERVAL` (seconds between triage request starts — raise it if the log shows repeated
429s), `PEEK_WORKERS`, `RESIDUAL_SAMPLE` / `NEGATIVE_SAMPLE`, `TIERS` / `TYPES`, `OUT`, `SCORED`
(the tiers to select from; default `scored.jsonl`).
