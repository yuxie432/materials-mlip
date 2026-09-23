# Running the Materials Cloud harvest on CSD3

Batch templates, probe and benchmark for harvesting VASP data from the **Materials Cloud
Archive**. They mirror the NOMAD templates (`../nomad/`) and share their conventions — set the
account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before `sbatch`,
`mkdir -p logs_mc` before the first submit. Design + live-verified API facts:
`docs/MATERIALS_CLOUD_HARVEST.md`.

```
scripts/csd3/materials_cloud/10_discover.sh     # stage 0-1: full census + gates + overlap flags + peeks
scripts/csd3/materials_cloud/15_bench.sh        # sizing: speed probe + AiiDA census + fetch/parse pilot
scripts/csd3/materials_cloud/20_pipeline.sh     # stage 2-4 overlapped (fetch || parse+purge) + verify
scripts/csd3/materials_cloud/30_bigparse.sh     # phase B: parse the deferred (too-big-for-RAM) primaries
scripts/csd3/materials_cloud/csd3_mc_probe.py   # --speed / --aiida probes (run by 15_bench.sh)
scripts/csd3/materials_cloud/csd3_mc_bench.py   # real-data fetch+parse pilot (run by 15_bench.sh)
```

## Separate MC tree (never mixed with Zenodo / NOMAD)

Everything Materials Cloud writes lives under **its own sibling root** `$MC_HARVEST_DATA`
(default `/rds/user/$USER/hpc-work/materials_cloud`). The Zenodo and NOMAD datasets are only
*read* (their `dataset/metadata.jsonl`) for the overlap flags at discover time.

```
/rds/user/$USER/hpc-work/
├── zenodo/            (Zenodo harvest)          <- read-only here
├── nomad/             (NOMAD harvest)           <- read-only here
└── materials_cloud/   manifests/ raw/ dataset/  <- this harvest
logs_mc/               SLURM .out/.err + per-job summary JSON (repo-relative, gitignored)
```

## Order of operations (run these on CSD3, from the repo root)

```bash
cd ~/materials-mlip && git pull
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo
export NOMAD_HARVEST_DATA=/rds/user/$USER/hpc-work/nomad
export MC_HARVEST_DATA=/rds/user/$USER/hpc-work/materials_cloud
mkdir -p logs_mc
MAN=$MC_HARVEST_DATA/manifests

# 0. (~1 min) live end-to-end smoke on a 16.5 MB record, isolated temp dir:
python -m materials_cloud_harvest.cli smoke          # expect "9/9 checks passed (PASS)"

# 1. census + triage (peeks run 4-way in parallel; resumable via the peek cache):
DISC=$(sbatch --parsable scripts/csd3/materials_cloud/10_discover.sh)

# 2. sizing job (queued behind 1): speed probe + sqlite AiiDA census + fetch/parse pilot on ~12 GB
sbatch --dependency=afterok:$DISC scripts/csd3/materials_cloud/15_bench.sh

# 3. review before fetching — send me these:
python -c "import json;print(json.dumps(json.load(open('$MAN/mc_keep.report.json'))['summary'],indent=1))"
cat $MAN/mc_speed.json $MAN/mc_bench.json
python -c "import json;d=json.load(open('$MAN/mc_probe_aiida.json'))['aiida'];print({k:v for k,v in d.items() if k!='files'})"
quota

# 4. the harvest (defaults sized for a dedicated ~800 GB / ~900k-inode slice; override from step 3):
RESUBMIT=1 sbatch scripts/csd3/materials_cloud/20_pipeline.sh
#    e.g.  WORKERS=8 PARSE_WORKERS=4 MAX_PRIMARY_BYTES=2000000000 RESUBMIT=1 \
#          sbatch --cpus-per-task=16 scripts/csd3/materials_cloud/20_pipeline.sh

# 5. phase B, after 4 has finished: the primaries deferred as too big for the pipeline's RAM cap
sbatch scripts/csd3/materials_cloud/30_bigparse.sh        # 32 cpus, one parse at a time, 16 GB cap

# 6. check
python -m materials_cloud_harvest.cli status --max-disk-bytes 680000000000 --max-disk-files 765000
python -m zenodo_harvest.cli verify --dataset-dir $MC_HARVEST_DATA/dataset
```

## What bounds the harvest, and the knobs

| stage | what bounds it | knob | how to set it |
|---|---|---|---|
| triage peeks (~1.5k) | **request latency** — each is 1-3 small Range reads through MC's API 302 hop (~0.3-2 s each) | `PEEK_WORKERS` (10_discover.sh, default 4) | MC allows 500 req/60 s; 4 workers paced at 0.4 s stay far below it |
| fetch bulk (~90 GB) | **S3 bandwidth** (a few multi-GB AIMD tarballs are most of the bytes) | `WORKERS` (20_pipeline.sh, default 4) | `mc_speed.json` → `recommended_fetch_workers` (best aggregate of 1/2/4/8 streams) |
| many small calcs (~10⁴) | **parse throughput** — one forkserver child per calc, ~0.3-1 s each | `PARSE_WORKERS` (default 3) | `mc_bench.json` → `small_parse.speedup`; keep `workers × ratio × cap` inside the RAM |
| long-AIMD primaries | **RAM** — pymatgen peaks ~10-12× the uncompressed file | `MAX_PRIMARY_BYTES` (default 2.5 GB) + `--cpus-per-task` | `mc_bench.json` → `worst_rss_ratio` / `suggested_caps`; bigger ones go to **30_bigparse.sh** |

`mc_bench.json → projection` gives the pilot's fetch-hours vs parse-hours (wall ≈ the larger —
the pipeline overlaps them). A token is **not** needed: every record is public, the API limit is
not approached, and the bytes come from presigned S3 URLs a token would not speed up.

## What to look at in the triage report (`mc_keep.report.json`)

* `summary.kept_records` / `fetch_units` / `bytes_to_fetch` — the harvest size.
* `summary.decisions` — `vasp_mention` (kept fail-safe), `vasp_evidence` (**blind spot recovered**:
  records that never mention VASP but whose zips/AiiDA exports hold VASP outputs),
  `peek_proved_no_vasp`, `no_vasp_evidence`, `evidence_gap_only`.
* `summary.blind_spot_recovered` — the record ids of the recovered blind spot.
* `summary.unpeekable_skipped` — tar-family bytes in non-mentioning records deliberately not
  downloaded (the residual blind spot).
* per-record `gaps` — sqlite_zip AiiDA exports, unreadable archives, nested-only zips.

And in the discover log: `flagged_overlap` + `overlap_examples` — MC records linked to (or titled
like) harvested Zenodo records, or cited by NOMAD calcs. They are **kept** (all MC→Zenodo links are
`IsSupplementTo`); the flags travel in provenance for the training-time dedup.

## Sizing (dedicated ~800 GB / ~900k inodes)

* **Disk**: the valve bounds only `$MC_HARVEST_DATA/raw`. Defaults `MAX_DISK_BYTES=680e9`,
  `MAX_DISK_FILES=765000` (~85% of the slice; the MC dataset + manifests are a few GB and not
  valve-tracked). Multi-archive records are split per archive by triage, so the largest staging
  unit is one archive + its extraction. Deferred big primaries stay staged for 30_bigparse.sh. If
  other jobs share the quota again, lower both to what `quota` shows free.
* **RAM**: `20_pipeline.sh` runs 3 parse workers on 16 `icelake-himem` cores (~106 GiB) with a
  2.5 GB (uncompressed) cap — rule `cpus × 6760 MiB ≥ PARSE_WORKERS × ratio × MAX_PRIMARY_BYTES
  + ~8 GiB`. `30_bigparse.sh` then parses what was deferred, one at a time, on 32 cores (~211 GiB,
  16 GB cap). Anything still bigger stays staged and is listed at the end of its log.
* **Time**: one 12 h SL3 job is expected for the pipeline; `RESUBMIT=1` covers an overrun and
  retries any unit still failing transiently (the pipeline exits non-zero for those).

## Rate limits & etiquette

MC allows 500 requests / 60 s (`X-RateLimit-Limit`, `Retry-After: 60` on 429 — honoured). Each file
request is a 302 from the API (latency, counted) + a presigned CSCS S3 transfer (bandwidth, not
counted). No token is used or needed — the adapter's anonymous session guarantees the Zenodo token
is never sent, and the shared fetch only ever attaches it to `zenodo.org`.
