# Running the harvest on CSD3

Batch templates for the full Zenodo harvest on Cambridge's CSD3. Every script is a
template: edit the `#SBATCH -A` account (find yours with `mybalance`) and the ENV SETUP
block, then submit from the repo root.

```
scripts/csd3/00_check_network.sh   # ONE-OFF: prove compute nodes can reach zenodo.org
scripts/csd3/10_discover.sh        # stage 0-1 (discover + triage) — network, 1 core
scripts/csd3/20_pipeline.sh        # stage 2-4 overlapped (fetch || parse+purge) + verify
scripts/csd3/30_parse_array.sh     # OPTIONAL: many-core array parse of a fetched manifest
scripts/csd3/31_merge_verify.sh    #   ...then merge the per-task dirs + verify + purge
```

## CSD3 facts these scripts are built around

| Fact | Value | Consequence for the harvest |
|---|---|---|
| Wallclock limit | **36 h** (SL1/SL2), **12 h** (SL3) | The harvest can exceed one job → every stage is resumable; `20_pipeline.sh` can self-resubmit (`RESUBMIT=1`). Longer runs need the `-long` QoS (up to 7 days) which must be requested from `support@hpc.cam.ac.uk` and uses the `icelake-long`/`cclake-long` partitions. |
| `/rds/user/$USER/hpc-work` | **1 TB and 1 million files**, Lustre, **not backed up** | Point `ZENODO_HARVEST_DATA` here. `--max-disk-bytes ~0.8 TB` **and** `--max-disk-files 800000`: measured, the inode limit binds FIRST (extracted VASP trees are ~7.6 KiB median per file, so 1M files ≈ 0.3 TB). |
| `/home/$USER` | 50 GB, backed up | Code and `.env` only — never job I/O. |
| Login nodes | capped at ~4 CPUs per user | Fine for a smoke test, **not** for the real fetch — run it as a batch job. |
| Array jobs | `--array=0-N`, `$SLURM_ARRAY_TASK_ID` | Used by `30_parse_array.sh` (parse is CPU-bound and embarrassingly parallel). |

Sources: [Quick Start](https://docs.hpc.cam.ac.uk/hpc/user-guide/quickstart.html),
[I/O management](https://docs.hpc.cam.ac.uk/hpc/user-guide/io_management.html),
[long jobs](https://docs.hpc.cam.ac.uk/hpc/user-guide/long.html).

## Do compute nodes have outbound internet?

**The CSD3 documentation does not state this either way, so verify it before submitting
the fetch** — run `scripts/csd3/00_check_network.sh` (a ~1 minute interactive job). The
harvest's fetch stage needs outbound HTTPS to `zenodo.org`. If compute nodes turn out to
be firewalled:

* check whether a proxy is published for your project (then export `https_proxy` in the
  ENV SETUP block — `requests` honours it automatically); or
* ask `support@hpc.cam.ac.uk`; or
* fall back to running only `fetch` on a login node with `--workers 2` and a modest
  `--max-disk-bytes`, in `screen`/`tmux`, and keep `parse`/`merge`/`verify` in batch jobs
  (they need no network). Fetch is fully resumable, so it can be stopped and restarted.

## Order of operations

```bash
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo   # scratch, read at import
mkdir -p logs                                                # SLURM opens -o/-e before the job runs
sbatch scripts/csd3/10_discover.sh                            # ~1-2 h (rate-limited)
sbatch scripts/csd3/20_pipeline.sh                            # the long one
# optional many-core parse instead of the pipeline's serial parse:
sbatch scripts/csd3/30_parse_array.sh
```

`mkdir -p logs` is required **before the first submit**: every script's `#SBATCH -o logs/…`
is opened by SLURM before the script body (which does its own `mkdir`) runs, so a missing
`logs/` makes the job fail to start.

## Parse memory (icelake-himem)

`parse` (pymatgen) is the only memory-hungry stage — its peak RSS is **~8.6× the
vasprun.xml/OUTCAR file size** (measured: a 633 MB file peaks at ~5.4 GB), and an
over-budget parse is a cgroup **SIGKILL** of the whole job, not a catchable error. On CSD3
memory is bundled with cores (icelake-himem = 6760 MiB/core), so `--cpus-per-task` on the
parse/pipeline jobs is bought for **RAM, not compute** (pymatgen is single-threaded). Size
`--max-primary-bytes` to the job's RAM: `~0.85 × cpus-per-task × 6.76 GB ÷ 8.6`
(`--cpus-per-task=4` → ~26 GiB → ~2.5 GB). Over-cap primaries are skipped
(`primary_too_large`) and kept on disk for a later bigger-RAM re-parse. Confirm the ratio on
your own data/hardware first:

```bash
srun -A MYACCT-SL3-CPU -p icelake-himem --cpus-per-task=4 --time=00:20:00 \
    python scripts/csd3/csd3_parse_memory.py --raw-dir $ZENODO_HARVEST_DATA/raw --top 8
# before any fetch, calibrate on synthetic samples instead:
srun -A MYACCT-SL3-CPU -p icelake-himem --cpus-per-task=4 --time=00:20:00 \
    python scripts/csd3/csd3_parse_memory.py --synthetic
```

Discovery is hard-limited by Zenodo to 30 search requests/minute, so it is single-stream
by design — do **not** parallelise it. Measured 2026-07-27 for the 3-resource-type run:
~34k hits ≈ 1.4k pages ≈ **under 1 h** of paging. Only `fetch` benefits from `--workers`.

## Resuming

Everything is resumable; re-running the same command continues where it stopped.

* `discover` — appends to `<out>.hits.jsonl` and skips completed query/date windows.
* `triage` — **not** resumable (it rewrites its keep-list), but cheap: only `archive`
  records are peeked, ~600 of them for the measured candidate set (~10 min). If it is
  interrupted, just re-run it.
* `fetch` — skips recids already in `fetched.jsonl`, and **resumes a part-transferred
  file over HTTP Range** (a killed job does not restart a 100 GB archive from byte 0).
* `parse` — skips calc_ids already in `metadata.jsonl`, prunes orphan frames from a
  crashed writer.
* `pipeline` — re-splits the same parts, so each batch's fetch/parse skip what is done.

## Staying inside the quota

`--max-disk-bytes` / `--max-disk-files` are enforced on **actual** usage (every byte and
file is charged as it is written), not on any predicted decompression ratio — measured
expansion on real records spans ~1x to 4.1x. When a limit is reached the record being staged
is rolled back whole, fetch stops resumably, and `pipeline` runs `parse` + `purge-raw` to
reclaim the space before re-fetching the same batch. Each run reports its own
`peak_staged_bytes` / `peak_staged_files`, so check those in the job log against your limits.

If the volume fills anyway, the `EDQUOT` write errors are treated as transient and those
records are retried by a later run — they are not silently dropped.

One caveat: if a **killed job leaves a `.parse.lock`** in the dataset dir, a lock from a
*different host* is never assumed stale (cross-node liveness is uncheckable). Confirm no
parse is running, then `rm <dataset-dir>/.parse.lock`.
