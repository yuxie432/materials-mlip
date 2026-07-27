# zenodo_harvest

Harvest openly-licensed DFT (VASP) calculation data from Zenodo and assemble it
into a compact, provenance-rich dataset for training a machine-learning
interatomic potential (MLIP). See [`docs/DESIGN.md`](docs/DESIGN.md) for the data
model, storage format, and coverage/quality strategy, and [`CLAUDE.md`](CLAUDE.md)
for domain conventions.

## Repository layout

```
zenodo_harvest/     the importable package (the pipeline itself)
tests/              offline pytest suite — top level by convention: pytest's rootdir
                    discovery expects it there, and it is deliberately NOT packaged
                    (pyproject ships only `zenodo_harvest`)
docs/               long-form documents: DESIGN.md (data model + storage design),
                    survey-findings.md (the measured "how much is on Zenodo?" study)
scripts/csd3/       CSD3 sbatch job templates + their runbook
scripts/estimate/   one-off measurement tools for the survey + their runbook
```

`scripts/` groups non-package executables by purpose; each subdirectory keeps its own
`README.md` runbook next to the code it drives, while findings and design live in `docs/`.

## Pipeline

```
stage 0  discover  keyword search          -> candidate manifest (JSONL)  [done]
stage 1  triage    file-listing + zip-peek -> keep-list (JSONL)           [done]
stage 2  fetch     download archives (checksum-verified) + extract VASP   [done]
stage 3  parse     pymatgen Vasprun/Vaspout / ASE OUTCAR -> frames        [done]
stage 4  store     sharded extxyz.gz + JSONL metadata store               [done]
```

Stages 2–4 also run as one overlapped, disk-paced command — `pipeline` — which splits
the keep-list into batches and runs `fetch` for batch *i+1* concurrently with
`parse`+`purge-raw` for batch *i*, so the network is never idle during parsing:

```
python -m zenodo_harvest.cli pipeline --in data/manifests/keep.jsonl \
    --parts 40 --workers 4 --max-bytes 0 --max-member-bytes 30000000000 \
    --max-disk-bytes 800000000000 --max-disk-files 800000 \
    --max-primary-bytes 4000000000
```

For cluster-scale parallel parsing there are four dataset-management subcommands:
`split` a manifest into N parts (one per array task), `merge-datasets` the per-task
dataset dirs into one, `verify` the merged metadata↔shard integrity + coverage
stats, and `purge-raw` the raw archives once their calcs are parsed.

Ready-to-edit CSD3 batch templates (with the cluster's wallclock/quota constraints
written down) live in [`scripts/csd3/`](scripts/csd3/README.md).

## Size, disk and memory controls

The harvest is much bigger than any one node's scratch (measured: ~2 TB of raw
archives for ~15–75 GB of final dataset), so fetch is paced rather than capped:

| Flag | Meaning |
|---|---|
| `--max-bytes` | skip any single file/archive larger than this; **`0` = uncapped** (the production setting — the transfer/storage lever is the disk valve, not a per-file cap) |
| `--max-member-bytes` | cap on each *extracted* file (decompression-bomb guard). Keep it generous (~30 GB) so long-AIMD `vasprun.xml`/`OUTCAR` — the frame-richest data — are not skipped |
| `--max-disk-bytes` | **disk budget** for the whole raw staging dir. Enforced on actual bytes as they are written (see below), so it is a hard bound |
| `--max-disk-files` | the same budget for **inodes** — files *and* directories, since CSD3's `/rds` is Lustre and a directory costs an inode too. On CSD3 this is the binding limit: `hpc-work` allows 1 TB *and* 1M files, while measured extracted VASP trees run ~7.6 KiB *median* per file, so 1M inodes can arrive near 0.3 TB |
| `--max-primary-bytes` | parse-side guard: refuse to *attempt* a `vasprun.xml`/`OUTCAR` bigger than this (`0` = no cap). pymatgen holds a whole trajectory in RAM, so on a batch scheduler one huge output can get the job cgroup-killed |
| `--workers` | concurrent record downloads in fetch (default 4). The main throughput lever |

### How the byte and inode limits are actually enforced

The hard part is that a download's size tells you almost nothing about how much disk it
will occupy: extracted VASP output is text, and measured expansion on real Zenodo records
ranged from **~1×** (already-compressed payloads) to **4.1×** (a 3.86 GB zip staging 15.9 GB
of `vasprun.xml`) — and a synthetic archive reached 880×. So nothing is predicted from a
ratio. Three layers:

1. **Prevent** — every byte and every inode is *charged in the ~1 MB chunk before it is
   written*, and refunded when deleted (an archive is refunded as soon as its VASP members
   are extracted, since its bytes are transient). A write that would breach a limit does not
   happen. Crucially the charge comes from **bytes as they land, never from a declared
   size** — not an archive header, not the Zenodo manifest, not `Content-Length` — because
   the production setting is `--max-bytes 0`, so the staging budget is the *only* bound on
   what reaches disk. `.7z` is the one format that decompresses in a single library call, so
   there the charge happens inside a writer handed to py7zr (`_BudgetedWriterFactory`).
   The invariant `staged ≤ limit` therefore holds for any compression ratio and any single
   file size, with no tuning constant and nothing taken on trust.
2. **Mitigate** — when a limit is reached, the record being staged is **rolled back whole**
   (a record is staged completely or not at all, so nothing partial is ever recorded as
   done) and fetch stops *cleanly and resumably*. `pipeline` then runs `parse` +
   `purge-raw` to turn staged files into dataset frames and reclaim the space, and
   **re-fetches the same batch**. That loop is what lets a ~2 TB harvest run inside a
   fixed quota. Two things keep the loop from spinning:
   - A refusal is classified by the **record's own footprint**, not by what happens to be
     staged beside it: if the record alone (including its transient archive-plus-extracted
     peak) exceeds the limit, no purge could ever help, so it is staged as far as it fits
     and reported (`record_exceeds_disk_budget`) instead of being retried for ever. This
     verdict is deterministic under `--workers N`, where a record almost never begins
     against an empty budget.
   - If a whole *parallel* pass stages nothing because concurrent records filled the budget
     between them, the run retries **serially** rather than handing back a stall.
   Anything an unrecorded record left behind is deleted and refunded on the spot —
   `purge-raw` only reclaims trees that reached the dataset, so a leftover would hold budget
   for the rest of the harvest.
3. **Survive** — if the real filesystem quota is hit anyway, the `ENOSPC`/`EDQUOT` write
   error is treated as **transient**, so those records are retried by a later run rather
   than being recorded as "contains no VASP" and skipped forever. The same applies to a
   record the budget refused: a space refusal never yields a terminal verdict, so raising
   the budget is always enough to collect it.

Every run reports its own `peak_staged_bytes`/`peak_staged_files`, so staying inside the
limits is verifiable from the summary rather than by watching from outside. Filling the
budget to ~98% is the *intended* outcome — safety comes from the check before each write,
not from leaving slack. Validated by 570 randomised end-to-end cases (compression ratios,
declared sizes wrong in both directions, 1–4 workers, both limits): no breach, and the
tally never drifts from what the filesystem reports.

Downloads are checksum-verified and **resume over HTTP Range**, so a job killed by its
wallclock does not restart a part-transferred 100 GB archive from byte 0.

## Quick start (WSL trial)

```bash
pip install -r requirements.txt          # stage 0-1 need only `requests`

python -m zenodo_harvest.cli discover \
    --query VASP --query OUTCAR --max-records 200 \
    --out data/manifests/candidates.jsonl

python -m zenodo_harvest.cli triage \
    --in data/manifests/candidates.jsonl \
    --out data/manifests/keep.jsonl --min-rank 3 --peek

pip install -e .[parse]                  # stage 2-4 add pymatgen + ase

python -m zenodo_harvest.cli fetch \
    --in data/manifests/keep.jsonl --max-bytes 500000000 --workers 4

python -m zenodo_harvest.cli parse \
    --in data/manifests/fetched.jsonl    # -> data/dataset/{shard-*.extxyz.gz,metadata.jsonl}

python -m zenodo_harvest.cli verify \
    --dataset-dir data/dataset           # integrity bijection + dataset stats
```

Each parsed ionic step becomes one extxyz frame with energy/forces/stress under MACE's
default **`REF_energy`/`REF_forces`/`REF_stress`** keys (train with those keys directly;
stress is Voigt-6 eV/Å³ in ASE's convention). Only openly-reusable licenses are kept by
default (`--no-license-gate` to disable). Set `ZENODO_TOKEN` to raise the rate limit.
Install `pip install -e .[archives]` to also harvest `.rar`/`.7z` uploads (rarfile also
needs an `unrar`/`bsdtar` binary). For a full harvest on the cluster, add `--exhaustive`
to `discover` (recursive date-partitioning past Zenodo's 10k search window).
