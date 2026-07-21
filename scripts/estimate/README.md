# Estimating accessible + relevant VASP data on Zenodo (and its storage cost)

Tooling to answer two questions **using the current harvest scripts**:

1. **How much** accessible, relevant VASP data is discoverable on Zenodo?
2. **How much storage** would it need — to transfer, to stage, and as the final
   `extxyz.gz` training dataset?

The method separates what is **exact** from what must be **measured**:

| Quantity | How obtained | Certainty |
|---|---|---|
| Record counts by category | discover manifest (the search API returns them) | **exact** for what the scripts find |
| Raw byte footprint / transfer volume | manifest embeds every file's `size` | **exact** (per cap) |
| Extract ratio, frames/record, bytes/frame | real fetch + parse of a sample | measured (± a factor) |
| Final dataset size | census × measured ratios | projected |

The one thing none of this can capture is the **metadata-only recall ceiling**:
`q` searches title/description/keywords, not file contents, so VASP data whose
metadata never says so is invisible to discovery. Every number here is therefore a
**lower bound** on what truly sits on Zenodo (see Caveats).

## Files

- `census.py` — exact byte census from manifest(s): counts + raw bytes by category,
  a **download-vs-cap sweep**, archive-format split, heavy-tail. Writes JSON.
- `sample_storage.py` — fetch + parse a size-stratified sample of rank≥3 records;
  measures **fetch yield** and the **ratios** (extract, frames/record, bytes/frame).
- `project.py` — census JSON × ratios JSON → **final storage projection** per cap.
- `slurm_discover.sh`, `slurm_sample.sh` — CSD3 SLURM templates.

## Procedure (CSD3)

Prereqs: `pip install -e .[parse]` (needs `pymatgen`+`ase` for the sample);
`ZENODO_TOKEN` in `.env` or the environment; point `ZENODO_HARVEST_DATA` at scratch.

### 1. Exhaustive discovery (the only slow, network-bound step)

```bash
sbatch scripts/estimate/slurm_discover.sh          # or run in tmux on a login node
```

Enumerates **all** default queries with recursive date-bisection past the 10k
window. Serial + polite (~30 req/min even with a token — more cores do not help).
**Resumable**: resubmit and it continues from the sidecar checkpoint. Expect
~1–2 h, dominated by the one broad `"ab initio" molecular dynamics forces` query
(OR-default → ~53k hits; the 11 precise queries together are <2k and finish in
minutes).

Output: `…/manifests/candidates_full.jsonl` (deduplicated, classified, gated).

### 2. Census (instant, no network)

```bash
python scripts/estimate/census.py \
    "$ZENODO_HARVEST_DATA/manifests/candidates_full.jsonl" \
    --json "$ZENODO_HARVEST_DATA/estimate_sample/census.json"
```

Answers question 1 (counts) and gives the exact transfer volume at each cap.

### No-cap harvest on a bounded disk (e.g. 1 TB scratch)

The final `extxyz.gz` + `metadata.jsonl` dataset is tiny (~tens of GB), so **run
uncapped** (`fetch --max-bytes 0`) to not miss data locked in big archives. The only
large, *transient* thing is the downloaded archive — deleted the moment its VASP files
are extracted. To keep peak disk well under quota, **pipeline in batches** rather than
fetching everything then parsing everything:

```bash
# per batch (or per SLURM array task on its own manifest part):
python -m zenodo_harvest.cli fetch  --in part.jsonl --max-bytes 0 --raw-dir "$RAW"
python -m zenodo_harvest.cli parse  --in "$MAN/fetched.jsonl" --dataset-dir "$DS" --raw-dir "$RAW"
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS"   # reclaim
```

Peak disk ≈ (largest single archive in flight) + (extracted files not yet purged) —
kept small by peek dropping non-VASP zips before download and by purging each batch.
`--max-member-bytes` still guards individual extracted files (raise it above 2 GB for
very long AIMD `vasprun.xml`).

### 3. Storage sample (fetch + parse; question 2's ratios)

```bash
sbatch scripts/estimate/slurm_sample.sh
```

Fetches + parses a stratified sample. **Raise `--cap-gb` / `--n` to tighten the
estimate**: yield and frames/record both rise with the cap because large
trajectories (the bulk of all frames) live in big archives. Re-run at your intended
production cap.

### 4. Project

```bash
python scripts/estimate/project.py \
    "$ZENODO_HARVEST_DATA/estimate_sample/census.json" \
    "$ZENODO_HARVEST_DATA/estimate_sample/ratios.json"
```

Prints transfer / staging / dataset size for each cap.

## Interpreting + caveats

- **"Relevant" = rank ≥ 3** (`vasp_direct` + `archive`), what triage keeps by
  default. `archive` records are only *confirmed* to hold VASP by the default zip
  peek (or at fetch for tar/rar/7z), so the fetch **yield** (measured in step 3)
  discounts archives that turn out to hold no parseable primary output. Measured
  2026-07-21: only ~24% of peekable-zip archives actually contain a vasprun/OUTCAR.
- **The per-file cap is the dominant storage lever.** Raw bytes are extremely
  heavy-tailed — a handful of multi-GB archives hold most of the volume — so the
  transfer/dataset totals swing by >10× between a 0.5 GB and an uncapped policy.
  Choose the cap deliberately.
- **Recall ceiling (lower bound):** metadata-only search misses VASP data with
  silent metadata; the broad OR query also *truncates* any single-day window with
  >10k hits (observed: 2017-10-23). `discover --community`/OAI-PMH would raise recall
  (future work).
- **License + access gates** already applied in discovery drop ~non-open and
  NC/ND/no-license records (recorded in the discover summary).
- **`processed_atomistic`** (uploaded extxyz/xyz/traj) is counted but **not ingested**
  (no reader yet), so it contributes 0 to the projected dataset.
