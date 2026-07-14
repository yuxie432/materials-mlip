# zenodo_harvest

Harvest openly-licensed DFT (VASP) calculation data from Zenodo and assemble it
into a compact, provenance-rich dataset for training a machine-learning
interatomic potential (MLIP). See [`docs/DESIGN.md`](docs/DESIGN.md) for the data
model, storage format, and coverage/quality strategy, and [`CLAUDE.md`](CLAUDE.md)
for domain conventions.

## Pipeline

```
stage 0  discover  keyword search          -> candidate manifest (JSONL)  [done]
stage 1  triage    file-listing + zip-peek -> keep-list (JSONL)           [done]
stage 2  fetch     download archives (checksum-verified) + extract VASP   [done]
stage 3  parse     pymatgen Vasprun/Vaspout / ASE OUTCAR -> frames        [done]
stage 4  store     sharded extxyz.gz + JSONL metadata store               [done]
```

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
    --in data/manifests/keep.jsonl --max-bytes 500000000

python -m zenodo_harvest.cli parse \
    --in data/manifests/fetched.jsonl    # -> data/dataset/{shard-*.extxyz.gz,metadata.jsonl}
```

Set `ZENODO_TOKEN` to raise the rate limit. For a full harvest on the cluster,
add `--exhaustive` to `discover` (recursive date-partitioning past Zenodo's 10k
search window).
