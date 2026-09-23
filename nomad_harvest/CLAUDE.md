## Second data source: NOMAD (`nomad_harvest/`)

A second source adapter harvests VASP data from **NOMAD** (https://nomad-lab.eu) into the
*same* `data/dataset/` schema, so both sources feed one MLIP training set. Full design +
live-verified API facts: `docs/NOMAD_HARVEST.md`. Scope (mentor): **direct uploads only**
(~7.1M VASP-DFT entries; AFLOW/OQMD/MP excluded — MP is harvested via `mp-api`), **raw
`vasprun.xml` re-parsed by the existing `parse.py`** (not NOMAD's normalized archive, whose
σ→0 energy label and stress sign are unreliable), **vasprun-only** (no wholesale OUTCAR — its
tail is ~170 TB).

- `nomad_harvest/` imports the shared `zenodo_harvest` **stages 3-5 (parse/store/verify/merge)
  unmodified**; only stages 0-2 are NOMAD-specific:
  - `client.py` — throttled, retrying NOMAD v1 REST client (anonymous reads; **keyset
    pagination only** — offset caps at 10k; self-throttle + exponential backoff on
    502/503/504). Discover/per-entry-fallback use `entries/*`. **`upload_raw_get`** does a
    paced, 429-retrying **Range GET of `GET /uploads/{id}/raw`** (the pre-packed upload zip) —
    that endpoint's limit is **1 in-flight connection per IP, a new one every ~5 s** (a separate,
    stricter bucket than `entries/*`), so it is serialised. No bulk `entries/raw/query` methods.
  - `upload_zip.py` — **targeted extraction from an upload's PRE-PACKED zip over HTTP Range.**
    NOMAD stores each published upload as one `raw-public.plain.zip`; `read_central_directory`
    reads the zip tail (suffix Range) and parses its **ZIP64-aware** central directory (the big
    uploads are >4 GB), and `fetch_members` **multi-range-pulls the wanted members by byte
    offset** (~250/request, ~8 KB Range-header cap), CRC-verified, mapping each by file offset
    (robust to the server coalescing adjacent ranges). Members are STORED → exact bytes, zero
    server assembly. The NOMAD analog of `zenodo_harvest/zipstream.py`, extended for ZIP64.
  - `harvest.py` — stage 0 discover (keyset-scan → license-gate + Zenodo-`references`-dedup →
    slimmed keep-list) and stage 2 fetch. **`fetch_candidates` groups the keep-list by
    `upload_id`** (`split_by_upload` keeps each upload whole in one pipeline part → its central
    directory is read once), then per upload reads the CD and fetches each entry's `mainfile`
    member (vasprun, or OUTCAR for OUTCAR-mainfile entries; `--want-outcar` also grabs a sibling
    OUTCAR). **The fetch is REQUEST-bound, not bandwidth-bound** (the endpoint costs ~5 s/request,
    1-in-flight/5s per IP — confirmed on CSD3 compute; both nodes share one NAT egress IP, so it is
    intrinsically serial and multi-node/parallel cannot help without a NOMAD rate-limit exemption),
    so `_fetch_upload` picks **per upload** between two mechanisms via `_should_whole_stream`
    (a RATE-INDEPENDENT bloat cap — whole unless it would over-fetch >2× the wanted bytes, since
    the achieved MB/s swings 4-37× with load so no measured rate is trustworthy): **targeted**
    multi-range (`upload_zip.fetch_members`,
    ≤256 members/request — the 8 KB Range-header cap) for high-bloat or few-entry uploads, or
    **whole-stream** (`upload_zip.stream_members`) — ONE transfer-bound request spanning all the
    wanted members, extracting each from the stream by offset (interior bloat streamed past and
    discarded, so peak disk == wanted bytes) — for LOW-bloat, many-entry uploads. The hybrid
    collapses ceil(n/256)+1 throttled requests/upload into ~2 for the low-bloat majority (survey
    2026-08-19: ~90% of entries live in uploads whose vaspruns are >0.7 of the bytes, so
    whole-stream sends 62-71% of entries and adds only ~5% transfer) → **~2-4 days vs the ~9-day
    all-targeted floor, no exemption**. Only the FETCH mechanism differs; staged files, calc_units,
    availability and provenance are byte-identical either way (validated live on a real ZIP64
    upload). Disk/inode-paced by the shared `StagingBudget` (reserving each entry's EXACT footprint
    from the CD), manifest resume, `stopped_disk_budget` for the pipeline. **Serial**; an
    upload/member the pre-packed path can't deliver **falls back to the per-entry
    `entries/{id}/raw` path** (a separate throttle bucket) → no coverage loss. **Availability is derived from the upload's central directory** (the zip's
    own per-calc file list) OR'd with NOMAD's parsed `available_properties` (`dos_electronic[_new]`
    →`dos`, `band_structure_electronic`→`eigenvalues`) + the parse-time embedded-vasprun probe —
    so the old fragile `rawdir/query` step is gone. `available_properties` is kept by
    `slim_candidate`; the unreliable `trajectory` property is not mapped.
  - `cli.py` — `python -m nomad_harvest.cli {discover,fetch,pipeline,status,smoke}`. `pipeline`
    splits the keep-list **by upload** (`split_by_upload`) and drives the **shared** `run_pipeline`
    (fetch batch *i+1* ∥ parse+purge batch *i*), disk-paced, into its own `data/dataset/nomad`
    dir; `merge-datasets` folds it in. No `--workers`/`--batch-size` (the fetch is serial).
  - `smoke.py` — live end-to-end Phase-0 validation in an isolated temp dir.
- **The shared parser namespaces by source.** `parse._calc_id`/`_frame` derive the source from
  `provenance.source` (`_source_of`, default `"zenodo"`), so NOMAD frames are tagged
  `source="nomad"` and calc_ids are `nomad:<entry_id>:…` — byte-identical for Zenodo, no
  cross-source id collision at `verify`/`merge-datasets`. No CLI flag; the provenance field
  drives it.
- CSD3 batch templates: `scripts/csd3/nomad/{10_discover,20_pipeline}.sh` (single-stream discover,
  then the overlapped disk-paced pipeline, self-resubmitting) + `csd3_nomad_prepacked_probe.py`
  (confirm the pre-packed fetch MB/s + throttle from a compute node). Full harvest of 7.1M is
  **targeted Range-extraction from ~3,792 upload zips** (~30k requests at ~15–30 MB/s) →
  **~1.5–3 days**, one self-resubmitting campaign. It is **serial** (the `/uploads/{id}/raw`
  1-conn/5s limit — no `--workers`, and CSD3's shared NAT can't parallelise it). Slicing with
  `--max-entries` is OPTIONAL. A **token does NOT help** (per-IP limit; the endpoint is anonymous);
  the one accelerator is a rate-limit **exemption** from `support@nomad-lab.eu` (lifting the per-IP
  concurrency cap → hours), optional not required.
- Offline tests: `tests/test_nomad.py` (network-free — query builder, keyset paging, backoff,
  dedup, staging-name logic, ZIP64 central-directory parse + multi-range extraction + CRC against
  an in-memory pre-packed zip, the disk-valve/resume/fallback of `fetch_candidates`, and
  `split_by_upload`). Live path: `python -m nomad_harvest.cli smoke -n 12`.
