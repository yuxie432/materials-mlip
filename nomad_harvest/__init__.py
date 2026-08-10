"""NOMAD harvest — second data source for the MLIP training set.

A thin source adapter for NOMAD (https://nomad-lab.eu) that plugs into the *existing*
Zenodo pipeline: stages 0-2 (discover/dedup/fetch) are NOMAD-specific and live here;
stages 3-5 (parse → store → merge/verify) are the SHARED, unmodified
:mod:`zenodo_harvest` code, imported — never copied — so both sources produce one
schema-identical dataset.

Scope (mentor 2026-08-10): direct uploads only (exclude AFLOW/OQMD/Materials Project),
dedup against already-fetched Zenodo records, raw ``vasprun.xml`` re-parsed by the
existing pymatgen parser. See docs/NOMAD_HARVEST.md.
"""
