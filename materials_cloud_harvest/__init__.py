"""Materials Cloud harvest — third data source for the MLIP training set.

A thin source adapter for the Materials Cloud Archive (https://archive.materialscloud.org, an
InvenioRDM repository run by EPFL/MARVEL) that plugs into the *existing* Zenodo pipeline:
stages 0-1 (discover = full-census enumeration + gates, triage = zip/AiiDA-export peeks) are
Materials-Cloud-specific and live here; stage 2 (fetch) is the SHARED ``zenodo_harvest.fetch``
driven with an anonymous Materials Cloud session; stages 3-5 (parse → store → merge/verify) are
the SHARED, unmodified :mod:`zenodo_harvest` code, imported — never copied — so all sources
produce one schema-identical dataset (calc_ids namespaced ``materials_cloud:<record_id>:…``).

Scope (user decisions 2026-09-23, see docs/MATERIALS_CLOUD_HARVEST.md): enumerate ALL records
(no keyword recall limit); keep VASP-mentioning records fail-safe + any other record whose
zip / legacy-AiiDA-export central directory shows VASP outputs; extract VASP from legacy-format
AiiDA exports; include only the two VASP exports of the Bosoni ACWF verification record; admit
CC-BY-NC(-SA) but drop ND / no-licence / ``mcloud-ne-1.0`` / ``asl``.
"""
