"""Zenodo census: find the VASP data that keyword discovery cannot see.

``zenodo_harvest.discover`` finds a record only if its metadata TEXT says DFT/VASP, but nearly all
VASP output sits inside archives and many deposits carry bare metadata ("Accompanying data for
<paper>"). This package replaces the keyword front-end with a census of every Zenodo record that
could hold VASP output (any archive, or a loose VASP primary), scores each record offline from
several independent signals (robust local text matching, known-VASP depositors and communities,
linked papers that cite the VASP method papers, Europe PMC data-availability mentions), peeks the
archives of the plausible ones (ZIP64-aware central directories; a head-peek of tar-family
streams), and emits an ORDINARY Zenodo keep-list for ``zenodo_harvest.cli pipeline`` — so the
fetch/parse/store path, the schema and the calc_ids are exactly those of the existing Zenodo
dataset. Design, measurements and decisions: ``docs/ZENODO_CENSUS.md``.
"""
