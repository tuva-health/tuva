# CCSR v2025.1 seed data — publish to S3 before release

The CCSR seeds in this folder are header-only. Their data is loaded at
`dbt seed` time from the versioned `value-sets` bundle in public object storage
via the `load_versioned_seed` post-hooks in `dbt_project.yml`.

**This update bumps the CCSR mart from v2023.1 to v2025.1.** The v2025.1 data
files are NOT yet published to object storage. Until they are published, any
`dbt seed`/`dbt build` (including CI) will fail to load these three seeds.

## Required before merge / release

Publish these gzipped files to the `value-sets` bundle in all mirrored buckets
(S3, GCS, Azure), alongside the existing `value-sets/<bundle-version>/` files:

- `ccsr__dxccsr_v2025_1_body_systems.csv.gz`
- `ccsr__dxccsr_v2025_1_cleaned_map.csv.gz`
- `ccsr__prccsr_v2025_1_cleaned_map.csv.gz`

Object keys must match the post-hook filenames, e.g.
`s3://tuva-public-resources/value-sets/<bundle-version>/ccsr__dxccsr_v2025_1_cleaned_map.csv.gz`.
Bump `tuva_seed_versions.value_sets` if publishing under a new bundle version.

## Source and transformation

Generated from HCUP CCSR v2025-1 (ICD-10 codes valid through 2025-09-30):

- DXCCSR: `DXCCSR_v2025-1.zip` -> `DXCCSR_v2025-1.csv`
- PRCCSR: `PRCCSR_v2025-1.zip` -> `PRCCSR_v2025-1.csv`
- https://hcup-us.ahrq.gov/toolssoftware/ccsr/dxccsr.jsp
- https://hcup-us.ahrq.gov/toolssoftware/ccsr/prccsr.jsp

Cleaning applied to match the seed schema: strip the single quotes wrapping
codes, rename headers to snake_case, drop the DXCCSR "Rationale for Default
Assignment" column, and convert empty CCSR category cells to null. Row counts:
dxccsr cleaned map 75,238; prccsr cleaned map 82,121; body_systems 22.

`ccsr__dxccsr_v2025_1_body_systems` was carried over from v2023.1 (the CCSR
parent-category set is stable). Re-verify the parent-category counts in each
`parent_category_description` against `DXCCSR-Reference-File-v2025-1.xlsx`
before release.
