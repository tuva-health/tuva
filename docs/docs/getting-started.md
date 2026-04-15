---
id: getting-started
title: "Getting Started"
hide_title: true
description: Instructions for getting started with the Tuva Project.
---

import TuvaCurrentRelease from '@site/src/components/TuvaCurrentRelease';

# 🏁 Getting Started

There are two main ways to get started with Tuva:

1. **Getting Started with Synthetic Data**: the fastest way to try Tuva, inspect the resulting schemas, and develop locally against versioned synthetic inputs.
2. **Getting Started with Real Data**: the production path for running Tuva on your own warehouse and mapped source data.

## 1. Getting Started with Synthetic Data

This path uses the Tuva repo itself and runs the `integration_tests` project. It is the best option if you want to evaluate Tuva quickly, inspect the output data model, or develop against a working package setup without first mapping your own data.

`integration_tests` is synthetic-only:

- `dbt seed` and `dbt build` load versioned synthetic data into the `raw_data` schema.
- `dbt run` assumes those `raw_data` tables already exist.
- On a fresh database, run `seed` or `build` before `run`.

This path is for evaluation, demos, and development. It is not the path for loading your own source data.

### Local DuckDB Setup From Scratch

1. Clone the Tuva repo and move into it.

```bash
git clone https://github.com/tuva-health/tuva.git
cd tuva
```

2. Create and activate a Python virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

3. Install dbt with the DuckDB adapter.

```bash
python -m pip install dbt-duckdb
```

4. Create a local dbt profile. The repo helper script expects a profile named `default` unless you set `TUVA_DBT_PROFILE`.

```bash
mkdir -p .dbt
cat > .dbt/profiles.yml <<'EOF'
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('TUVA_DUCKDB_PATH') }}"
      schema: main
      threads: 4
EOF
```

5. Point the profile at a local DuckDB file.

```bash
export DBT_PROFILES_DIR="$PWD/.dbt"
export TUVA_DUCKDB_PATH="$PWD/tuva.duckdb"
```

6. Run the synthetic integration project from the repo root.

```bash
./scripts/dbt-local deps
./scripts/dbt-local build --full-refresh
./scripts/dbt-local run
```

The default synthetic input size is `small`. To use the larger synthetic payload, rerun the build with:

```bash
./scripts/dbt-local build --full-refresh --vars '{synthetic_data_size: large}'
```

If you want to load synthetic data without running the full graph, use:

```bash
./scripts/dbt-local seed --full-refresh
```

### Inspect The Resulting Data Model

After the build completes, inspect the generated schemas and tables with your preferred SQL client. For example:

```sql
select schema_name
from information_schema.schemata
where schema_name not in ('information_schema', 'pg_catalog', 'main')
order by 1;

select count(*) from raw_data.medical_claim;
select count(*) from core.patient;
```

### Use Synthetic Data In An Existing Warehouse

You can run the same synthetic path against an existing warehouse such as Snowflake, Databricks, BigQuery, Redshift, or Fabric.

1. Install the correct dbt adapter for your warehouse.
2. Point a `default` dbt profile at that warehouse.
3. Run the same `integration_tests` commands from this repo:

```bash
./scripts/dbt-local deps
./scripts/dbt-local build --full-refresh
```

If you are using dbt Cloud or plain dbt CLI instead of `./scripts/dbt-local`, set the project directory to `integration_tests`.

For warehouse-specific setup guidance, see [Supported Data Warehouses](./data-warehouse-support/data-warehouse-support-overview.md).

## 2. Getting Started with Real Data

This is the normal path for running Tuva on your own data in your existing warehouse.

### Prerequisites

- A working dbt project
- A warehouse connection already configured in `profiles.yml`
- Your raw claims data already loaded into the warehouse

The current release of Tuva is <TuvaCurrentRelease />.

### Step 1: Create Or Use A dbt Project

Create a new dbt project if you do not already have one, or use an existing project that is already connected to your warehouse.

### Step 2: Add The Tuva Package

Add the Tuva package to your `packages.yml`. Replace `<current-release>` with the release shown above.

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: "<current-release>"
```

Then install the package:

```bash
dbt deps
```

### Step 3: Map Your Source Data To The Tuva Input Layer

Map your warehouse tables to the Tuva [Input Layer](./input-layer.mdx). For a claims-first implementation, you should create the Input Layer models for:

- `eligibility`
- `medical_claim`
- `pharmacy_claim`

If you later want to run clinical models or provider attribution, map those Input Layer sub-parts as well.

### Step 4: Configure Tuva Vars

Set the broad enablement vars in your `dbt_project.yml`.

```yaml
vars:
  claims_enabled: true
  # clinical_enabled: true
  # provider_attribution_enabled: true
```

Start with `claims_enabled: true` for a claims-only implementation. Enable `clinical_enabled` or `provider_attribution_enabled` only after those corresponding Input Layer tables are mapped.

For more detail, see [dbt Variables](./dbt-variables.md).

### Step 5: Run Tuva

Once your Input Layer mapping is in place, run:

```bash
dbt build
```

That will build the Tuva package on top of your warehouse data, load the required Tuva seed data, and run the package tests.

### References

- [Input Layer](./input-layer.mdx)
- [dbt Variables](./dbt-variables.md)
- [Supported Data Warehouses](./data-warehouse-support/data-warehouse-support-overview.md)
