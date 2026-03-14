---
id: getting-started
title: "Getting Started"
hide_title: true
description: Instructions for getting started with the Tuva Project.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 🏁 Getting Started

To run Tuva you need to do the following:

1. Load your healthcare data (e.g. claims, EHR) into a data warehouse (e.g. Snowflake, Databricks)
2. Install [dbt](https://docs.getdbt.com/docs/core/installation-overview) -- a free open-source tool for transforming data inside your data warehouse
3. Create a new dbt project and connect that project to your data warehouse
4. Map your raw healthcare data to the Tuva [Input Layer](input-layer)
5. Import the Tuva package into your dbt project
6. Run the entire dbt project (i.e. execute `dbt build`)

Below we describe how to do this in more detail. If you do not have access to healthcare data yet, or if you just want to run Tuva locally first, start with the [demo](https://github.com/tuva-health/demo) project on DuckDB using the exact steps below.

## Run the Demo Locally with DuckDB

The commands below were validated locally against the current `tuva-health/demo` repository on March 14, 2026. In that validation run, `dbt deps` resolved `tuva-health/the_tuva_project` to `0.17.1`.

1. Clone the repo and move into it.

```bash
git clone https://github.com/tuva-health/demo.git
cd demo
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

This installs both `dbt-core` and the DuckDB adapter for dbt. You do not need a separate `pip install dbt-core`.

4. Create a local dbt profile for the demo project. The demo project's `dbt_project.yml` uses the profile name `default`, so define that profile in a user-managed profile directory instead of using the CI profiles under `integration_tests/profiles`.

```bash
mkdir -p .dbt
cat > .dbt/profiles.yml <<'EOF'
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DEMO_DUCKDB_PATH') }}"
      threads: 4
EOF
```

5. Point that local dbt profile at a DuckDB file.

```bash
export DBT_PROFILES_DIR="$PWD/.dbt"
export DEMO_DUCKDB_PATH="$PWD/demo.duckdb"
```

6. Verify that dbt can connect to DuckDB.

```bash
dbt debug
```

7. Install the dbt packages.

```bash
dbt deps
```

8. Build the demo project with claims, clinical, and provider attribution all enabled.

```bash
dbt build
```

This creates a local DuckDB database file at `demo.duckdb`. The build also downloads the synthetic input data, terminology, and value sets from `tuva-public-resources`, so you need an internet connection while it runs.

This full `dbt build` path was validated locally on March 14, 2026 against `tuva-health/the_tuva_project` `0.17.1` on DuckDB. The run completed successfully in about 41 minutes on a fresh local database file. You should still expect warning-level data quality output from the demo's synthetic source data, but the build finishes successfully with claims, clinical, and provider attribution all turned on.

After the build finishes, you can inspect the output locally. If you want to use the DuckDB command-line tool for that, install it first:

```bash
brew install duckdb
```

Then open the database:

```bash
duckdb demo.duckdb
```

```sql
select count(*) from core.patient;
select count(*) from core.encounter;
```

The video below is still a helpful walkthrough, but the commands above are the current runbook.

<div style={{maxWidth: '960px', width: '100%', margin: '0 auto'}}>
  <div style={{position: 'relative', paddingBottom: '56.25%', height: 0}}>
    <iframe
      src="https://www.youtube.com/embed/C6A1rxkqe_A?si=Rl74kyq9xhPiiVGL"
      title="YouTube video player"
      frameBorder="0"
      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
      referrerPolicy="strict-origin-when-cross-origin"
      allowFullScreen
      style={{position: 'absolute', top: 0, left: 0, width: '100%', height: '100%'}}
    />
  </div>
</div>

