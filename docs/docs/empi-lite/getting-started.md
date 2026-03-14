# Getting Started

This guide walks you through setting up EMPI Lite from a fresh clone to your first successful `dbt run`. Estimated time: **1-2 hours** if your source tables are already available.


## Prerequisites

Before you start, confirm you have:

- [ ] **dbt Core ≥ 1.7** installed (`dbt --version`)
- [ ] The appropriate **dbt adapter** for your warehouse (`dbt-snowflake`, `dbt-bigquery`, `dbt-redshift`, `dbt-databricks`, or `dbt-duckdb`)
- [ ] Warehouse credentials and a database/schema where EMPI Lite can create tables
- [ ] Source data: an `eligibility` table, a `patient` table, or both (see [Data Requirements](./data-requirements.md))
- [ ] Permission to create schemas and tables in your warehouse


## Step 1 - Clone the repo

```bash
git clone <your-repo-url> empi_lite
cd empi_lite/empi_lite    # all dbt commands run from the inner directory
```

> The double directory `empi_lite/empi_lite/` is intentional. The inner directory is the dbt project root. Always run `dbt` commands from there.


## Step 2 - Configure your dbt profile

Add a profile named `empi_lite` to `~/.dbt/profiles.yml`. Profile templates for all supported warehouses are below.

### Snowflake

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>           # e.g., xy12345.us-east-1
      user: <your_user>
      password: <your_password>
      role: <your_role>
      warehouse: <your_warehouse>
      database: <your_database>
      schema: empi
      threads: 4
```

### BigQuery

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth                     # or service-account
      project: <your_gcp_project>
      dataset: empi
      threads: 4
      timeout_seconds: 300
```

### Amazon Redshift

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: redshift
      host: <your_cluster>.redshift.amazonaws.com
      user: <your_user>
      password: <your_password>
      port: 5439
      dbname: <your_database>
      schema: empi
      threads: 4
```

### Microsoft Fabric / Synapse

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: ODBC Driver 18 for SQL Server
      server: <your_server>.sql.azuresynapse.net
      port: 1433
      database: <your_database>
      schema: empi
      authentication: CLI               # or ActiveDirectoryPassword
      threads: 4
```

### Databricks

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <your_workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<your_warehouse_id>
      token: <your_pat_token>
      catalog: <your_catalog>
      schema: empi
      threads: 4
```

### DuckDB

```yaml
empi_lite:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/empi.duckdb       # use ':memory:' for in-memory
      schema: empi
      threads: 4
```


## Step 3 - Create the manual review tables

EMPI Lite reads from two tables in the `empi_manual_review` schema. They must exist (even if empty) before the first `dbt run`. You can either run the DDL below or use the project macro: `dbt run-operation create_manual_review_tables`.

```sql
CREATE SCHEMA IF NOT EXISTS your_database.empi_manual_review;

CREATE TABLE IF NOT EXISTS your_database.empi_manual_review.match_review_decisions (
    data_source_a       VARCHAR     NOT NULL,
    source_person_id_a  VARCHAR     NOT NULL,
    data_source_b       VARCHAR     NOT NULL,
    source_person_id_b  VARCHAR     NOT NULL,
    is_match            BOOLEAN,              -- TRUE = match, FALSE = no match, NULL = pending
    reviewed            BOOLEAN,              -- FALSE = pending, TRUE = decision finalized
    reviewer_name       VARCHAR,
    review_date         DATE,
    notes               VARCHAR
);

CREATE TABLE IF NOT EXISTS your_database.empi_manual_review.split_review_decisions (
    data_source         VARCHAR     NOT NULL,
    source_person_id    VARCHAR     NOT NULL,
    is_split            BOOLEAN,               -- TRUE = split, FALSE = not split, NULL = pending
    reviewed            BOOLEAN,               -- FALSE = pending, TRUE = decision finalized
    reviewer_name       VARCHAR,
    review_date         DATE,
    notes               VARCHAR
);
```


## Step 4 - Point EMPI Lite at your source tables

Open `models/sources.yml` and update the database/schema vars to match where your source tables live:

```yaml
# models/sources.yml (excerpt)
sources:
  - name: empi_input
    database: "{{ var('empi_input_database') }}"
    schema:   "{{ var('empi_input_schema') }}"
    ...
```

Then set the corresponding vars in `dbt_project.yml`:

```yaml
# dbt_project.yml
vars:
  empi_lite:
    empi_input_database: your_database
    empi_input_schema:   your_schema

    # Table identifiers (if your table names differ from the defaults)
    empi_eligibility_table: eligibility
    empi_patient_table:     patient

    # Manual review schema
    empi_manual_review_database: your_database
    empi_manual_review_schema:   empi_manual_review
```

If you only have one source type (e.g., eligibility only, no patient table), the staging model handles this automatically - the `patient` source is optional.


## Step 5 - Install dbt packages

```bash
dbt deps
```

This installs `dbt_utils` (the only external dependency).


## Step 6 - Load seeds

Load all reference data seeds:

```bash
# Load all seeds except the large static ones (ZIP proximity table, eval ground truth)
dbt seed --exclude tag:large_seed

# Load the ZIP code proximity table (only needed once; takes a few minutes on first load)
dbt seed --select gaz2024zcta5distance50miles --full-refresh
```

> **Tip:** After the initial load, the ZIP proximity table never needs to be reloaded unless you explicitly want to refresh it. `dbt seed --exclude tag:large_seed` is the correct command for all routine runs.


## Step 7 - Run the project

```bash
dbt run
```

This builds all staging, intermediate, and final models. On first run with a large dataset, expect this to take 15-45 minutes depending on patient volume and warehouse size.

You can monitor build progress with:

```bash
dbt run --select tag:final    # build only final output tables
dbt run --select empi_crosswalk empi_golden_record    # build specific tables
```


## Step 8 - Run tests

```bash
dbt test
```

All tests should pass. Common failure causes on first run:

- Source tables missing required columns → add the columns (nulls allowed)
- Manual review tables don't exist → re-run the DDL from Step 3
- Profile misconfiguration → check `dbt debug` output


## Step 9 - Validate your results

Open the output tables and spot-check match quality:

```sql
-- How many records matched vs. singletons?
SELECT match_status, COUNT(*) as records
FROM empi.empi_crosswalk
GROUP BY match_status;

-- How large are the clusters?
SELECT cluster_size, COUNT(DISTINCT empi_id) as clusters
FROM empi.empi_crosswalk
GROUP BY cluster_size
ORDER BY cluster_size;

-- Review a sample of matches with their narratives
SELECT *
FROM empi.empi_patient_events
WHERE event_type = 'EMPI_MATCH'
LIMIT 25;

-- Check data quality anomalies
SELECT *
FROM empi.empi_demographic_anomalies
ORDER BY affected_records DESC;
```

If match quality looks off (too many or too few matches), see the [Configuration Guide](./configuration.md) for threshold tuning.


## Step 10 - Enable snapshots (optional but recommended)

Snapshots enable change-detection events in `empi_patient_events` - you'll see `EMPI_ID_CHANGED`, `MATCH_STATUS_CHANGED`, and `SOURCE_DATA_UPDATED` events across runs.

Run snapshots after the first `dbt run`:

```bash
dbt snapshot
```

Then enable snapshot events in `dbt_project.yml`:

```yaml
vars:
  empi_lite:
    empi_snapshot_enabled: true
```

On all subsequent runs, execute in order:

```bash
dbt snapshot && dbt run
```


## Installation pattern A - Local package import

If you want EMPI Lite to run as part of your **existing dbt project** rather than as a standalone project, use the local package import pattern.

In your existing dbt project's `packages.yml`:

```yaml
packages:
  - local: ../empi_lite/empi_lite    # path relative to your project root
```

Then run `dbt deps` in your existing project. EMPI Lite models will build alongside your existing models in a single `dbt run`.

Your existing project's `dbt_project.yml` must include the EMPI Lite vars:

```yaml
vars:
  empi_lite:
    empi_input_database: your_database
    empi_input_schema: your_schema
    match_threshold: 0.70
    review_threshold_low: 0.50
    review_threshold_high: 0.69
    empi_snapshot_enabled: false
    empi_custom_attributes_enabled: false
```


## Routine operations

Once set up, the ongoing operational cadence is:

```bash
# On each source data refresh:
dbt snapshot && dbt run

# Process review queue (ad hoc or scheduled):
# 1. Query empi_review_queue_matches and empi_review_queue_splits
# 2. Insert decisions into empi_manual_review tables
# 3. Re-run: dbt run
```


## Upgrading to a new version

When a new version of EMPI Lite is released:

1. Pull the latest code from the upstream repo
2. Do **not** overwrite:
   - `seeds/empi_blocking_rules.csv` (if you've customized blocking groups)
   - `seeds/empi_attribute_scores.csv` (if you've tuned weights)
   - `seeds/empi_invalid_values.csv` (if you've added custom invalid values)
   - `dbt_project.yml` (contains your vars)
   - `models/sources.yml` (contains your source configuration)
3. Safe to overwrite: all `models/` SQL files, `macros/`, `snapshots/`, `tests/`
4. Run `dbt deps && dbt seed --exclude tag:large_seed && dbt run`

> **Recommended:** Keep your customizations on a dedicated git branch. Merge new versions into `main` and cherry-pick or rebase your branch.
