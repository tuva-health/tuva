# Data Requirements

EMPI Lite requires one or both of the following source tables: **`eligibility`** and **`patient`**. You configure where EMPI Lite finds these tables using vars in `dbt_project.yml` - no model code changes required.

At a minimum, you need either `eligibility` or `patient`. Most implementations have both.


## Understanding the two source types

These two tables represent fundamentally different kinds of source data:

**`eligibility`** - Medical claims / insurance enrollment data. One row per enrollment span. The same person typically has multiple rows - one for each month or contract period they were enrolled. This is the primary data source for organizations working primarily with claims data.

**`patient`** - Clinical / EHR data. One row per patient. A single authoritative demographic record per person from a clinical system. Patient records are preferred over eligibility records in golden record resolution when both exist in the same cluster.

Most implementations have both: claims enrollment data contributes demographic history over time, while EHR patient records provide cleaner, more complete demographics.


## Using EMPI Lite alongside Tuva

EMPI Lite is designed to run **alongside** the Tuva pipeline, not instead of it. The two are complementary: EMPI Lite resolves patient identity, and Tuva builds clinical and financial marts on top of that resolved identity.

The integration works as follows:

```
Your raw source data
    ↓
EMPI Lite (dbt run)
    → produces tuva_patient and tuva_eligibility in your warehouse
      with person_id = empi_id
    ↓
Your Tuva pipeline
    → reads from EMPI Lite's output tables instead of your raw source tables
    → all downstream Tuva marts inherit empi_id as the person identifier
```

**What this means for your existing Tuva setup:** If you are already running Tuva, you will need to reconfigure your Tuva pipeline's input layer to point at the EMPI-resolved `tuva_patient` and `tuva_eligibility` tables rather than your original raw source tables. This is a one-time reconfiguration - after it is done, every Tuva mart automatically uses the resolved `empi_id`.

EMPI Lite and Tuva can run in the same dbt project, with EMPI Lite running first. Point EMPI Lite at your raw source tables via the `empi_input_database` / `empi_input_schema` vars, and point your Tuva input layer at the EMPI Lite output schema.


## Quick compatibility check

**If you already use the Tuva input layer:** Your existing raw `eligibility` and `patient` tables are the inputs to EMPI Lite. After EMPI Lite runs, it produces Tuva-compatible output tables (`tuva_patient`, `tuva_eligibility`) where `person_id = empi_id`. You then reconfigure your Tuva connector to read from those output tables instead of your originals.

**If you are mapping from custom source tables:** Create staging views or transformation models that conform to the column specs below, and point EMPI Lite at those views.


## eligibility table

Represents enrollment/insurance spans. This is the primary source for most implementations.

### Required columns

These columns must exist. They drive matching or identity assignment.

| Column | Type | Description |
|---|---|---|
| `data_source` | VARCHAR | Name of the source system (e.g., `CLAIMS_PAYER`, `MEDICAID`). This value becomes the `data_source` key in all EMPI outputs. |
| `person_id` | VARCHAR | Patient identifier in the source system. This becomes `source_person_id` in EMPI outputs. |

### Demographic columns (used for matching)

All are nullable. Missing values simply do not contribute to or detract from match scores.

| Column | Type | Notes |
|---|---|---|
| `first_name` | VARCHAR | |
| `last_name` | VARCHAR | |
| `birth_date` | DATE | |
| `death_date` | DATE | |
| `social_security_number` | VARCHAR | Any format; normalized internally (dashes, spaces removed) |
| `sex` or `gender` | VARCHAR | `'male'` \| `'female'` \| `'unknown'` |
| `race` | VARCHAR | |
| `address` | VARCHAR | Street address |
| `city` | VARCHAR | |
| `state` | VARCHAR | 2-letter code or full name; normalized internally |
| `zip_code` | VARCHAR | 5-digit preferred; ZIP+4 accepted |
| `phone` | VARCHAR | Any format; normalized internally (digits only) |

### Enrollment columns (used for event timeline)

| Column | Type | Description |
|---|---|---|
| `enrollment_start_date` | DATE | Start of the enrollment span. |
| `enrollment_end_date` | DATE | End of the enrollment span. Null = currently active. |

### Optional pass-through columns

These are not used by the matching engine but are passed through to the Tuva-compatible output tables.

| Column | Type |
|---|---|
| `middle_name` | VARCHAR |
| `name_suffix` | VARCHAR |
| `ethnicity` | VARCHAR |
| `death_flag` | BOOLEAN |
| `member_id` | VARCHAR |
| `subscriber_id` | VARCHAR |
| `subscriber_relation` | VARCHAR |
| `payer` | VARCHAR |
| `payer_type` | VARCHAR |
| `plan` | VARCHAR |
| `enrollment_status` | VARCHAR |
| `group_id` | VARCHAR |
| `group_name` | VARCHAR |

### Full DDL

The DDL below uses standard SQL types. See the warehouse-specific notes that follow for type substitutions.

```sql
CREATE TABLE your_database.your_schema.eligibility (

    -- Identity (required)
    data_source             VARCHAR     NOT NULL,
    person_id               VARCHAR     NOT NULL,

    -- Demographics (used for matching, all nullable)
    first_name              VARCHAR,
    middle_name             VARCHAR,
    last_name               VARCHAR,
    name_suffix             VARCHAR,
    gender                  VARCHAR,
    race                    VARCHAR,
    ethnicity               VARCHAR,
    birth_date              DATE,
    death_date              DATE,
    death_flag              BOOLEAN,
    social_security_number  VARCHAR,
    address                 VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    zip_code                VARCHAR,
    phone                   VARCHAR,
    email                   VARCHAR,

    -- Enrollment spans
    enrollment_start_date   DATE,
    enrollment_end_date     DATE,

    -- Payer / plan (optional pass-through)
    member_id               VARCHAR,
    subscriber_id           VARCHAR,
    subscriber_relation     VARCHAR,
    payer                   VARCHAR,
    payer_type              VARCHAR,
    plan                    VARCHAR,
    enrollment_status       VARCHAR,
    group_id                VARCHAR,
    group_name              VARCHAR
);
```

**Warehouse-specific type notes:**
- **BigQuery:** Replace `VARCHAR` with `STRING`. Replace `BOOLEAN` with `BOOL`.
- **Microsoft Fabric / SQL Server:** Replace `BOOLEAN` with `BIT`. Store as `0`/`1`.
- **Snowflake, Redshift, Databricks, DuckDB:** DDL above works as written.


## patient table

Represents clinical patient demographics from an EHR or similar system. The schema mirrors `eligibility` but without enrollment span columns.

Patient records are **preferred over eligibility records** in golden record resolution - if a patient record exists in the cluster, its demographics are used as the primary source.

### Required columns

| Column | Type | Description |
|---|---|---|
| `data_source` | VARCHAR | Name of the source system. |
| `person_id` | VARCHAR | Patient identifier in the source system. |

### Demographic columns

Same as `eligibility`. All nullable.

| Column | Type |
|---|---|
| `first_name` | VARCHAR |
| `last_name` | VARCHAR |
| `birth_date` | DATE |
| `death_date` | DATE |
| `social_security_number` | VARCHAR |
| `sex` | VARCHAR |
| `race` | VARCHAR |
| `address` | VARCHAR |
| `city` | VARCHAR |
| `state` | VARCHAR |
| `zip_code` | VARCHAR |
| `phone` | VARCHAR |
| `county` | VARCHAR |

### Full DDL

```sql
CREATE TABLE your_database.your_schema.patient (

    -- Identity (required)
    data_source             VARCHAR     NOT NULL,
    person_id               VARCHAR     NOT NULL,

    -- Demographics (all nullable)
    first_name              VARCHAR,
    middle_name             VARCHAR,
    last_name               VARCHAR,
    name_suffix             VARCHAR,
    sex                     VARCHAR,
    race                    VARCHAR,
    ethnicity               VARCHAR,
    birth_date              DATE,
    death_date              DATE,
    death_flag              BOOLEAN,
    social_security_number  VARCHAR,
    address                 VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    zip_code                VARCHAR,
    county                  VARCHAR,
    phone                   VARCHAR,
    email                   VARCHAR
);
```

**Warehouse-specific type notes:** Same as `eligibility` - BigQuery uses `STRING` / `BOOL`; Microsoft Fabric uses `BIT` for `BOOLEAN`.


## Manual review tables

Two empty tables are required in a schema named `empi_manual_review`. EMPI Lite reads from these tables - reviewers write decisions into them. You can create them via `dbt run-operation create_manual_review_tables` or run the DDL below.

```sql
-- Create the schema
CREATE SCHEMA IF NOT EXISTS your_database.empi_manual_review;

-- Match review decisions
-- Reviewers insert rows here after reviewing empi_review_queue_matches
CREATE TABLE IF NOT EXISTS your_database.empi_manual_review.match_review_decisions (
    data_source_a       VARCHAR     NOT NULL,
    source_person_id_a  VARCHAR     NOT NULL,
    data_source_b       VARCHAR     NOT NULL,
    source_person_id_b  VARCHAR     NOT NULL,
    is_match            BOOLEAN,    -- TRUE = match, FALSE = no match, NULL = pending review
    reviewed            BOOLEAN,    -- FALSE = pending, TRUE = decision finalized
    reviewer_name       VARCHAR,
    review_date         DATE,
    notes               VARCHAR
);

-- Split review decisions
-- Reviewers insert rows here after reviewing empi_review_queue_splits
CREATE TABLE IF NOT EXISTS your_database.empi_manual_review.split_review_decisions (
    data_source         VARCHAR     NOT NULL,
    source_person_id    VARCHAR     NOT NULL,
    is_split            BOOLEAN,    -- TRUE = split, FALSE = not split, NULL = pending review
    reviewed            BOOLEAN,    -- FALSE = pending, TRUE = decision finalized
    reviewer_name       VARCHAR,
    review_date         DATE,
    notes               VARCHAR
);
```

These tables start empty. They only grow as reviewers make decisions. Both tables must exist (even if empty) before the first `dbt run`.

**Warehouse-specific type notes:** BigQuery uses `STRING` for all text columns. Microsoft Fabric uses `NVARCHAR` or `VARCHAR`. All other warehouses accept `VARCHAR` as written.


## Custom attributes table (optional)

If you enable `empi_custom_attributes_enabled: true` in `dbt_project.yml`, EMPI Lite will also read from a `custom_attributes` source table. This allows you to add organization-specific identifiers (email, NPI, subscriber ID, employee ID, etc.) to the matching engine without modifying any model code.

```sql
CREATE TABLE your_database.your_schema.custom_attributes (
    data_source         VARCHAR     NOT NULL,
    person_id           VARCHAR     NOT NULL,
    attribute           VARCHAR     NOT NULL,   -- e.g., 'email', 'npi', 'employee_id'
    value               VARCHAR     NOT NULL
);
```

One row per `(data_source, person_id, attribute)`. See [Configuration Guide](./configuration.md#custom-attributes) for how to register custom attributes with the scoring engine.


## Data volume notes

Naively comparing every patient record against every other record is O(n²). Blocking reduces this to a tractable number of candidate pairs by only comparing records that share at least one blocking group hash. The number of pairs that actually get scored is determined by blocking group overlap in your data, not by your total patient count.
