---
id: medicare-cclf-connector
title: "Medicare CCLF Connector"
hide_title: true
---

# Medicare CCLF Connector

The Medicare CCLF Connector is a dbt project that transforms raw CMS Comprehensive Claims and Line Feed (CCLF) files into the Tuva Project [Input Layer](../input-layer). It is the primary connector for running Tuva analytics on MSSP ACO data.

## What Is CCLF Data?

CMS provides monthly CCLF files to MSSP ACOs containing claims for their assigned beneficiaries. The files cover:

- **Part A** — Institutional claims (inpatient, outpatient, SNF, home health, hospice), including header records, revenue center detail, diagnosis codes, and procedure codes
- **Part B** — Professional and DME claims (physician/supplier and durable medical equipment)
- **Part D** — Pharmacy claims
- **Beneficiary demographics** — Age, sex, race, dual eligibility status, and other beneficiary attributes
- **MBI cross-reference** — Historical Medicare Beneficiary Identifiers (used to link beneficiaries across file vintages)

CCLF files use a naming convention that encodes the file date (e.g., `P.A1234.ACO.ZC1Y18.D240101.T000000`), which the connector parses and attaches as `file_date` to enable deduplication across monthly and run-out files.

## Model Layers

The connector uses a three-layer dbt architecture:

### Staging (views)

Type-casting only — one model per CCLF source table:

| Model | Source |
|---|---|
| `stg_parta_claims_header` | Part A header |
| `stg_parta_claims_revenue_center_detail` | Part A revenue center |
| `stg_parta_diagnosis_code` | Part A diagnoses |
| `stg_parta_procedure_code` | Part A procedures |
| `stg_partb_physicians` | Part B physician claims |
| `stg_partb_dme` | Part B DME claims |
| `stg_partd_claims` | Part D pharmacy claims |
| `stg_beneficiary_demographics` | Beneficiary demographics |
| `stg_beneficiary_xref` | MBI cross-reference |
| `stg_enrollment` | Enrollment input (from ALR connector or custom) |

### Intermediate (tables)

~18 models that handle deduplication, ADR (Add/Drop/Revision) logic, and code pivoting:

| Model | Purpose |
|---|---|
| `int_beneficiary_demographics_deduped` | Deduplicated demographics using latest `file_date` |
| `int_beneficiary_xref_deduped` | Deduplicated MBI cross-reference |
| `int_enrollment` | Processed enrollment dates |
| `int_institutional_claim_deduped` | Part A claims after initial dedup |
| `int_institutional_claim_adr` | Part A claims after Add/Drop/Revision resolution |
| `int_physician_claim_deduped` | Part B physician claims deduped |
| `int_physician_claim_adr` | Part B physician claims after ADR |
| `int_dme_claim_deduped` | Part B DME claims deduped |
| `int_dme_claim_adr` | Part B DME claims after ADR |
| `int_pharmacy_claim_deduped` | Part D pharmacy claims deduped |
| `int_*_pivot` | Diagnosis and procedure codes pivoted to wide format |

### Final (tables)

Three Tuva Input Layer tables:

| Table | Description |
|---|---|
| `eligibility` | One row per member per month (or per enrollment span) |
| `medical_claim` | Standardized medical claims from Parts A and B |
| `pharmacy_claim` | Standardized pharmacy claims from Part D |

## ADR (Add/Drop/Revision) Handling

CMS CCLF files use an Add/Drop/Revision system to manage claim corrections across monthly files:

- **A (Add)** — New claim record
- **D (Drop)** — Cancel a previously submitted record
- **R (Revision)** — Replace a previously submitted record

The connector applies ADR logic to produce a final, corrected set of claims by matching Drop and Revision records against their corresponding Add records using the claim ID. This is critical for accurate cost and utilization analytics.

## Enrollment Options

The connector supports two ways to provide member enrollment data:

### Option 1: Member months from the ALR Connector (recommended for MSSP)

Run the [CMS ALR Connector](cms-alr-connector) first. It produces an `enrollment` table with one row per beneficiary per month, derived from the AALR assignment reports. Configure the CCLF connector to use it:

```yaml
vars:
  enrollment_database: "your_database"
  enrollment_schema: "your_alr_output_schema"
  enrollment_table: "enrollment"
```

### Option 2: Enrollment spans from a custom source

If you have enrollment spans (start/end dates) from another source, you can provide them directly in the `stg_enrollment` source. The connector will expand spans into member months automatically.

## Configuration

Set these variables in `dbt_project.yml` or via `--vars`:

```yaml
vars:
  # CCLF source data
  input_database: "your_database"
  input_schema: "your_mssp_schema"

  # Enrollment source (ALR connector output)
  enrollment_database: "your_database"
  enrollment_schema: "your_alr_output_schema"
```

## How to Run

```bash
cd medicare_cclf_connector

# Install dbt dependencies
dbt deps

# Build all models and run tests
dbt build

# Run tests only
dbt test

# Run a specific model
dbt run --select medical_claim
```

## Output

The three output tables conform to the Tuva Input Layer schema. Once populated, you can run the full Tuva Project on top of them to generate:

- Core Data Model (encounters, conditions, procedures, labs)
- Financial PMPM
- Chronic Conditions
- Quality Measures
- Readmissions
- CMS-HCCs
- And all other Tuva data marts

See the [Tuva Input Layer](../input-layer) documentation for the full column specifications.

## Supported Databases

| Database | Supported |
|---|---|
| BigQuery | Yes |
| Redshift | Yes |
| Snowflake | Yes |

## Project Structure

```
medicare_cclf_connector/
├── dbt_project.yml
├── models/
│   ├── staging/           # One view per CCLF source table
│   ├── intermediate/      # Dedup, ADR, and pivot models
│   └── final/             # eligibility, medical_claim, pharmacy_claim
├── macros/                # Cross-database adapter dispatch macros
├── integration_tests/     # End-to-end test suite
└── docs/                  # Source documentation
```
