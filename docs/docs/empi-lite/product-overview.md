# EMPI Lite - Product Overview

**Enterprise Master Patient Index - delivered as a dbt package**


## The Problem

Patient data arrives from multiple systems - an EHR, a claims payer, a lab, a state registry - each with its own patient identifier and its own version of a person's name, address, and date of birth. There is no single authoritative answer to *"who is this patient, and where do they appear across our data?"*

The result is overcounting, fragmented care histories, duplicate outreach, inaccurate attribution, and analytics that cannot be trusted at the patient level.

EMPI Lite solves this. It connects patient records across source systems, assigns a single persistent identity to each real person, and delivers that identity back to every downstream model and report.


## What You Get

EMPI Lite is a **complete, production-ready master patient index** built entirely in dbt SQL. It ships as full source code that runs in your warehouse - no black-box API, no vendor dependency, no data leaving your environment.

### 1. Probabilistic Identity Matching

EMPI Lite compares patient records across every source system and computes a weighted similarity score for each candidate pair using up to 16 demographic attributes.

**How it works:**
- **Blocking** reduces the comparison space from O(n²) to manageable scale. Records are grouped by composite attribute hashes (e.g., last name + birth date, SSN) - only records sharing at least one blocking group become candidate pairs.
- **Attribute scoring** applies per-attribute weights, exact match scores, fuzzy match (edit-distance similarity), and mismatch penalties. Every attribute is independently configurable.
- **Normalized scoring** produces a 0-1 similarity score that accounts for which attributes were present on each record - a pair with only two attributes populated is not penalized against a pair with ten.
- **ZIP code proximity** - ZIP codes within 50 miles receive partial match credit, not a binary match/miss.

Default matching attributes and their weights:

| Attribute | Weight | Notes | Source |
|---|---|---|---|
| Social Security Number | 20 | Exact match; strong mismatch penalty | eligibility / patient |
| Birth Date | 10 | Fuzzy (edit-distance) | eligibility / patient |
| Last Name | 7 | Fuzzy (Levenshtein) | eligibility / patient |
| Email | 6 | Exact match | custom attributes table |
| Death Date | 5 | Fuzzy | eligibility / patient |
| Phone | 4 | Exact (digits normalized) | eligibility / patient |
| Address | 4 | Fuzzy | eligibility / patient |
| First Name | 3 | Fuzzy | eligibility / patient |
| ZIP Code | 3 | Proximity-scored via Census distance table | eligibility / patient |
| State | 2 | Exact | eligibility / patient |
| Sex | 2 | Exact | eligibility / patient |
| City | 2 | Fuzzy | eligibility / patient |
| County | 1.5 | Exact | patient only |
| Race | 1 | Exact | eligibility / patient |

All weights, scores, penalties, and fuzzy thresholds are controlled by a seed CSV - no code changes required to re-tune. Attributes marked "custom attributes table" require enabling the custom attributes feature and supplying values in a separate key-value source table.


### 2. Persistent EMPI ID Assignment

Every source patient record receives an `empi_id` - a stable identifier that represents the real person behind the record, regardless of which system it came from.

Matched records are grouped into clusters using **connected-components analysis** (iterative min-label propagation). If patient A matches patient B, and patient B matches patient C, all three receive the same `empi_id` - even if A and C were never directly compared.

**`empi_crosswalk`** - the primary output table - maps every `(data_source, source_person_id)` pair to its `empi_id`, along with:
- `match_status` - `SINGLETON`, `EMPI_MATCHED`, or `MANUAL_MATCHED`
- Cluster size and data source membership
- Enrollment date spans per source and across the full cluster


### 3. Golden Record

**`empi_golden_record`** resolves one authoritative demographic row per EMPI ID from all the records in each cluster.

Resolution logic:
- Clinical patient records are preferred over eligibility/enrollment records
- The most recent effective date wins within each record type
- Critical identifiers (SSN, phone, death date, address) are backfilled from any record in the cluster when the primary record is missing them
- Field coverage counts (`ssn_source_count`, `birth_date_source_count`, etc.) provide downstream confidence signals
- Built-in data quality flags surface potential problems: `has_conflicting_birth_dates`, `has_conflicting_last_names`, `is_matched_record`


### 4. Full Audit Trail

**`empi_patient_events`** is a complete chronological record of everything that has ever happened to a patient identity. Every row is a discrete, human-readable event.

| Event Type | When it fires |
|---|---|
| `DEMOGRAPHIC_LOADED` | Each demographic record ingested from a source system |
| `DEMOGRAPHIC_CHANGE` | A field (name, address, phone, sex) changed between consecutive records for the same person |
| `EMPI_MATCH` | Two source IDs were linked - algorithmically or by a reviewer |
| `RECORD_SPLIT` | A record was flagged as containing data for more than one patient |
| `ENROLLMENT_START` | A new enrollment span began |
| `ENROLLMENT_END` | An enrollment span ended |
| `EMPI_ID_CHANGED` | *(when snapshots enabled)* A person's cluster membership changed between runs |
| `MATCH_STATUS_CHANGED` | *(when snapshots enabled)* A record moved from SINGLETON to matched |
| `SOURCE_DATA_UPDATED` | *(when snapshots enabled)* An upstream source record was corrected |

Every `EMPI_MATCH` event includes a **plain-English narrative** explaining exactly why the match was made:

> *"Automatically linked: [EHR_A:PAT-001] and [CLAIMS_B:MBR-9921]. Last name matched exactly. Birth date matched exactly. First name was a close fuzzy match (Jon vs. John, 75% similarity). Address matched exactly. Social security number matched exactly. Similarity score: 91.4% - above the 70.0% match threshold."*


### 5. Manual Review Workflows

Not every match decision can or should be made by an algorithm. EMPI Lite ships two ready-to-use worklists that feed human reviewers and automatically propagate their decisions into the matching engine.

#### Match Review Queue

**`empi_review_queue_matches`** surfaces candidate pairs whose score falls in the configurable review zone - not high enough to auto-match, not low enough to dismiss. Each row includes:

- Side-by-side attribute comparison with exact, close-match, and no-match labels
- Full person profiles for both records with all available attributes
- Lists of matching and mismatching attributes
- `review_priority` (HIGH / MEDIUM / LOW) based on how close the score is to the match threshold
- SSN masked for safe display in BI tools

Once a reviewer inserts a decision into the `match_review_decisions` worklist table and re-runs dbt, that decision is immediately reflected in the EMPI crosswalk, golden record, and event trail.

#### Split Review Queue

**`empi_review_queue_splits`** identifies source person records whose own demographic history contains conflicting values - a signal that the source record may contain data for more than one patient (a common EHR data entry error).

Priority is driven by severity of the conflict:

| Priority | Triggered by |
|---|---|
| CRITICAL | Conflicting birth date or SSN |
| HIGH | Conflicting last name |
| MEDIUM | Conflicting first name, sex, or email |
| LOW | Conflicting address, phone, or other fields |

A split record, once confirmed, is excluded from automatic matching entirely and can only be linked to other records through explicit manual decisions.


### 6. Data Quality Monitoring

**`empi_demographic_anomalies`** runs statistical surveillance over the entire patient population to detect two classes of data quality problem:

**Frequency outliers** - demographic values that appear across a disproportionate share of records relative to the distribution for that attribute. Common examples: test birth dates (`1900-01-01`), placeholder names (`UNKNOWN`, `TEST`), recycled phone numbers. Detected using a log z-score model so the heavily right-skewed frequency distribution is normalized before testing. Fully tunable thresholds.

**Unique field violations** - identifier fields (SSN, email) that are shared across two or more distinct EMPI IDs. These are the most clinically dangerous data quality issues - a shared SSN means two different patients may receive each other's care history, billing, or clinical decisions.

In addition, EMPI Lite ships an **invalid values seed** containing ~23,000 known-bad demographic values (test SSNs, placeholder names and addresses, fake birth dates) that are suppressed before matching begins.


### 7. Snapshot-Based Change Detection

EMPI Lite snapshots capture the state of the crosswalk and source tables at each run, enabling questions that a stateless pipeline cannot answer:

- *When was this person first assigned to their current EMPI ID?*
- *Did this cluster reorganize after last week's data refresh?*
- *Which source records were corrected upstream between runs?*
- *How many records transitioned from SINGLETON to matched this month?*

Five snapshot targets are included: crosswalk history, source eligibility, source patient, blocking rules, and attribute score configuration.


### 8. Precision / Recall Evaluation Framework

For organizations with ground-truth data, EMPI Lite includes a built-in evaluation framework that measures how well the matching engine is performing.

**`empi_threshold_analysis`** computes precision, recall, and F1 score at every 0.01 threshold increment from 0.50 to 0.95, enabling data-driven threshold selection rather than guesswork.

**`empi_threshold_examples`** surfaces specific false positives and false negatives at the current threshold - the fastest way to understand why the model is making the decisions it is.


### 9. Custom Attribute Matching

The matching engine is fully extensible. Any organization-specific identifier - NPI, employee ID, health plan subscriber ID, custom biometric - can be registered as a custom attribute and will automatically participate in blocking, scoring, the review queues, and the audit trail.

No core model changes are required - custom attributes are configured entirely through a source table and seed CSV rows.


### 10. Tuva Integration

EMPI Lite writes Tuva-compatible `patient` and `eligibility` tables where `person_id = empi_id`. Every downstream Tuva mart - claims, clinical, quality measures - automatically inherits the resolved patient identity with no additional transformation required.


## Outputs at a Glance

| Table | What it answers |
|---|---|
| `empi_crosswalk` | What is the EMPI ID for this source person? |
| `empi_golden_record` | What are the best demographics for this patient? |
| `empi_patient_events` | What has happened to this patient's identity over time? |
| `empi_demographics_tall` | What is every known demographic value for this patient across all sources? |
| `empi_review_queue_matches` | Which pairs need a human match/no-match decision? |
| `empi_review_queue_splits` | Which records may contain data for more than one patient? |
| `empi_demographic_anomalies` | Where is demographic data statistically suspicious or dangerously shared? |
| `empi_threshold_analysis` | What is the precision/recall of the matching engine at each threshold? |


## What Is and Is Not Included

### Included

- Full dbt source code for all models, macros, seeds, and snapshots
- 4 default blocking groups (name+DOB, SSN, name only, last+DOB) - extensible with any custom attribute
- 16 pre-configured matching attributes with tuned weights
- ~23,000 known-bad invalid value suppressions
- US Census ZIP code proximity table (365,000+ ZIP pair distances)
- US state code normalization seed
- Manual review worklist schema and workflow
- Plain-English match narrative generation
- Unit tests for staging, intermediate, and final models
- Full documentation

### Not Included

- A hosted UI or application layer (the review queues are tables - you bring your own BI tool or app)
- A REST API or real-time matching endpoint (EMPI Lite is a batch dbt pipeline)
- A pre-built patient portal or clinician-facing workflow application


## Delivery Model

EMPI Lite is delivered as a **clone-and-customize** dbt project. You receive the complete source code and own it outright. There is no runtime dependency on a vendor system, no data sent to a third party, and no license keys.

Two integration patterns are supported:

- **Import into an existing dbt project** via a local package reference (`local: ../empi_lite/empi_lite`) - EMPI Lite runs alongside your existing models in a single `dbt run`
- **Run as a standalone dbt project** with its own dedicated profile and run cadence

Both patterns support all six major data warehouses: Snowflake, BigQuery, Redshift, Microsoft Fabric, DuckDB, and Databricks.


## Typical Implementation Timeline

| Phase | Activities | Typical Duration |
|---|---|---|
| **Setup** | Clone repo, configure profile, point at source tables, load seeds, first `dbt run` | 1-2 days |
| **Validation** | Review match quality, spot-check golden records, tune thresholds | 3-5 days |
| **Review workflow** | Create worklist tables, establish review cadence, process initial queue | 1-2 weeks |
| **Steady state** | Scheduled `dbt run` + `dbt snapshot` on each source data refresh | Ongoing |


## Requirements

- dbt Core ≥ 1.7
- One of: Snowflake, BigQuery, Redshift, Microsoft Fabric, DuckDB, or Databricks
- Source tables: an `eligibility` table and/or a `patient` table with standard demographic columns
- Two empty worklist tables in a `empi_manual_review` schema (DDL provided in `README.md`)
