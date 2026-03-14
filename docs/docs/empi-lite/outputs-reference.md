# Output Table Reference

EMPI Lite produces eight final output tables, all written to the `empi` schema in your warehouse. This document describes each table, its columns, and example rows.


## empi_crosswalk

**The primary identity mapping table.** One row per `(data_source, source_person_id)` pair. Every source record in every source system maps to exactly one `empi_id`.

**How to use it:** Join this table to any other table using `data_source` and `source_person_id` to add `empi_id`. Then join on `empi_id` to any other EMPI output.

### Key columns

| Column | Type | Description |
|---|---|---|
| `empi_id` | VARCHAR | Globally unique stable identifier for the real person (deterministic hash, format `EMP-` + hex or numeric). Shared by all source records that have been linked to the same individual. |
| `data_source` | VARCHAR | Source system name (e.g., `EHR_SYSTEM`, `CLAIMS_PAYER`). |
| `source_person_id` | VARCHAR | Patient identifier as it appears in the source system. |
| `match_status` | VARCHAR | `SINGLETON` - not linked to any other record. `EMPI_MATCHED` - linked algorithmically. `MANUAL_MATCHED` - linked by a human reviewer. |
| `cluster_size` | INTEGER | How many source records share this `empi_id`. |
| `cluster_data_sources` | VARCHAR | Pipe-delimited list of all data sources represented in the cluster. |
| `cluster_enrollment_start` | DATE | Earliest enrollment start date across all records in the cluster. |
| `cluster_enrollment_end` | DATE | Latest enrollment end date across all records in the cluster. |
| `source_enrollment_start` | DATE | Enrollment start for this specific source record. |
| `source_enrollment_end` | DATE | Enrollment end for this specific source record. |

### Example rows

```
empi_id      data_source    source_person_id   match_status    cluster_size   cluster_data_sources
-----------  -------------  -----------------  --------------  -------------  ------------------------------------
EMP-0000042  EHR_SYSTEM     PAT-10042          EMPI_MATCHED    2              EHR_SYSTEM|CLAIMS_PAYER
EMP-0000042  CLAIMS_PAYER   MBR-88199A         EMPI_MATCHED    2              EHR_SYSTEM|CLAIMS_PAYER
EMP-0000043  EHR_SYSTEM     PAT-10043          SINGLETON       1              EHR_SYSTEM
EMP-0000044  EHR_SYSTEM     PAT-10044          EMPI_MATCHED    3              EHR_SYSTEM|CLAIMS_PAYER|LAB_SYSTEM
EMP-0000044  CLAIMS_PAYER   MBR-10031          EMPI_MATCHED    3              EHR_SYSTEM|CLAIMS_PAYER|LAB_SYSTEM
EMP-0000044  LAB_SYSTEM     LAB-000291         EMPI_MATCHED    3              EHR_SYSTEM|CLAIMS_PAYER|LAB_SYSTEM
```


## empi_golden_record

**One authoritative demographic row per EMPI ID.** Resolves the best available values from all source records in each cluster.

**Resolution logic:**
- Patient records are preferred over eligibility/enrollment records
- The most recent effective date wins within each record type
- Critical identifiers (SSN, phone, death date, address) are backfilled from any record in the cluster when the primary record is missing them

**How to use it:** Join on `empi_id` to get resolved demographics for any patient. Use the `has_conflicting_*` flags to qualify downstream analysis.

### Key columns

| Column | Type | Description |
|---|---|---|
| `empi_id` | VARCHAR | Patient identifier. |
| `first_name`, `last_name` | VARCHAR | Resolved name. |
| `birth_date` | DATE | Resolved date of birth. |
| `death_date` | DATE | Resolved date of death (backfilled from any cluster record). |
| `social_security_number` | VARCHAR | Resolved SSN. |
| `sex` | VARCHAR | Resolved sex/gender. |
| `address`, `city`, `state`, `zip_code` | VARCHAR | Resolved address. |
| `phone`, `email` | VARCHAR | Resolved contact info. |
| `ssn_source_count` | INTEGER | Number of distinct SSN values seen across the cluster. |
| `birth_date_source_count` | INTEGER | Number of distinct birth dates seen across the cluster. |
| `is_matched_record` | BOOLEAN | True if this patient has been linked to at least one other source record. |
| `has_conflicting_birth_dates` | BOOLEAN | True if two or more distinct birth dates exist across the cluster. |
| `has_conflicting_last_names` | BOOLEAN | True if two or more distinct last names exist across the cluster. |
| `primary_record_type` | VARCHAR | The source record type used as the primary source (`PATIENT` or `ELIGIBILITY`). |

### Example rows

```
empi_id      first_name  last_name   birth_date   sex     state   is_matched_record   has_conflicting_birth_dates   has_conflicting_last_names
-----------  ----------  ----------  -----------  ------  ------  -------------------  ---------------------------  --------------------------
EMP-0000042  Jonathan    Smith       1978-03-14   male    OR      true                 false                         false
EMP-0000043  Maria       Gonzalez    1991-07-02   female  CA      false                false                         false
EMP-0000044  Robert      Johnson     1965-11-28   male    CO      true                 false                         false
EMP-0000091  Jane        Doe         1983-05-19   female  TX      true                 true                          false
```

`EMP-0000091` has conflicting birth dates - the same person appears with different DOBs in different source systems. A downstream analyst should investigate before relying on the birth date.


## empi_patient_events

**A complete chronological audit trail.** One row per event per patient. Every match, every demographic change, every enrollment span, and every snapshot-detected change is recorded here.

**How to use it:** Filter on `empi_id` to reconstruct the complete history for any patient. Filter on `event_type` to find all matches, all demographic changes, etc.

### Event types

| Event type | Description |
|---|---|
| `DEMOGRAPHIC_LOADED` | A demographic record was ingested from a source system. |
| `DEMOGRAPHIC_CHANGE` | A demographic field changed between consecutive records for the same person in the same source. |
| `EMPI_MATCH` | Two source IDs were linked - algorithmically or by a reviewer. Includes a plain-English narrative. |
| `RECORD_SPLIT` | A source record was flagged as containing data for more than one real patient. |
| `ENROLLMENT_START` | A new enrollment span began in a source system. |
| `ENROLLMENT_END` | An enrollment span ended. |
| `EMPI_ID_CHANGED` | *(snapshots enabled)* A record's cluster membership changed between pipeline runs. |
| `MATCH_STATUS_CHANGED` | *(snapshots enabled)* A record transitioned from `SINGLETON` to `EMPI_MATCHED`. |
| `SOURCE_DATA_UPDATED` | *(snapshots enabled)* An upstream source record was corrected between runs. |

### Key columns

| Column | Type | Description |
|---|---|---|
| `empi_id` | VARCHAR | Patient identifier. |
| `event_date` | DATE | When the event occurred (or was detected). |
| `event_type` | VARCHAR | See table above. |
| `event_description` | VARCHAR | Human-readable description of the event. |
| `data_source` | VARCHAR | Source system involved. |
| `source_person_id` | VARCHAR | Source record involved. |

### Example rows

```
empi_id      event_date   event_type            event_description
-----------  -----------  --------------------  -----------------------------------------------------------------------
EMP-0000042  2019-01-14   DEMOGRAPHIC_LOADED    Record loaded from EHR_SYSTEM (PAT-10042): Jonathan Smith, DOB 1978-03-14
EMP-0000042  2019-01-14   ENROLLMENT_START      Enrollment began at CLAIMS_PAYER (MBR-88199A) on 2018-11-01
EMP-0000042  2019-01-15   EMPI_MATCH            Automatically linked: [EHR_SYSTEM:PAT-10042] and
                                                [CLAIMS_PAYER:MBR-88199A]. Last name matched exactly. Birth date
                                                matched exactly. First name was a close fuzzy match (Jon vs. Jonathan,
                                                78% similarity). Social security number matched exactly.
                                                Similarity score: 93.2% - above the 70.0% match threshold.
EMP-0000042  2023-06-01   DEMOGRAPHIC_CHANGE    Address changed on EHR_SYSTEM (PAT-10042):
                                                123 Oak St → 847 Pine Ave
```


## empi_demographics_tall

**Every known demographic value for every patient, across all sources, in a long/tall format.** Useful for understanding what values exist across a cluster and for building source-level comparison views.

### Key columns

| Column | Type | Description |
|---|---|---|
| `empi_id` | VARCHAR | Patient identifier. |
| `data_source` | VARCHAR | Which source system provided this value. |
| `source_person_id` | VARCHAR | Source record that provided this value. |
| `attribute` | VARCHAR | Demographic attribute name (e.g., `first_name`, `birth_date`, `ssn`). |
| `value` | VARCHAR | The attribute value from this source. |
| `attribute_tier` | VARCHAR | `STANDARD` (eligibility/patient columns) or `CUSTOM` (from custom attributes table). |


## empi_review_queue_matches

**Candidate pairs in the review zone, ready for human adjudication.** Score is above the dismissal threshold but below the auto-match threshold. A reviewer sets `is_match = TRUE` (same person) or `is_match = FALSE` (not a match), then sets `reviewed = TRUE` to finalize the decision.

See [Manual Review Workflow](./manual-review.md) for the full decision workflow.

### Key columns

| Column | Type | Description |
|---|---|---|
| `data_source_a` / `data_source_b` | VARCHAR | Data source for each record in the pair. |
| `source_person_id_a` / `source_person_id_b` | VARCHAR | Source person ID for each record. |
| `person_profile_a` / `person_profile_b` | VARCHAR | Demographic summary for each record (SSN masked). |
| `side_by_side_comparison` | VARCHAR | Attribute-by-attribute comparison with match indicators. |
| `normalized_score` | FLOAT | Normalized score between 0 and 1. |
| `review_priority` | VARCHAR | `HIGH` (close to match threshold), `MEDIUM`, or `LOW`. |
| `matching_attributes` | VARCHAR | Comma-separated list of attributes that agreed. |
| `mismatching_attributes` | VARCHAR | Comma-separated list of attributes that conflicted. |

### Example rows

```
data_source_a   source_person_id_a   data_source_b   source_person_id_b   normalized_score   review_priority
--------------  -------------------  --------------  -------------------  -----------------  ---------------
EHR_SYSTEM      PAT-10091            CLAIMS_PAYER    MBR-20047             0.61               HIGH
EHR_SYSTEM      PAT-10145            LAB_SYSTEM     LAB-000512           0.57               MEDIUM
EHR_SYSTEM      PAT-10201            CLAIMS_PAYER   MBR-30110             0.52               LOW
```


## empi_review_queue_splits

**Source records whose own demographic history contains conflicting values.** A signal that a source record may contain data for more than one real patient (a common EHR data entry error).

See [Manual Review Workflow](./manual-review.md) for the split decision workflow.

### Key columns

| Column | Type | Description |
|---|---|---|
| `data_source` | VARCHAR | Source system containing the suspicious record. |
| `source_person_id` | VARCHAR | The record with conflicting demographics. |
| `record_count` | INTEGER | Number of distinct demographic records found for this source person. |
| `review_priority` | VARCHAR | `CRITICAL` (conflicting DOB or SSN), `HIGH` (conflicting last name), `MEDIUM`, or `LOW`. |
| `inconsistent_attribute_count` | INTEGER | Number of attributes with conflicting values. |
| `inconsistent_attributes` | VARCHAR | Comma-separated list of attributes that conflict. |
| `inconsistency_summary` | VARCHAR | Newline-delimited summary of each conflicting attribute and its observed values, ordered by severity. |

### Example rows

```
data_source   source_person_id   record_count   review_priority   inconsistent_attribute_count   inconsistent_attributes   inconsistency_summary
------------  -----------------  -------------  ---------------   ----------------------------   -----------------------  -------------------------------------------
EHR_SYSTEM    PAT-10188          2              CRITICAL          1                              birth_date                birth_date: 1972-04-01, 1989-11-15
EHR_SYSTEM    PAT-10244          2              HIGH              1                              last_name                 last_name: Martinez, Hoffman
```


## empi_demographic_anomalies

**Statistical surveillance over the patient population.** Surfaces values that appear across a disproportionate share of records and identifier fields shared across multiple distinct patients.

### Anomaly types

| Anomaly type | Description |
|---|---|
| `FREQUENCY_OUTLIER` | A demographic value appears across an unusually high share of records relative to the distribution for that attribute - likely a test value, placeholder, or recycled value. |
| `UNIQUE_FIELD_VIOLATION` | An identifier field (SSN, email) is shared across two or more distinct EMPI IDs. This is a high-risk data quality issue. |

### Example rows

```
anomaly_type             attribute   value          affected_records   pct_of_population   description
-----------------------  ----------  -------------  -----------------  ------------------  ------------------------------------------
FREQUENCY_OUTLIER        birth_date  1900-01-01     847                2.3%                Placeholder/test birth date shared by 847 records
FREQUENCY_OUTLIER        first_name  TEST           312                0.8%                Likely test/placeholder first name
FREQUENCY_OUTLIER        phone       555-000-0000   214                0.6%                Placeholder phone number
UNIQUE_FIELD_VIOLATION   ssn         123-45-6789    14                 -                   SSN shared across 14 distinct EMPI IDs
UNIQUE_FIELD_VIOLATION   email       test@test.com  28                 -                   Email shared across 28 distinct EMPI IDs
```


## empi_score_distribution

**Score distribution across all candidate pairs.** Shows how many pairs fall in each 0.01 score bucket. Used for threshold selection and understanding the shape of your data.


## empi_attribute_coverage

**Attribute fill rates across the patient population.** Shows what percentage of records have each demographic attribute populated. Low fill rates identify attributes that will contribute little to matching quality for your specific data.


## Evaluation models (optional)

If you supply a `true_patient_mappings` seed (ground-truth known-correct links), two additional evaluation models are available:

| Table | Description |
|---|---|
| `empi_threshold_analysis` | Precision, recall, and F1 score at every 0.01 threshold increment from 0.50 to 0.95. |
| `empi_threshold_examples` | Specific false positives and false negatives at the current threshold - for understanding why the model makes specific decisions. |


## Tuva integration tables

EMPI Lite also writes two tables to the `public` schema for downstream Tuva compatibility:

| Table (alias) | Description |
|---|---|
| `tuva_patient` (alias: `patient`) | Tuva-compatible patient table where `person_id = empi_id`. |
| `tuva_eligibility` (alias: `eligibility`) | Tuva-compatible eligibility table where `person_id = empi_id`. |

Every downstream Tuva mart automatically inherits the resolved patient identity with no additional transformation.
