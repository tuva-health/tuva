# Manual Review Workflow

Not every match decision should be made by an algorithm. EMPI Lite ships two ready-to-use review queues and a decision workflow that automatically propagates reviewer decisions into the matching engine, crosswalk, golden record, and event trail.


## Overview

There are two types of review:

| Queue | Table | What it surfaces |
|---|---|---|
| **Match review** | `empi_review_queue_matches` | Candidate pairs in the review zone - above the dismissal threshold but below the auto-match threshold. A reviewer decides: same person or not? |
| **Split review** | `empi_review_queue_splits` | Source records whose demographic history contains conflicting values - a signal that the record may contain data for more than one patient. |

Both queues are output tables. You query them in your BI tool, review app, or spreadsheet, and write decisions into the corresponding worklist table in `empi_manual_review`. On the next `dbt run`, decisions flow through automatically.


## Match review

### Who reviews

Typically a data steward, health information management specialist, or analyst familiar with the source systems.

### Step 1 - Query the review queue

```sql
SELECT *
FROM empi.empi_review_queue_matches
WHERE review_priority = 'HIGH'
ORDER BY normalized_score DESC;
```

Each row represents one candidate pair. Key columns:

| Column | Description |
|---|---|
| `data_source_a`, `data_source_b` | Source systems for each record |
| `source_person_id_a`, `source_person_id_b` | Person IDs in their respective source systems |
| `normalized_score` | Match score (0-1) |
| `review_priority` | `HIGH` (just below match threshold), `MEDIUM`, or `LOW` |
| `person_profile_a`, `person_profile_b` | Demographic summary for each record (SSN masked) |
| `side_by_side_comparison` | Attribute-by-attribute comparison with match indicators |
| `matching_attributes` | Comma-separated list of attributes that agreed |
| `mismatching_attributes` | Comma-separated list of attributes that conflicted |

### Step 2 - Make a decision

For each pair reviewed, insert a row into the worklist table. Set `is_match = TRUE` for same person, `is_match = FALSE` for different people, and `reviewed = TRUE` when the decision is finalized:

```sql
-- Same person: link these records
INSERT INTO empi_manual_review.match_review_decisions
    (data_source_a, source_person_id_a, data_source_b, source_person_id_b, is_match, reviewed, reviewer_name, review_date, notes)
VALUES
    ('EHR_SYSTEM', 'PAT-10091', 'CLAIMS_PAYER', 'MBR-20047', TRUE, TRUE, 'jsmith', CURRENT_DATE, 'Same patient, name typo in claims');

-- Different people: do not link
INSERT INTO empi_manual_review.match_review_decisions
    (data_source_a, source_person_id_a, data_source_b, source_person_id_b, is_match, reviewed, reviewer_name, review_date, notes)
VALUES
    ('EHR_SYSTEM', 'PAT-10145', 'LAB_SYSTEM', 'LAB-000512', FALSE, TRUE, 'jsmith', CURRENT_DATE, 'Different birth dates, different patients');
```

- `is_match = TRUE` - same person; both records receive the same `empi_id` and `match_status = 'MANUAL_MATCHED'`
- `is_match = FALSE` - different people; the pair is permanently suppressed from the review queue
- `reviewed = TRUE` - decision is finalized (required for EMPI Lite to process the row)

### Step 3 - Re-run dbt

```bash
dbt run
```

On the next run:
- Rows with `is_match = TRUE` and `reviewed = TRUE` are incorporated as manual links
- Rows with `is_match = FALSE` and `reviewed = TRUE` permanently suppress the pair - it will not re-appear in the review queue
- A `MANUAL_MATCH` event (with reviewer name, if provided) is written to `empi_patient_events` for matched pairs

### Decision pair direction

`data_source_a`/`source_person_id_a` and `data_source_b`/`source_person_id_b` are interchangeable - the decision for `('EHR_SYSTEM', 'PAT-001', 'CLAIMS', 'MBR-001')` has the same effect as `('CLAIMS', 'MBR-001', 'EHR_SYSTEM', 'PAT-001')`.


## Split review

### What is a split record?

A "split" occurs when a single `(data_source, source_person_id)` has conflicting demographic values across its history - for example, the same EHR patient ID appears with two different birth dates at different points in time. This is a common data entry error where one source record contains data for two real patients.

### Who reviews

Same data steward or HIM team as match review. Split review is typically lower volume but higher severity.

### Step 1 - Query the split queue

```sql
SELECT *
FROM empi.empi_review_queue_splits
ORDER BY
    CASE review_priority
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        WHEN 'LOW'      THEN 4
    END;
```

| Priority | Triggered by |
|---|---|
| `CRITICAL` | Conflicting birth date or SSN - strong signal of two different patients |
| `HIGH` | Conflicting last name |
| `MEDIUM` | Conflicting first name, sex, or email |
| `LOW` | Conflicting address, phone, or other fields |

### Step 2 - Make a decision

```sql
-- Confirmed split: this source record contains data for 2+ patients
INSERT INTO empi_manual_review.split_review_decisions
    (data_source, source_person_id, is_split, reviewed, reviewer_name, review_date, notes)
VALUES
    ('EHR_SYSTEM', 'PAT-10188', TRUE, TRUE, 'jsmith', CURRENT_DATE,
     'DOB changed from 1972 to 1989 - two different patients shared this ID');

-- Not a split: the conflicting values are a known data quality issue, not a true split
INSERT INTO empi_manual_review.split_review_decisions
    (data_source, source_person_id, is_split, reviewed, reviewer_name, review_date, notes)
VALUES
    ('EHR_SYSTEM', 'PAT-10244', FALSE, TRUE, 'jsmith', CURRENT_DATE,
     'Name change due to marriage - confirmed same patient');
```

- `is_split = TRUE` - confirmed split; record is excluded from automatic matching
- `is_split = FALSE` - not a split; record is cleared from the split queue
- `reviewed = TRUE` - decision is finalized (required for EMPI Lite to process the row)

### Step 3 - Re-run dbt

On the next run:
- Records with `is_split = TRUE` are excluded from automatic matching entirely - they can only be linked through explicit decisions (`is_match = TRUE`) in `match_review_decisions`
- A `RECORD_SPLIT` event is written to `empi_patient_events`
- Records with `is_split = FALSE` are cleared from the split queue


## Split + match interaction

A record confirmed as split is automatically excluded from algorithmic matching. To link it to another record after splitting, use the match review decision workflow:

```sql
-- First, confirm the split
INSERT INTO empi_manual_review.split_review_decisions
    (data_source, source_person_id, is_split, reviewed, reviewer_name, review_date, notes)
VALUES ('EHR_SYSTEM', 'PAT-10188', TRUE, TRUE, 'jsmith', CURRENT_DATE, 'Two patients shared this ID');

-- Then, manually link the split record to its correct match
INSERT INTO empi_manual_review.match_review_decisions
    (data_source_a, source_person_id_a, data_source_b, source_person_id_b, is_match, reviewed, reviewer_name, review_date, notes)
VALUES ('EHR_SYSTEM', 'PAT-10188', 'CLAIMS_PAYER', 'MBR-55001', TRUE, TRUE, 'jsmith', CURRENT_DATE, 'Linked after split');
```


## Monitoring review progress

```sql
-- How many pairs are in the review queue?
SELECT review_priority, COUNT(*) as pairs
FROM empi.empi_review_queue_matches
GROUP BY review_priority;

-- How many decisions have been made?
SELECT is_match, COUNT(*) as decisions
FROM empi_manual_review.match_review_decisions
WHERE reviewed = TRUE
GROUP BY is_match;

-- Unreviewed HIGH priority pairs (in queue but no decision yet)
SELECT q.*
FROM empi.empi_review_queue_matches q
LEFT JOIN empi_manual_review.match_review_decisions d
    ON (q.data_source_a = d.data_source_a AND q.source_person_id_a = d.source_person_id_a
        AND q.data_source_b = d.data_source_b AND q.source_person_id_b = d.source_person_id_b)
    OR (q.data_source_a = d.data_source_b AND q.source_person_id_a = d.source_person_id_b
        AND q.data_source_b = d.data_source_a AND q.source_person_id_b = d.source_person_id_a)
WHERE d.data_source_a IS NULL
  AND q.review_priority = 'HIGH';
```


## Recommended review cadence

| Scenario | Suggested cadence |
|---|---|
| Initial deployment | Process the full review queue before going live |
| Ongoing steady state | Review `HIGH` priority pairs weekly; `MEDIUM`/`LOW` monthly |
| After a new source system is added | Review `HIGH` and `CRITICAL` splits immediately |
| After threshold changes | Re-review pairs that moved into or out of the review zone |


## Building a reviewer interface

The review queues are regular warehouse tables - you can build a reviewer interface using any tool that can read and write to your warehouse:

- **BI tools** (Tableau, Looker, Power BI): build a review dashboard that surfaces pairs side-by-side and allows decisions via an input form or linked spreadsheet
- **Retool / Budibase / AppSmith**: lightweight internal apps with write-back to the worklist tables
- **Spreadsheet export**: export the queue to a shared spreadsheet, collect decisions, bulk-insert back via SQL
- **Streamlit / Dash**: custom Python app for more complex review workflows

The only interface requirement is that decisions end up as rows in `empi_manual_review.match_review_decisions` or `empi_manual_review.split_review_decisions`.
