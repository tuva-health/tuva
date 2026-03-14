# Configuration Guide

EMPI Lite is configured through three mechanisms: **dbt vars** (thresholds and feature flags), **blocking rules** (which attribute combinations create candidate pairs), and **attribute scores** (per-attribute weights and scoring behavior). All three are tunable without modifying any model code.


## Matching thresholds

Set in `dbt_project.yml` under `vars > empi_lite`.

| Variable | Default | Description |
|---|---|---|
| `match_threshold` | `0.70` | Pairs scoring at or above this are automatically linked. Raise to be more conservative (fewer matches, fewer false positives). Lower to be more aggressive (more matches, fewer missed links). |
| `review_threshold_low` | `0.50` | Pairs scoring at or above this (and below `match_threshold`) are routed to the match review queue. Pairs below this are dismissed without review. |
| `review_threshold_high` | `0.69` | Controls the `HIGH` / `MEDIUM` / `LOW` priority label on review queue rows. Pairs between `review_threshold_high` and `match_threshold` receive `HIGH` priority. Does not affect which pairs enter the queue - that is controlled by `review_threshold_low` and `match_threshold` exclusively. |

```yaml
# dbt_project.yml
vars:
  empi_lite:
    match_threshold: 0.70
    review_threshold_low: 0.50
    review_threshold_high: 0.69
```

### How the three thresholds interact

Scores are continuous. A pair with score `0.697` is below `match_threshold` (0.70) and above `review_threshold_high` (0.69), so it lands in the review queue with `HIGH` priority - it is **not** auto-matched. The boundaries work as follows:

```
score < 0.50              → dismissed, no action
0.50 ≤ score < 0.70       → routed to review queue
    0.50 ≤ score ≤ 0.69   →   MEDIUM or LOW priority
    0.69 < score < 0.70   →   HIGH priority
score ≥ 0.70              → auto-matched
```

`review_threshold_high` is purely a priority label - it does not create a gap in coverage. Every score between `review_threshold_low` and `match_threshold` lands in the queue.

### Choosing your thresholds

**Start with the defaults.** Run the project, examine `empi_patient_events` match narratives for a sample of `EMPI_MATCH` events, and spot-check 20-30 matches manually.

**Signs the match threshold is too low (too many false positives):**
- Matches where only one attribute agreed (e.g., same last name, nothing else)
- Patients being merged who are clearly different people
- Large clusters with 5+ records where some records don't belong

**Signs the match threshold is too high (too many false negatives):**
- Obvious same-person records in different systems not being linked
- High review queue volume with most pairs being obvious matches
- Low `EMPI_MATCHED` percentage, high `SINGLETON` percentage


## Blocking rules

Naively comparing every patient record against every other record is O(n²) - infeasible at any meaningful scale. Blocking rules solve this by first grouping records into buckets, so only records that land in the same bucket are compared.

**Configured in:** `seeds/empi_blocking_rules.csv`

After editing this file, re-run:
```bash
dbt seed --select empi_blocking_rules && dbt run
```

### AND within a group, OR across groups

This is the most important concept to understand when configuring blocking.

**Within a group - AND logic.** All attributes in a group must match for two records to be placed in the same bucket. Group 1 requires FIRST_NAME **and** LAST_NAME **and** BIRTH_DATE to all agree (as a composite hash) before two records become candidates. A single attribute disagreement excludes the pair from that group's bucket.

**Across groups - OR logic.** A pair becomes a candidate if they share at least one group's bucket. Two records that don't share a Group 1 hash (name + DOB) but do share a Group 2 hash (SSN alone) will still be compared. A pair only has to satisfy one group to enter scoring.

```
Group 1: FIRST_NAME AND LAST_NAME AND BIRTH_DATE  ──┐
Group 2: SOCIAL_SECURITY_NUMBER                     ├── any one match → candidate pair → scored
Group 3: FIRST_NAME AND LAST_NAME                   │
Group 4: LAST_NAME AND BIRTH_DATE                  ──┘
```

This design lets you tune the blocking strategy precisely: tighter groups (more AND conditions) reduce the candidate pair count; additional groups (more OR options) improve recall at the cost of more comparisons.

### Default blocking groups

| group_id | Attributes (all must match - AND) | Purpose |
|---|---|---|
| 1 | `FIRST_NAME, LAST_NAME, BIRTH_DATE` | Catches most same-person records with consistent demographics |
| 2 | `SOCIAL_SECURITY_NUMBER` | Catches records where names differ but SSN agrees |
| 3 | `FIRST_NAME, LAST_NAME` | Catches records where DOB is missing or entered differently |
| 4 | `LAST_NAME, BIRTH_DATE` | Catches records where first name varies (nicknames, initials) |

### Seed structure

```csv
group_id,attribute,enabled
1,FIRST_NAME,true
1,LAST_NAME,true
1,BIRTH_DATE,true
2,SOCIAL_SECURITY_NUMBER,true
3,FIRST_NAME,true
3,LAST_NAME,true
4,LAST_NAME,true
4,BIRTH_DATE,true
```

Each row is one attribute within a blocking group. The composite hash for a group is built from all attributes in that group where `enabled = true`.

### Adding or removing blocking groups

To add a new group (e.g., phone number as a standalone blocking key):

```csv
5,PHONE,true
```

To disable a group without deleting it, set `enabled` to `false`:

```csv
3,FIRST_NAME,false
3,LAST_NAME,false
```

**Note:** Blocking rules are snapshotted - changes are tracked in `empi_blocking_rules_snapshot`.

### Tuning guidance

- **Too many missed matches (false negatives)?** Add more blocking groups so pairs with fewer shared attributes can still become candidates. More groups = more candidate pairs = more compute.
- **Pipeline too slow?** Remove groups or tighten existing ones (add more AND conditions). Fewer pairs to score = faster pipeline, but some matches may be missed.
- Custom attributes can participate in any blocking group - see [Custom Attributes](#custom-attributes) below.


## Attribute scores

Attribute scores control how each demographic attribute contributes to the similarity score.

**Configured in:** `seeds/empi_attribute_scores.csv`

After editing this file, re-run:
```bash
dbt seed --select empi_attribute_scores && dbt run
```

### Attributes and their scoring behavior

The scoring engine ships with pre-tuned weights and scoring behavior for 14 demographic attributes. The full configuration lives in `seeds/empi_attribute_scores.csv`, which is included in your repo.

The attributes, in rough order of discriminating power, are:

| Attribute | Matching method |
|---|---|
| Social Security Number | Exact match; strong mismatch penalty |
| Birth Date | Fuzzy (edit-distance) |
| Last Name | Fuzzy (Levenshtein similarity) |
| Email | Exact match |
| Death Date | Fuzzy |
| Phone | Exact (digits normalized) |
| Address | Fuzzy |
| First Name | Fuzzy |
| ZIP Code | Geographic proximity (within 50 miles = partial credit) |
| State | Exact |
| Sex | Exact |
| City | Fuzzy |
| County | Exact |
| Race | Exact |

### Seed structure

```csv
attribute,weight,use_fuzzy_match,fuzzy_threshold,exact_match_score,mismatch_penalty
SOCIAL_SECURITY_NUMBER,...
BIRTH_DATE,...
LAST_NAME,...
...
```

### Understanding each column

**`weight`** - The maximum positive contribution this attribute can make to the total score denominator. Higher weight = this attribute matters more.

**`use_fuzzy_match`** - If `true`, a fuzzy (edit-distance / Levenshtein) similarity score is computed. Pairs with similarity above `fuzzy_threshold` receive partial credit proportional to similarity. If `false`, only exact matches score.

**`fuzzy_threshold`** - Minimum similarity ratio (0-1) to receive any fuzzy match credit. Pairs below this threshold are treated as mismatches.

**`exact_match_score`** - Points awarded for an exact match. Typically equal to `weight`.

**`mismatch_penalty`** - Points subtracted when two non-missing, non-matching values exist. A penalty of 0 means a mismatch is simply neutral (no points gained, no points lost). A negative penalty means mismatches actively reduce the score.

**ZIP code special behavior:** ZIP codes are scored using geographic proximity rather than exact or fuzzy match. Two ZIP codes within 50 miles receive partial credit; beyond 50 miles they are treated as a mismatch. The `mismatch_penalty` column still applies for ZIPs that are far apart.

### How normalized scoring works

The final similarity score is normalized to account for which attributes were present on each record:

```
score = sum(attribute_scores) / sum(weights_for_present_attributes)
```

A pair where both records are missing 8 of 14 attributes is not penalized against a pair where all 14 are present. This is critical for real-world data where fill rates vary significantly across source systems.

### Tuning guidance

**To increase the importance of an attribute:** Raise its `weight` and `exact_match_score`.

**To make mismatches hurt more:** Lower (more negative) the `mismatch_penalty`.

**To allow fuzzier name matches:** Lower the `fuzzy_threshold` for `FIRST_NAME` or `LAST_NAME` (e.g., 0.60 instead of 0.70).

**To make an attribute mandatory for matching:** Set a very high mismatch penalty (e.g., `-100`) on an attribute like SSN. A SSN mismatch will then drive the score below any reasonable threshold, preventing the match.

**Attribute scores are snaphotted** - changes are tracked in `empi_attribute_scores_snapshot`.


## Feature flags

| Variable | Default | Description |
|---|---|---|
| `empi_snapshot_enabled` | `false` | Set to `true` after the first `dbt snapshot` run. Enables change-detection events in `empi_patient_events`. |
| `empi_custom_attributes_enabled` | `false` | Set to `true` if you have a `custom_attributes` source table. |


## Data quality / anomaly detection tuning

| Variable | Default | Description |
|---|---|---|
| `anomaly_z_threshold` | `3.0` | Log z-score threshold for frequency outlier detection. Higher = fewer, more extreme flags. Recommended range: 2.5-4.0. |
| `anomaly_min_count` | `10` | Minimum number of patients sharing a value before it can be flagged as an outlier. Prevents small-N noise. |
| `anomaly_min_pct_name` | `0.02` | Minimum population share (first/last name) to flag - 2%. |
| `anomaly_min_pct_dob` | `0.01` | Minimum population share (birth date) to flag - 1%. |
| `anomaly_min_pct_contact` | `0.005` | Minimum population share (phone, address) to flag - 0.5%. |
| `anomaly_min_pct_zip` | `0.10` | Minimum population share (ZIP, city, payer) to flag - 10%. Higher because these are legitimately concentrated. |


## Custom attributes

To add organization-specific identifiers to the matching engine:

**1. Enable the feature:**
```yaml
vars:
  empi_lite:
    empi_custom_attributes_enabled: true
```

**2. Populate the `custom_attributes` source table** with your data (see [Data Requirements](./data-requirements.md#custom-attributes-table-optional)).

**3. Register the attribute in `empi_attribute_scores.csv`:**

Add a row for each custom attribute, specifying a weight appropriate to its discriminating power, whether it should use fuzzy matching, and the desired mismatch penalty. Refer to the existing rows in the seed for examples.

```csv
attribute,weight,use_fuzzy_match,fuzzy_threshold,exact_match_score,mismatch_penalty
NPI,...
EMPLOYEE_ID,...
```

**4. Optionally add the attribute to a blocking group in `empi_blocking_rules.csv`:**
```csv
5,EMAIL,true
6,NPI,true
```

**5. Re-run:**
```bash
dbt seed && dbt run
```

Custom attributes automatically participate in blocking, scoring, the review queues, and the audit trail. No model code changes required.


## Source configuration reference

All source table pointers live in `models/sources.yml` and are controlled by these vars:

| Variable | Description |
|---|---|
| `empi_input_database` | Database containing your source tables |
| `empi_input_schema` | Schema containing your source tables |
| `empi_eligibility_table` | Table name for the eligibility source (default: `eligibility`) |
| `empi_patient_table` | Table name for the patient source (default: `patient`) |
| `empi_custom_attributes_table` | Table name for custom attributes (default: `empi_custom_attributes`) |
| `empi_manual_review_database` | Database for manual review tables |
| `empi_manual_review_schema` | Schema for manual review tables (default: `empi_manual_review`) |
