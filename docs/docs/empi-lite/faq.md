# Frequently Asked Questions


## General

### What exactly is a master patient index?

A master patient index (MPI) is a system that links patient records across disparate source systems and assigns a single persistent identifier to each real person. When a patient appears in your EHR, claims data, lab system, and state registry under four different IDs, an MPI is what tells you all four records belong to the same individual.

Without an MPI, patient-level analytics - utilization, cost, outcomes, attribution - are fundamentally unreliable. Records get double-counted, care histories get fragmented, and outreach reaches the wrong people or the same person multiple times.

### What makes EMPI Lite different from other patient matching solutions?

Three things:

1. **You own it.** EMPI Lite is delivered as dbt source code you clone and run in your own warehouse. No black-box API, no data sent to a third party, no vendor dependency at runtime.

2. **It is transparent.** Every match comes with a plain-English narrative explaining exactly why it was made. Every configuration knob - blocking rules, attribute weights, thresholds - is an editable CSV or a dbt variable. Nothing is hidden.

3. **It runs where your data lives.** Because EMPI Lite is just SQL in dbt, it runs inside whatever warehouse you already use. No data movement, no ETL to a vendor system, no HIPAA BAA required for the tool itself.

### Is EMPI Lite a SaaS product?

No. EMPI Lite is delivered as source code. You host it, you run it, you own it. There is no hosted version, no runtime dependency on our infrastructure, and no data that leaves your environment.


## Data and privacy

### Does patient data get sent anywhere?

No. EMPI Lite runs entirely inside your warehouse as dbt SQL. The only data movement is within your own infrastructure.

### Do I need a BAA (Business Associate Agreement) to use EMPI Lite?

EMPI Lite itself processes no data - it is code that runs in your warehouse. Your existing data agreements with your warehouse provider cover the data. There is no separate BAA required for the tool.

### Can EMPI Lite handle PHI / HIPAA data?

Yes. Because the tool runs inside your existing warehouse environment, PHI is subject to exactly the same controls as the rest of your data. EMPI Lite does not add any new data exposure surface.


## Matching accuracy

### How accurate is the matching?

Match quality depends heavily on data quality in your source systems. In typical healthcare data environments with reasonable demographic completeness:

- Auto-matched records are typically very high precision (>95%) at the default 70% threshold
- Common failure modes are missing attributes (no SSN in one system) and data entry errors (name misspellings, transposed DOB digits)

The precision/recall evaluation framework (`empi_threshold_analysis`) lets you measure accuracy precisely if you have ground-truth data.

### What percentage of records will be matched vs. singletons?

This depends entirely on your input data. The key factors are:

- **How many source systems** you are matching across - more systems mean more potential pairs
- **Population overlap** across systems - if all sources serve the same patients, match rates are high; if they serve largely distinct populations, most records will be singletons
- **Attribute completeness** - sparse demographics (missing SSN, inconsistent names) reduce blocking recall
- **Data quality** - name misspellings, transposed DOB digits, and shared placeholder values all affect results

There is no meaningful baseline to cite. Run the pipeline on your data and examine the match status distribution in `empi_crosswalk` to understand your own population.

### What happens to patients who don't match anything?

They become `SINGLETON` records - they get their own unique `empi_id` and appear in the crosswalk, golden record, and event trail as single-record clusters. Singletons are first-class citizens. Being unmatched is a valid and expected state.

### Can EMPI Lite match across more than two source systems?

Yes. Matching is transitive. If patient A (EHR) matches patient B (claims), and patient B (claims) matches patient C (lab), all three get the same `empi_id` via connected-components clustering - even if A and C were never directly compared.

### What if two records match that shouldn't?

This is a false positive (Type I error). Options:

1. **Raise the match threshold** - reduces false positives but may increase missed matches
2. **Increase mismatch penalties** for critical attributes (e.g., SSN) - a confirmed SSN mismatch will overpower other agreements
3. **Use the review queue** - move borderline pairs into the review zone for human adjudication before auto-matching
4. **Insert a row into `match_review_decisions`** with `is_match = FALSE, reviewed = TRUE` for specific pairs that should be permanently suppressed

### What if two records that should match don't?

This is a false negative (Type II error). Options:

1. **Lower the match threshold**
2. **Add a blocking group** that would catch these pairs (e.g., if they share a phone number but nothing else, add PHONE as a blocking group)
3. **Insert a row into `match_review_decisions`** with `is_match = TRUE, reviewed = TRUE` to manually link specific pairs
4. **Enable custom attributes** if the shared identifier isn't in the default attribute set


## Setup and integration

### How long does initial setup take?

For most teams: **1-2 days** to configure and run successfully, **3-5 more days** to validate match quality and tune thresholds. The review queue workflow takes 1-2 weeks to establish depending on queue volume.

### Do I need to modify the dbt models to point at my data?

No. Source table pointers are configured via vars in `dbt_project.yml`. You do not need to edit any SQL.

### Can EMPI Lite run alongside my existing dbt project?

Yes. Use the local package import pattern - EMPI Lite runs as a package inside your existing dbt project and builds alongside your existing models in a single `dbt run`. See [Getting Started](./getting-started.md).

### What warehouses are supported?

Snowflake, BigQuery, Amazon Redshift, Microsoft Fabric (Synapse / SQL Server), Databricks, and DuckDB.

### Do I need Tuva to use EMPI Lite?

No. Tuva integration is optional. EMPI Lite's core outputs - `empi_crosswalk`, `empi_golden_record`, `empi_patient_events`, and the review queues - stand on their own and can be consumed by any downstream model or BI tool.

### How does EMPI Lite work with Tuva?

EMPI Lite is designed to run **alongside** Tuva as an upstream identity resolution step. The intended pipeline is:

1. Your raw source data feeds into EMPI Lite
2. EMPI Lite resolves patient identity and produces Tuva-compatible `tuva_patient` and `tuva_eligibility` tables where `person_id = empi_id`
3. Your Tuva pipeline reads from those EMPI-resolved tables instead of your raw source tables
4. All downstream Tuva marts inherit `empi_id` as the patient identifier automatically

If you are currently running Tuva, integrating EMPI Lite requires a one-time reconfiguration of your Tuva input layer to point at the EMPI output tables. After that change, no further modifications to the Tuva pipeline are needed.


## Operations

### How often should I run dbt run?

As often as your source data refreshes. Common cadences:

- **Daily**: run `dbt snapshot && dbt run` nightly after source data loads
- **Weekly**: run weekly if source data refreshes weekly
- **On-demand**: run after major source data loads

The pipeline is stateless by default - each run re-derives the full crosswalk from current source data. Snapshot-based change detection (`empi_snapshot_enabled: true`) adds state tracking across runs.

### Can I add a new source system after the initial run?

Yes. Add the new source system's records to your `eligibility` or `patient` source tables, and re-run. EMPI Lite will match new records against the existing population on the next run. New matches generate `EMPI_MATCH` events and updated crosswalk assignments.

### What happens to EMPI IDs when records change?

By default (without snapshots), `empi_id` is derived from the current state of the data on each run. If a record's demographics change in a way that affects cluster membership, its `empi_id` could change.

With snapshots enabled (`empi_snapshot_enabled: true`), EMPI Lite detects these changes and logs `EMPI_ID_CHANGED` events, giving you a full history of how identity assignments have evolved.

### How do I handle a source system that is decommissioned or renamed?

Update your source data to remove or rename the decommissioned records, then re-run. Manual review decisions referencing the old source IDs are still honored (they are stored permanently in the worklist tables). The crosswalk for decommissioned records will no longer be populated.

### Can I run EMPI Lite incrementally?

EMPI Lite's intermediate matching models are full-refresh by design - the connected-components clustering is a global algorithm that requires seeing all records simultaneously. Staging models can be made incremental for staging only if desired.

For very large datasets, the primary performance lever is blocking rule efficiency (fewer, tighter blocking groups = fewer candidate pairs = faster scoring) and warehouse compute sizing.


## Customization

### Can I add attributes that aren't in the default list?

Yes - via the custom attributes table. Any attribute can be registered as a custom attribute and will participate in blocking, scoring, review queues, and the audit trail. See [Configuration Guide - Custom Attributes](./configuration.md#custom-attributes).

### Can I change how names are cleaned or standardized?

Yes. Name standardization (lowercasing, stripping punctuation, normalizing spaces) happens in the staging model. You can fork the staging model and add custom logic - for example, nickname normalization, handling hyphenated names, or stripping organization prefixes.

### Can I add my own blocking groups?

Yes - add rows to `empi_blocking_rules.csv`. Any attribute (including custom attributes) can be a blocking key. See [Configuration Guide - Blocking Rules](./configuration.md#blocking-rules).

### Can I change the scoring algorithm?

The scoring weights, fuzzy thresholds, and mismatch penalties are all configurable via `empi_attribute_scores.csv` without modifying code. The underlying scoring model (Levenshtein similarity for fuzzy, geographic proximity for ZIP) is implemented in the matching engine. If you need a fundamentally different algorithm for a specific attribute, you can fork the relevant intermediate model.


## Upgrading

### How are new versions delivered?

New versions are pushed to the repo. You pull the latest code, preserve your customizations (blocking rules, attribute scores, dbt vars), and re-run.

### What customizations might I need to preserve across upgrades?

- `seeds/empi_blocking_rules.csv` - if you've added custom blocking groups
- `seeds/empi_attribute_scores.csv` - if you've tuned weights or added custom attributes
- `seeds/empi_invalid_values.csv` - if you've added custom invalid values
- `dbt_project.yml` - your vars and thresholds
- `models/sources.yml` - your source table pointers

Core model SQL (staging, intermediate, final), macros, snapshots, and tests are safe to overwrite from new versions.

### Will upgrades change my empi_ids?

Version upgrades that touch core matching logic may affect `empi_id` assignments on the next full run. We flag in release notes when this is the case. If stability of `empi_id` across versions is critical for your use case, run with snapshots enabled - changes will be logged in `empi_patient_events` as `EMPI_ID_CHANGED` events.
