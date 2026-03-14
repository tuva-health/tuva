---
id: medicare-orec
title: "Medicare OREC"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__medicare_orec.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_orec.csv_0_0_0.csv.gz">Download CSV</a>

## What are Medicare OREC Codes?

The **Original Reason for Entitlement Code (OREC)** is a one-digit code used by the Centers for Medicare & Medicaid Services (CMS) to indicate the beneficiary’s original reason for becoming entitled to Medicare coverage. It reflects the initial eligibility pathway and is based on Title II of the Social Security Act.

There are three primary entitlement reasons reflected by OREC codes:
- **01** – Aged (65 or older)
- **02** – Disabled (under age 65 and entitled due to disability)
- **03** – End-Stage Renal Disease (ESRD)

OREC is a beneficiary-level variable and does not change over time, even if the person later becomes entitled under a different reason (e.g., an individual originally entitled due to ESRD who later qualifies due to age will retain the original ESRD OREC).

## On What Kind of Claims are OREC Codes Found?

OREC codes are **not found on individual claims** submitted by providers. Instead, they are found in **CMS beneficiary eligibility and enrollment files**, such as:
- Medicare Beneficiary Summary Files (MBSF)
- Master Beneficiary Summary File (MBSF Base)
- Chronic Conditions Warehouse (CCW) data extracts

Analysts often use OREC when working with longitudinal beneficiary-level data to stratify populations or segment by original coverage criteria.

## How Often are OREC Codes Updated?

OREC codes are **assigned once at the point of original entitlement** and generally remain **static** throughout the beneficiary’s life. CMS does not typically update these codes retroactively, even if a beneficiary’s circumstances change.

Because of this, OREC is:
- Stable across time
- Useful for grouping beneficiaries by original coverage pathway
- Not sensitive to year-over-year data updates

## Code Structure

OREC is a **one-character numeric code**, commonly formatted as a string in data extracts. The most common values are:

| Code | Description                    |
|------|--------------------------------|
| 01   | Aged (65+)                     |
| 02   | Disabled (under 65)            |
| 03   | End-Stage Renal Disease (ESRD) |
| Unknown or blank | Sometimes appears in research extracts where entitlement info is unavailable or suppressed |

## Notes for Analysts

- **Fixed Characteristic**: OREC reflects the original eligibility pathway and does **not update** when a beneficiary becomes eligible under a different condition.
- **Stratification Tool**: It is useful for defining cohorts such as “originally disabled” or “originally ESRD,” which may have different utilization patterns or risk profiles than aged enrollees.
- **Limitations**: It does not reflect current eligibility status. For example, a person with OREC=03 (ESRD) may now be aged 70 and no longer in dialysis, but the code remains ESRD.

## Key Use Cases

- **Population segmentation**: Differentiate between aged, disabled, and ESRD populations when conducting descriptive or comparative analyses.
- **Risk adjustment models**: Account for differences in baseline entitlement that may correlate with morbidity or healthcare utilization.
- **Policy evaluation**: Analyze the effects of Medicare policy changes on specific beneficiary subgroups (e.g., ESRD coverage expansions).
- **Eligibility-based studies**: Identify and track outcomes by initial reason for Medicare enrollment.


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Medicare OREC](https://resdac.org/cms-data/variables/medicare-original-reason-entitlement-code-orec).
2. Scroll through the page and find the *code* and *code value* table section.    
3. Copy and paste the code list into any text editor or spreadsheet.
4. Format the codes as a CSV and save
5. Import the CSV file into any data warehouse
6. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/medicare_orec.csv
from [table_created_in_step_5]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the medicare_orec file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [medicare_orec file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__medicare_orec.csv)
3. Submit a pull request

