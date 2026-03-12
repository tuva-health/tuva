---
id: medicare-status
title: "Medicare Status"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__medicare_status.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_status.csv_0_0_0.csv.gz">Download CSV</a>


## What are Medicare Status Codes?

The **Medicare Status Code** is a single-character code assigned by CMS to indicate the type of Medicare coverage that a beneficiary has at a point in time. These codes are used to identify whether a person is enrolled in Medicare Part A, Part B, both, or neither, and they provide important context for coverage eligibility in claims and enrollment data.

Unlike the Original Reason for Entitlement Code (OREC), which describes how someone became eligible for Medicare, the **Medicare Status Code describes the current coverage status**.

## On What Kind of Claims are Medicare Status Codes Found?

Medicare Status Codes are **not found on individual claims** submitted by providers. Instead, they are present in:
- **Beneficiary enrollment and eligibility files** from CMS (e.g., MBSF Base, CCW extracts)
- **Denominator files** used in research and risk adjustment

They may be used as part of eligibility checks to determine whether a beneficiary is actively enrolled in Medicare coverage at the time of service.

## How Often are Medicare Status Codes Updated?

Medicare Status Codes are **updated monthly** (or more frequently) by CMS as enrollment statuses change. Updates occur when beneficiaries:
- Newly enroll or disenroll from Part A or B
- Transition between Medicare Advantage and Fee-for-Service
- Become deceased or lose eligibility

Because of their dynamic nature, these codes should be interpreted with respect to a specific **reference date or month**.

## Code Structure

Medicare Status Codes are typically **one-character alphanumeric codes**. Common codes include:

| Code | Description                               |
|------|-------------------------------------------|
| A    | Aged with Medicare Part A only            |
| B    | Aged with Medicare Part B only            |
| C    | Aged with both Part A and B               |
| D    | Disabled with Part A only                 |
| E    | Disabled with Part B only                 |
| F    | Disabled with both Part A and B           |
| G    | ESRD only with Part A and B               |
| H    | Aged or Disabled, no Part A or B (rare)   |
| M    | Medicare Advantage enrollment (MA plan)   |
| X    | Deceased                                  |

> Note: Exact codes and descriptions may vary slightly by CMS data product.

## Notes for Analysts

- **Time-sensitive**: Always consider the effective date of the Medicare Status Code when analyzing data across time periods.
- **Coverage check**: Use this field to confirm if a beneficiary had active coverage on the date of service.
- **Part A vs. Part B**: Important for distinguishing inpatient vs. outpatient eligibility.
- **Medicare Advantage**: The presence of certain codes (e.g., M) may indicate enrollment in Medicare Advantage, which affects claims visibility and care patterns.
- **Missing Claims**: Beneficiaries enrolled in Medicare Advantage often do not appear in Fee-for-Service claims data.

## Key Use Cases

- **Eligibility filtering**: Ensure beneficiaries are appropriately covered during study periods.
- **Attribution logic**: Exclude or include individuals in cohorts based on Medicare Advantage status.
- **Part A/B population segmentation**: Define populations by coverage type for utilization, cost, or quality measurement.
- **Data completeness assessments**: Identify gaps due to MA enrollment or inactive status (e.g., deceased).


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Medicare Status Code-Latest in Year](https://resdac.org/cms-data/variables/medicare-status-code-latest-year)
2. Scroll through the page and find the code and code value table    
3. Copy and paste the code list into any text editor or spreadsheet.
4. Format the codes as a CSV and save
5. Import the CSV file into any data warehouse
6. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/medicare_status.csv
from [table_created_in_step_5]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents of the medicare_status file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [medicare_status file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__medicare_status.csv)
3. Submit a pull request
