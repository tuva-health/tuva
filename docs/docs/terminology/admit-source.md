---
id: admit-source
title: "Admit Source"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__admit_source.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_source.csv_0_0_0.csv.gz">Download CSV</a>

## What are Admit Source Codes?

Admit Source Codes are standardized values used in institutional claims (especially inpatient) to indicate the source of a patient's admission to a healthcare facility. They describe how the patient arrived at the hospital—whether through the emergency room, physician referral, transfer from another facility, or other means.

## On what kind of claims are Admit Source Codes found?

Admit Source Codes are primarily found on **institutional inpatient claims**. They are part of the **header-level claim information** and are most relevant to **UB-04** (CMS-1450) claims submitted by hospitals and facilities for Medicare and other payers.

## How often are Admit Source Codes updated?

Admit Source Codes are relatively **stable** and not frequently changed. They are maintained by the **National Uniform Billing Committee (NUBC)**, and any updates are typically released in coordination with NUBC’s semiannual or annual updates. However, code definitions or usage guidelines may be clarified or expanded periodically.

## Code Structure

Admit Source Codes are typically **1-digit or 2-digit alphanumeric values**. Examples include:

| Code | Description                                 |
|------|---------------------------------------------|
| 1    | Physician referral                          |
| 2    | Clinic referral                             |
| 4    | Transfer from a hospital                    |
| 5    | Transfer from a skilled nursing facility    |
| 7    | Emergency room                              |
| 9    | Information not available                   |

Note: The list above is illustrative. Refer to the official NUBC manual or CMS specifications for the complete, up-to-date set.

## Notes for Analysts

- **Critical to care pathway analysis**: Knowing how a patient was admitted helps differentiate planned admissions (e.g. physician referral) from unplanned (e.g. emergency room).
- **Often under-utilized**: Admit Source is frequently overlooked in favor of other fields like Admission Type or Discharge Disposition but provides complementary insights.
- **Check for default or missing values**: Code "9" or null values may indicate incomplete data or facility-level reporting issues.
- **Use with caution across payers**: While required for Medicare inpatient claims, completeness and consistency may vary across commercial claims.

## Key Use Cases for Admit Source Codes

- **Distinguishing emergent vs. non-emergent admissions**.
- **Identifying care transitions**, such as transfers from other acute or post-acute facilities.
- **Analyzing readmission pathways**, especially for policy and quality measurement.
- **Supporting risk stratification models** by incorporating admission context.
- **Enhancing cohort definitions** for utilization or outcome studies.



## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

**The below description outlines the update process as it existed prior to changes in the ResDac site no longer publishing updates to this code set. Updates are currently on hold until a new source can be identified**

1. Navigate to the [ResDac Inpatient website](https://resdac.org/cms-data/files/ip-ffs)
2. Click "View Data Documentation" under the page title
3. Locate and select the Variable Name "Claim Source Inpatient Admission Code"
4. Open the .txt file at the bottom of the webpage 

Follow steps 5-11 if there are any changes to the admit source codes.  Otherwise, skip to step 12

5. Copy and paste the code list into any text editor
6. Format the codes as a CSV file and save
   - Find and replace "â€”" with a hyphen (-)
7. Import the CSV file into any data warehouse
8. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/admit_source.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
9. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date.
10. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [admit source file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__admit_source.csv)
3. Submit a pull request
