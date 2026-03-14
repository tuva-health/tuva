---
id: admit-type
title: "Admit Type"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__admit_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_type.csv_0_0_0.csv.gz">Download CSV</a>


## What are Admit Type Codes?

Admit Type Codes are standardized values used on institutional (facility) inpatient claims to indicate the **general nature of a patient's admission**. These codes provide context about the urgency or purpose of a hospital admission and are commonly used in conjunction with other claim-level information such as Admit Source and Patient Status.

They are maintained by the **National Uniform Billing Committee (NUBC)** and appear in the UB-04 manual under **Form Locator 14**.

---

## On what kind of claims are Admit Type Codes found?

Admit Type Codes are found **only on inpatient institutional claims** submitted using the **UB-04 claim form (or its electronic equivalent, the 837I format)**. They are **not present on outpatient or professional (CMS-1500/837P) claims**.

---

## How often are Admit Type Codes updated?

Admit Type Codes are maintained by the **NUBC** and are updated **infrequently**—typically only when a new category is introduced or when clarification of an existing category is needed. It is important to track any changes across **UB-04 manual revisions** or **quarterly CMS/NUBC updates**.

---

## Code Structure

Admit Type Codes are **1-digit numeric values** from `1` to `9`, each representing a category of admission:

| Code | Description                   |
|------|-------------------------------|
| 1    | Emergency                     |
| 2    | Urgent                        |
| 3    | Elective                      |
| 4    | Newborn                       |
| 5    | Trauma Center                 |
| 6    | Not Used                      |
| 7    | Information Not Available     |
| 8    | Reserved for National Assignment |
| 9    | Reserved for National Assignment |

Note: Codes `6`, `8`, and `9` are not in active use but may appear in datasets due to legacy or invalid submissions.

---

## Notes for Analysts

- **Admit Type should not be used alone** to infer acuity or severity; pair with Admit Source, DRG, and diagnosis codes for a fuller picture.
- Code `7` ("Information Not Available") is sometimes used when admission data is incomplete—flag such records for review.
- Some payers may require specific Admit Types to trigger payment or coverage validation, particularly for Trauma (`5`).

---

## Key Use Cases for Admit Type Codes

- **Segmentation** of inpatient utilization by admission type (e.g., comparing elective vs. emergency admissions).
- **Trend analysis** of emergency vs. scheduled admissions.
- **Payment logic** in risk-adjustment or rate-setting models.
- **Quality analysis**, especially for identifying avoidable admissions.
- **Denial management** and claim audits for incorrectly coded admissions.

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

**The below description outlines the update process as it existed prior to changes in the ResDac site no longer publishing updates to this code set. Updates are currently on hold until a new source can be identified**

1. Navigate to the [ResDac Inpatient website](https://resdac.org/cms-data/files/ip-ffs)
2. Click "View Data Documentation" under the page title
3. Locate and select the Variable Name "Claim Inpatient Admission Type Code"
4. Open the .txt file at the bottom of the webpage 

Follow steps 5-11 if there are any changes.  Otherwise, skip to step 12

5. Copy and paste the code list into any text editor
6. Format the codes as a CSV file and save
   - Find and replace "â€”" with a hyphen (-)
7. Import the CSV file into any data warehouse
8. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/admit_type.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
9. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
10. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [Admit type file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__admit_type.csv)
3. Submit a pull request
