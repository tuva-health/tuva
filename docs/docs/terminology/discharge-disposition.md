---
id: discharge-disposition
title: "Discharge Disposition"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__discharge_disposition.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/discharge_disposition.csv_0_0_0.csv.gz">Download CSV</a>

## What are Discharge Disposition Codes?

Discharge Disposition Codes are standardized values used to indicate the patient’s status at the time of discharge from a healthcare facility. These codes capture whether the patient was discharged to home, transferred to another facility, left against medical advice, or died, among other possibilities.

These codes help healthcare payers, providers, and analysts understand patient flow and outcomes after an inpatient or outpatient encounter.

---

## On what kind of claims are Discharge Disposition Codes found?

Discharge Disposition Codes are primarily found on **institutional inpatient and outpatient facility claims**, especially those billed using the UB-04 (CMS-1450) claim form or its electronic equivalent (837I).

They are **not typically present** on professional claims (837P).

---

## How often are Discharge Disposition Codes updated?

Discharge Disposition Codes are maintained by the **National Uniform Billing Committee (NUBC)**. Updates are relatively infrequent but **do occur periodically**, often in response to changes in post-acute care policies or reimbursement structures. 

It’s important to reference the **most recent UB-04 manual or CMS documentation** for the latest valid values.

---

## Code Structure

- **Format**: 2-digit alphanumeric
- **Example Values**:
  - `01` – Discharged to home or self-care
  - `02` – Discharged/transferred to a short-term general hospital
  - `20` – Expired
  - `30` – Still patient (used for claims before discharge occurs)
  - `50` – Discharged to hospice – home
  - `63` – Discharged/transferred to long term care hospital (LTCH)

Some codes imply expected follow-up care, while others may trigger specific reimbursement considerations.

---

## Notes for Analysts

- The discharge disposition can influence **DRG assignment and reimbursement**, especially in MS-DRG and post-acute payment systems.
- Codes like `20` (expired) may be useful for **mortality tracking** or **utilization review**.
- Be cautious of **default or placeholder values** (e.g., `01` used excessively when the actual disposition is unknown).
- Some states or facilities may use **custom or legacy values**, especially in older datasets.

---

## Key Use Cases

- **Readmission tracking**: Determine if a patient was discharged home vs. to another facility, which may affect readmission definitions.
- **Mortality studies**: Identify in-hospital deaths using codes like `20`.
- **Post-acute planning**: Identify discharge to skilled nursing facilities, long-term hospitals, or hospice for care coordination.
- **Compliance audits**: Validate that discharge status aligns with documentation and billing (e.g., hospice or rehab transfers).
- **Bundled payments and cost modeling**: Distinguish between patients discharged home vs. those needing institutional follow-up care.



## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

**The below description outlines the update process as it existed prior to changes in the ResDac site no longer publishing updates to this code set. Updates are currently on hold until a new source can be identified**

1. Navigate to the [ResDac Inpatient website](https://resdac.org/cms-data/files/ip-ffs)
2. Click "View Data Documentation" under the page title
3. Locate and select the Variable Name "Patient Discharge Status Code"
4. Open the .txt file at the bottom of the webpage

Follow steps 5-11 if there are any changes.  Otherwise, skip to step 12

5. Copy and paste the code list into any text editor
6. Format the codes as a CSV file and save
   - Find and replace "â€”" with a hyphen (-)
7. Import the CSV file into any data warehouse
8. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/discharge_disposition.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
9. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
10. Copy and paste the updated codes into [Discharge Disposition file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__discharge_disposition.csv)
11. Submit a pull request
12. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
13. Submit a pull request
