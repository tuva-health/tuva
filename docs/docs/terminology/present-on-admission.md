---
id: present-on-admission
title: "Present on Admission"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__present_on_admission.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/present_on_admission.csv_0_0_0.csv.gz">Download CSV</a>

## What are Present on Admission (POA) Indicators?

The **Present on Admission (POA)** indicator is a code used on inpatient hospital claims to specify whether a diagnosis was present at the time the order for inpatient admission occurred. POA indicators help differentiate between pre-existing conditions and conditions that arose during hospitalization.

These indicators are critical for quality reporting and payment determination, especially in the context of Hospital-Acquired Condition (HAC) reduction programs.

---

## On what kind of claims are POA Indicators found?

- **Institutional inpatient claims** (UB-04 / CMS-1450)
- They apply to **ICD diagnosis codes** reported on inpatient stays.
- Not required for outpatient or professional claims.

---

## How often are POA Indicators updated?

- The **rules and guidance** for POA indicator use may be updated **annually** in conjunction with **ICD-10-CM updates** and CMS policy changes.
- However, the **set of valid POA codes** and their meanings are **stable** and do not change frequently.

---

## Code Structure

Each diagnosis code on an inpatient claim (except for exempt codes and the principal diagnosis in certain cases) is accompanied by a single-character POA indicator:

| Code | Description                                                                 |
|------|-----------------------------------------------------------------------------|
| Y    | Diagnosis was present at the time of inpatient admission                   |
| N    | Diagnosis was not present at the time of inpatient admission               |
| U    | Documentation is insufficient to determine whether the condition was POA   |
| W    | Clinically undetermined—provider is unable to determine if condition was POA |
| 1    | Exempt from POA reporting (typically for codes where POA is not applicable)|

---

## Notes for Analysts

- **POA indicators are not reported** for certain diagnosis codes considered *exempt* by CMS. These are generally conditions that are always present (e.g., congenital conditions) or that are not relevant for POA tracking.
- POA indicators **do not apply** to the **external cause of injury codes** unless the condition they relate to is not exempt.
- Pay special attention to **secondary diagnosis codes** when using POA for quality analysis or HAC flagging.
- Diagnosis codes with POA = 'N' can affect hospital payment through HAC reduction programs.

---

## Key Use Cases for POA Indicators

- **Identifying hospital-acquired conditions (HACs)** for quality measures.
- **Risk adjustment and severity indexing** by distinguishing pre-existing conditions.
- **Payment penalties or exclusions** under CMS HAC and Value-Based Purchasing programs.
- **Clinical quality audits** to assess documentation completeness or compliance.


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Present on Admission](https://www.cms.gov/medicare/payment/fee-for-service-providers/hospital-aquired-conditions-hac/coding)
2. Scroll through the page and find the **CMS POA Indicator Options and Definitions** section    
3. Copy and paste the code list into any text editor or spreadsheet
4. Format the codes as a CSV and save
5. Import the CSV file into any data warehouse
6. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/present_on_admission.csv
from [table_created_in_step_5]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```

7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents of the present_on_admission file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [present_on_admission file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__present_on_admission.csv)
3. Submit a pull request
