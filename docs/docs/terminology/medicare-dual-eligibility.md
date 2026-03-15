---
id: medicare-dual-eligibility
title: "Medicare Dual Eligibility"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__medicare_dual_eligibility.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/medicare_dual_eligibility.csv_0_0_0.csv.gz">Download CSV</a>

## What is the Medicare Dual Eligibility Code?

The **Medicare Dual Eligibility Code** identifies a Medicare beneficiary’s Medicaid status during a given month. It is assigned by the Centers for Medicare & Medicaid Services (CMS) and used in various CMS datasets — including the Monthly Enrollment Detail (EDB) and Medicare Enrollment Database (EDB) files — to describe the type of Medicaid benefits received in addition to Medicare.

This code is crucial for identifying **dually eligible beneficiaries** — individuals enrolled in both Medicare and Medicaid — and for distinguishing between different levels of Medicaid assistance.

---

## Where Is This Code Used?

The Dual Eligibility Code is included in CMS administrative files such as:

- Medicare Enrollment Database (EDB)
- Master Beneficiary Summary File (MBSF)
- Monthly Enrollment Detail files
- Medicare Advantage encounter data and claims data (derived or appended)
  
It is assigned monthly and reflects the beneficiary’s dual status for that calendar month.

---

## Code Values and Meanings

CMS defines the following standard dual eligibility codes:

| Code | Meaning                                                                 |
|------|-------------------------------------------------------------------------|
| 01   | Eligible for Medicare and full Medicaid benefits                        |
| 02   | Qualified Medicare Beneficiary (QMB) only                               |
| 03   | QMB plus full Medicaid benefits                                         |
| 04   | Specified Low-Income Medicare Beneficiary (SLMB) only                  |
| 05   | SLMB plus full Medicaid benefits                                       |
| 06   | Qualified Individual (QI)                                               |
| 08   | Other dual eligibility (e.g., medically needy or other limited benefits)|
| 09   | Not dually eligible (Medicare only)                                     |

> **Note:** Some datasets may also include code `00` for unknown or missing data, depending on vintage and source.

---

## Notes for Analysts

- **Assigned Monthly:** Dual eligibility codes are assigned by CMS on a **monthly basis** and may change over time for an individual.
- **Full vs. Partial Duals:**
  - *Full duals* typically include codes 01, 03, and 08 — these beneficiaries receive full Medicaid benefits.
  - *Partial duals* (codes 02, 04, 05, 06) receive Medicaid assistance for premiums and/or cost-sharing but not full Medicaid.
- **Code 09** indicates Medicare-only status (i.e., the individual is not enrolled in Medicaid at all).
- **Missing or Unknown Codes:** These should be interpreted carefully — they may result from data lags, enrollment processing issues, or eligibility transitions.
- **Impact on Coverage and Cost:** Dual status can affect beneficiary cost-sharing, benefits received, and risk adjustment in various Medicare payment models.

---

## Key Use Cases

- **Policy Analysis:** Understanding dual eligibility is essential for studying low-income populations, evaluating Medicaid expansion, or analyzing care models like D-SNPs and PACE.
- **Program Eligibility:** Dual status is a qualifying factor for many CMS programs and waivers, including LIS (Low-Income Subsidy) and Medicare Savings Programs.
- **Stratified Reporting:** Used in CMS reporting and research to segment beneficiaries by income and benefit richness for quality, cost, and utilization comparisons.


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Medicare-Medicaid Dual Eligibility Code - Latest in Year](https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-latest-year#:~:text=CMS%20generally%20considers%20beneficiaries%20as,%2C%2005%2C%20or%2006).
2. Scroll through the page and find the code and code value table.    
3. Copy and paste the code list into any text editor
4. Format the codes as a CSV and save
5. Import the CSV file into any data warehouse
6. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/medicare_dual_eligibility.csv
from [table_created_in_step_5]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the medicare_dual_eligibility file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [medicare dual eligibility](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__medicare_dual_eligibility.csv) file
3. Submit a pull request

