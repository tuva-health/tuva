---
id: mdc
title: "MDC"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__mdc.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/mdc.csv_0_0_0.csv.gz">Download CSV</a>


## What are Major Diagnostic Categories (MDCs)?

Major Diagnostic Categories (MDCs) are broad groupings of diagnoses that represent the primary reason for hospital inpatient admission. Each MDC corresponds to a body system or a specific medical condition, such as diseases of the circulatory system or pregnancy and childbirth.

MDCs are used as the first level of classification in the Medicare Severity Diagnosis Related Groups (MS-DRGs) system. There are typically 25–27 MDCs in each version, with some additional special assignment codes (e.g., MDC 0 or 99) used in certain cases.

## How are they related to MS-DRG Codes?

Every MS-DRG is assigned to a single MDC based on the principal diagnosis and procedure codes on a claim. The MDC acts as the “parent” category that determines the general clinical grouping of the DRG.

> Example:  
> MDC 05 — Diseases & Disorders of the Circulatory System  
> → DRG 280 (Acute Myocardial Infarction), DRG 291 (Heart Failure)

## How are MDC Codes related to ICD-10-CM and ICD-10-PCS codes?

MDC assignment is **derived** from:
- The **principal ICD-10-CM diagnosis code**
- In some cases, the **ICD-10-PCS procedure code**

The official CMS MS-DRG Grouper processes these codes to assign an MS-DRG, which in turn is associated with one MDC.

## On what kind of claims are MDC Codes found?

MDCs are **not explicitly included** on the claim record.

However:
- They are **inferred** from the DRG, which *is* found on institutional (inpatient) claims.
- MDCs are **used internally** in the MS-DRG classification logic but are not stored as a discrete field on standard claim files.

> **If you need to assign an MDC:**  
> - Use the DRG on the claim to look up the corresponding MDC in a reference file.  
> - Or run the claim through a Grouper if DRG is not available.

## Do MDC Code Values change as MS-DRG versions change?

Yes. With each annual update to the MS-DRG Grouper (effective October 1), CMS may:
- Add or revise DRG-to-MDC mappings
- Change logic for how ICD codes map into the system
- Add new special MDCs (e.g., MDC 0 for Pre-MDC transplant and ECMO cases)

## How do MDC Codes Compare to Other Diagnostic Groupers like CCSR?

MDCs and other groupers such as **CCSR (Clinical Classifications Software Refined)** serve similar purposes—organizing diagnosis data into higher-level clinical categories—but they differ in key ways:

| Feature | MDC | CCSR |
|--------|-----|------|
| Maintained By | CMS | AHRQ (Agency for Healthcare Research and Quality) |
| Based On | DRG logic and inpatient claims | ICD-10-CM diagnosis codes |
| Used In | Inpatient hospital reimbursement | Clinical analytics, dashboards, utilization studies |
| Claim Requirement | Requires grouped DRG | Can be applied directly to ICD-10-CM diagnoses |
| Granularity | Broad (25–27 categories) | More specific (approx. 530 categories) |
| Body System vs. Clinical | Body system-based | Clinically nuanced (e.g., separates anxiety vs. depression) |

In summary:
- **MDCs** are tightly coupled to the **MS-DRG reimbursement logic**
- **CCSR** and other tools are often used for **analytic segmentation** across **all claim types**, not just inpatient

## Code Structure

- MDC codes are 2-digit numeric codes:
  - `01`–`25`: Body systems (e.g., Nervous System, Digestive System)
  - `00`: Pre-MDC (transplants, ECMO)
  - `99`: Invalid or ungroupable
- Each MDC corresponds to a clinical area and includes many DRGs

## Notes for Analysts

- MDCs are useful for **high-level rollups** of inpatient utilization and cost
- Each inpatient claim with a valid DRG can be mapped to exactly **one MDC**
- Some MDCs are **mutually exclusive** (e.g., Pregnancy vs. Digestive)
- Be cautious when using MDCs to analyze transfers or **partial payments**—MDCs do not reflect final payment details

## Key Use Cases for MDC Codes

- **Clinical dashboards** that track inpatient service lines
- **Financial modeling** based on DRG families
- **Policy analysis** involving MS-DRG changes or population segmentation
- **Case mix analysis** for health systems or payers

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [CMS MS DRG website](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software)
2. Under the section "MS-DRG Definitions Manual and Software", click on "V42 Definitions Manual Table of Contents - Full Titles - HTML Version"
    - The version (e.g. V42) will change with each new release.    
3. Click on the hyperlink "Design and Development of the Diagnosis Related Group (DRGs)"
4. Scroll through the PDF to find the "Major Diagnostic Categories" table
5. Copy and paste the code list into any text editor
6. Format the codes as a CSV and save
7. Import the CSV file into any data warehouse
8. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/mdc.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
9. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
10. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents
of the mdc file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [MDC file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__mdc.csv)
3. Submit a pull request

