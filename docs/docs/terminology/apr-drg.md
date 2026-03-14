---
id: apr-drg
title: "APR-DRG"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 05-21-2025</em></small>
</div>


import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__apr_drg.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/apr_drg.csv_0_0_0.csv.gz">Download CSV</a>

## What are APR-DRGs?

**APR-DRG** stands for *All Patient Refined Diagnosis Related Groups*. It is a patient classification system that categorizes hospital stays into groups based on clinical similarity and resource usage, incorporating both severity of illness (SOI) and risk of mortality (ROM).

APR-DRGs are designed to better reflect the complexity of care across all patient populations, including neonates, pediatrics, and adults.

## Who Created APR-DRG?

APR-DRGs were developed by **3M Clinical and Economic Research Department* in collaboration with the **Children's Hospital Association** and several physician groups. The system builds upon the earlier work on Refined DRGs by Yale University.

## How are APR-DRG Codes related to ICD-10-CM and ICD-10-PCS codes?

APR-DRG assignment is based on diagnosis and procedure codes submitted on inpatient claims, using the **ICD-10-CM** (diagnosis) and **ICD-10-PCS** (procedure) classification systems. The grouping logic maps combinations of codes to a base DRG, then assigns levels for severity of illness and risk of mortality.

## How are APR-DRG Codes different from MS-DRG Codes?

While **MS-DRGs** (Medicare Severity DRGs) are used primarily by Medicare for payment of adult inpatient hospital stays, **APR-DRGs** are designed to:

- Capture a **broader population**, including pediatrics and obstetrics.
- Include **four severity of illness (SOI)** and **four risk of mortality (ROM)** subclasses per base DRG.
- Provide **greater granularity** for comparing resource utilization and outcomes across hospitals.
- Be used more often in **state Medicaid programs** and **commercial payers**.

MS-DRGs, by contrast, use fewer subclasses and are more focused on Medicare beneficiaries and payment alignment.

## Who Maintains APR-DRG Codes?

APR-DRGs are **maintained by Solventum, a spin-off of 3M Health Care**, a private company. Unlike MS-DRGs (which are publicly available through CMS), the use of APR-DRGs typically requires a **commercial license**.

Organizations that wish to use the official grouping logic or software must obtain a license from 3M.

## On what kind of claims are APR-DRG Codes found?

APR-DRG codes are found on **inpatient facility claims**. They are most commonly used in:

- **State Medicaid programs**
- **Commercial insurers**
- **Children’s hospitals and academic medical centers**

They are not typically found on Medicare fee-for-service claims.

## How are APR-DRG Codes related to cost?

Like MS-DRGs, APR-DRGs serve as a proxy for **resource intensity and hospital cost**. The assigned APR-DRG, in combination with its severity level, can drive:

- **Reimbursement rates**
- **Cost benchmarking**
- **Performance measurement**

Some payers may assign weights to each APR-DRG and SOI combination to calculate expected payment or cost.

## Code Structure

An APR-DRG code has the following structure:

- **Three-digit base DRG** (e.g., 139 – "Other Pneumonia")
- **Two single-digit modifiers**:
  - **Severity of Illness (SOI)**: 1 (minor) to 4 (extreme)
  - **Risk of Mortality (ROM)**: 1 (minor) to 4 (extreme)

A complete grouping might be expressed as:  
**139-3-2** = DRG 139, SOI level 3, ROM level 2

Some systems may concatenate these into a single string (e.g., `13932`).

## Notes for Analysts

- APR-DRGs are often **not present in publicly available claims data**, especially Medicare FFS. Verify availability before use.
- A single DRG (e.g., 139) may be split into **16 possible combinations** (4 SOI × 4 ROM).
- Severity levels are **relative** and based on clinical coding, not lab values or vitals.
- **Inpatient claims without APR-DRGs** may occur if the grouper was not applied or if licensing is restricted.

## Key Use Cases for APR-DRG Codes

- **State Medicaid rate setting**
- **Children’s hospital comparisons**
- **Quality and outcome benchmarking**
- **Case-mix adjustment** for pediatric and obstetric populations
- **Population health analytics** for commercial insurers


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Solventum APR DRG page](https://www.solventum.com/en-us/home/h/f/b5005024009/)
2. Find the description files worded 'Solventum APR DRG descriptions'
3. Open the latest description file, named 'APR DRG &lt;latestversion&gt; descriptions'
4. Copy the code block from the file and paste it into a text editor.
5. Format the codes as a CSV file and save
    - You can paste it into Google Sheets or Excel
    - Use the pipe symbol (`|`) as the custom delimiter
    - Save/export the sheet as a `.csv` file
6. Import the CSV file into your data warehouse
    - Ensure that empty fields are imported as `null`, not blank strings (`''`)
7. Transform the uploaded data to another table to match the Tuva Terminology standard:
    - DRG → apr_drg_code
    - Type → medical_surgical
    - MDC → mdc_code
    - Long Description → apr_drg_description
8. Unload the table from the data warehouse to a CSV file in S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/apr_drg.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
overwrite = true;
```
9. Create a branch in [docs](https://github.com/tuva-health/docs). Update the `last_updated` column in the table above with the current date
10. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [APR-DRG file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__apr_drg.csv)
3. Submit a pull request
