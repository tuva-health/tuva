---
id: hcpcs-level-2
title: "HCPCS Level 2"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 05-21-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__hcpcs_level_2.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_level_2.csv_0_0_0.csv.gz">Download CSV</a>


## What is HCPCS Level II?

HCPCS Level II (Healthcare Common Procedure Coding System, Level II) is a standardized code set used in the United States to describe products, supplies, and services not included in the CPT (Current Procedural Terminology) codes. These include durable medical equipment (DME), ambulance services, prosthetics, orthotics, and certain drugs and non-physician services.

**Claim Usage:**
- HCPCS Level II codes typically appear on the **claim line level**, rather than the header.
- They are commonly used in **Medicare**, **Medicaid**, and **commercial insurance** claims—particularly **professional** and **outpatient institutional** claims (e.g., UB-04 forms).
- They are tied to both **billed charges** and **allowed amounts**, with pricing determined by a mix of national and regional **fee schedules** and **carrier-specific reimbursement policies**.

**Granularity and Pricing:**
- Each HCPCS code corresponds to a specific item or service (e.g., "E0110 – Crutch, underarm, wood, adjustable or fixed, pair").
- Pricing is informed by multiple structures:
  - **CMS DMEPOS fee schedule** (for durable equipment and prosthetics)
  - **Medicare ASP Drug Pricing** (for Part B drugs)
  - **Carrier/local MAC policies** for miscellaneous services

## Who Maintains HCPCS Level II?

The **Centers for Medicare & Medicaid Services (CMS)** maintains HCPCS Level II codes.

- CMS updates the HCPCS Level II code set **quarterly** and **annually**, with public comment periods.
- The **HCPCS Workgroup**, which includes representatives from CMS, private insurers, and other stakeholders, reviews code requests and proposes changes.

## Code Structure

HCPCS Level II codes:
- Begin with a **single letter (A–V)** followed by **four digits** (e.g., A0429, J3490).
- Groupings often reflect service categories:
  - `Axxxx` – Transportation services (e.g., ambulance)
  - `Bxxxx` – Enteral/parenteral therapy
  - `Exxxx` – DME equipment
  - `Jxxxx` – Drugs administered other than oral method
  - `Kxxxx`, `Qxxxx`, `Sxxxx`, `Txxxx` – Temporary or local codes

**Modifiers** are often appended (e.g., `J3301–LT`) to specify laterality, professional/technical components, or billing scenarios.

## Notes for Analysts

- HCPCS Level II codes are essential for understanding **non-physician services**, **DME claims**, and **injectable drug billing**.
- Often used in **line-item analysis** of claims; look for them in conjunction with:
  - **Revenue center codes** on UB-04s
  - **National Drug Codes (NDCs)** for precise identification of drugs
  - **Modifiers**, which may affect reimbursement and should be parsed carefully
- Analysts should account for **local coverage determinations (LCDs)** and **fee schedules** when comparing allowed vs. billed charges.

## Why Isn’t There a HCPCS Level I?

There *is* a Level I in HCPCS: it's the **CPT (Current Procedural Terminology)** code set, which covers physician and outpatient services.

However, the term “HCPCS” is often used synonymously with **Level II**, since **Level I (CPT)** is maintained and copyrighted by the **American Medical Association (AMA)**.

## Why Doesn’t Tuva Include CPT Codes in Its Terminology Set?

The **CPT code set** is owned by the **American Medical Association (AMA)**. Although CPT codes are **mandated for use** in nearly all U.S. medical billing systems, the AMA imposes strict **licensing restrictions** on redistribution or even descriptive usage of CPT codes.

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [CMS HCPCS Quarterly Update page](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update)

2. Download the latest ZIP file available.

3. Extract the file named `HCPC<year>_<month>_ANWEB.xlsx` from the ZIP.  
   *This is the only file needed.*

4. Load the extracted Excel file into your data warehouse.

5. Transform the uploaded data into a new table that matches the Tuva Terminology standard:

   | Source Column        | Tuva Column            |
   |----------------------|------------------------|
   | hcpc                 | hcpcs                  |
   | seqnum               | seqnum                 |
   | recid                | recid                  |
   | long description     | long_description       |
   | short description    | short_description      |

   - Ensure that null values are properly represented as `null`, not blank strings (`''`).

6. Unload the table to S3 as a `.csv` file (requires credentials with write permissions to the bucket):
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/hcpcs_level_2.csv
from 
(  select hcpcs, seqnum, recid, substr(long_description, 1, 2000), short_description
   from [table_created_in_step_5]
)
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
overwrite = true;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs). Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents of the terminology file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Copy and paste the updated header into the [HCPCS Level II file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__hcpcs_level_2.csv)
3. Submit a pull request
