---
id: cvx
title: "CVX"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 08-21-2025</em></small>
</div>

## Data Dictionary

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__cvx.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/cvx.csv_0_0_0.csv.gz">Download CSV</a>

## What is CVX?

**CVX** stands for *Vaccine Administered Code Set*. It is a standardized vocabulary to uniquely identify vaccines.

- **Maintained by**: **HL7 International** (FHIR ValueSet for vaccine codes)  
- **Purpose**: Provides a standard set of codes for vaccines to support interoperability across EHRs, immunization information systems (IIS), and claims data.  
- **Usage**: Vaccine administration, clinical documentation, public health reporting, analytics, and interoperability with HL7/FHIR standards.  

📎 [HL7 FHIR Vaccine Code ValueSet](https://hl7.org/fhir/R5/valueset-vaccine-code.html)  

## Who Maintains CVX?

- The **HL7 International Vocabulary/Terminology group** maintains the official *FHIR Vaccine Code ValueSet*, which includes CVX as a coding system.  
- Updates are published through FHIR releases and ballot cycles.  
- CVX continues to be used in parallel with other coding systems (e.g., SNOMED CT, ATC, RxNorm) to support interoperability.  

## Code Structure

Each **CVX code**:

- Is **numeric** (typically 1–3 digits).  
- Represents a **specific vaccine** or **vaccine grouping**.  
- Has associated fields in HL7 FHIR including:  
  - **code** → CVX code (numeric identifier)  
  - **Display** → shorter clinical description or context  
  - **English (English, en)** → shorter clinical description or context  

**Example** (from HL7 FHIR ValueSet):  
> `207`  
> `SARS-COV-2 (COVID-19) vaccine, mRNA, spike protein, LNP, preservative free, 100 mcg/0.5mL dose`  
> `COVID-19, mRNA, LNP-S, PF, 100 mcg/0.5 mL dose (Moderna)`   

## Key Use Cases for CVX Codes

- **Clinical Documentation**: Ensures consistent recording of administered vaccines in EHRs.  
- **Public Health Reporting**: Standardized reporting to immunization information systems (IIS).  
- **Claims Processing**: Used by payers and clearinghouses to identify vaccine services.  
- **Analytics & Research**: Vaccine uptake trends, coverage monitoring, safety and effectiveness studies.  
- **Interoperability**: Integrated with HL7 V2, CDA, and FHIR Immunization resources.  

### 📌 Notes for Data Analysts

- CVX codes can be paired with **MVX codes** (manufacturer codes) for complete vaccine identity.  
- Historical/retired codes are important for longitudinal patient records.  
- Mapping between **CVX and other code systems** (SNOMED CT, NDC, RxNorm) may be required depending on the use case.  
- Tuva’s source files preserve the official HL7 FHIR CVX ValueSet, including active, retired, and draft codes.  

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set.  

1. Navigate to the [HL7 FHIR Vaccine Code ValueSet](https://hl7.org/fhir/R5/valueset-vaccine-code.html).  
2. Download or extract the **ValueSet expansion (JSON/XML)** from HL7.  
3. Save the file locally and extract the coding system elements.  
4. Ensure the following fields are retained and mapped as:  
   - `Code` → **cvx**  
   - `English (English, en)` → **short_description**   
   - `Display` → **long_description**  
5. Convert to a **CSV file** in UTF-8 encoding.  
6. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/cvx.csv
from [table_created_in_step_6]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the CVX file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [CVX file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__cvx.csv)
3. Submit a pull request

