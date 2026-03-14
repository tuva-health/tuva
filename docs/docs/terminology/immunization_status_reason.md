---
id: immunization-status-reason
title: "Immunization Status Reason"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 08-21-2025</em></small>
</div>

## Data Dictionary

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm jsonPath="nodes.seed\.the_tuva_project\.terminology__immunization_status_reason.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_status_reason.csv_0_0_0.csv.gz">Download CSV</a>

## What is Immunization Status Reason?

**Immunization Status Reason** defines coded reasons why an immunization was given or not given.  

- **Maintained by**: HL7 International (FHIR Infrastructure Work Group)  
- **Purpose**: Standardizes reasons related to immunization events (e.g., contraindications, immunity, vaccine unavailability).  
- **Usage**: Used in FHIR Immunization resources to explain immunization status.  

📎 [HL7 FHIR ValueSet: Immunization Status Reason (R4)](https://hl7.org/fhir/R4/valueset-immunization-status-reason.html)

## Who Maintains Immunization Status Reason?

- Maintained by the **HL7 FHIR Infrastructure Work Group**.  
- Published as part of HL7 FHIR R4.  

## Code Structure

Each **Immunization Status Reason** includes:

- A **code** representing the reason.  
- A **display** string.  
- A **definition** explaining the context.  

Example codes:

| Code       | Display             | Definition                           |
|------------|---------------------|---------------------------------------|
| IMMUNE     | Immunity            | Patient already has immunity          |
| MEDPREC    | Medical Precaution  | Administration not done for safety    |
| OSTOCK     | Out of Stock        | Vaccine unavailable at time of visit |
| PATOBJ     | Patient Objection   | Patient or guardian refused vaccine   |

## Key Use Cases

- **Clinical Context**: Explains why immunization was skipped or not needed.  
- **Public Health**: Helps track vaccine shortages or refusal reasons.  
- **Analytics**: Identifies immunization gaps and barriers.  

### Notes for Data Analysts

- Always consider these codes in conjunction with immunization status codes.  
- Some codes represent "not given" scenarios, while others describe positive immunity status.  

## Tuva Seed File Update Process

This is the process for updating the terminology in Tuva’s package:

1. Navigate to the [Immunization Status Reason ValueSet](https://hl7.org/fhir/R4/valueset-immunization-status-reason.html).
2. Navigate to **Expansion**
3. Download or extract the data.  
4. Save the file locally and extract the coding system elements.  
5. Ensure the following fields are retained and mapped as:  
   - `code` → **ROUTE_CODE**  
   - `display` → **DESCRIPTION**  
   - Extract the **code type** from the `system` URI and assign it to **CODE_TYPE**.  
     -  Examples:  
     - If `system = "http://terminology.hl7.org/CodeSystem/v3-ActReason"`  
       → **CODE_TYPE = "ActReason"**  
     - If `system = "http://snomed.info/sct"`  
       → **CODE_TYPE = "SNOMED-CT"**  
     - This ensures that CODE_TYPE reflects the semantic category of the code, not the full URI.
 
6. Convert to a UTF-8 encoded CSV file. 
7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example for Snowflake
copy into s3://tuva-public-resources/terminology/immunization_status_reason.csv
from [your_table]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [your_s3_integration]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the immunization_status_reason file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [Immunization Status Reason](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__immunization_status_reason.csv)
3. Submit a pull request
