---
id: immunization-status
title: "Immunization Status"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 08-21-2025</em></small>
</div>

## Data Dictionary

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm jsonPath="nodes.seed\.the_tuva_project\.terminology__immunization_status.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_status.csv_0_0_0.csv.gz">Download CSV</a>

## What is Immunization Status?

**Immunization Status** indicates whether an immunization was successfully administered, not given, or entered in error.  

- **Maintained by**: HL7 International (FHIR Infrastructure Work Group)  
- **Purpose**: Captures the outcome status of immunization events.  
- **Usage**: Required in FHIR Immunization resources for accurate recordkeeping.  

📎 [HL7 FHIR ValueSet: Immunization Status (R4)](https://hl7.org/fhir/R4/valueset-immunization-status.html)

## Who Maintains Immunization Status?

- Maintained by the **HL7 FHIR Infrastructure Work Group**.  
- Published as part of HL7 FHIR R4.  

## Code Structure

Each **Immunization Status**:

- Has a **code** indicating outcome.  
- A **display** string.  

Example codes:

| Code  | Display         |
|-------|-----------------|
| completed | Completed   |
| entered-in-error | Entered in Error |
| not-done  | Not Done    |                   |

## Key Use Cases

- **Clinical Documentation**: Tracks vaccine administration outcomes.  
- **Public Health Surveillance**: Provides accurate vaccination status reporting.  
- **Analytics**: Differentiates between missed vaccines, completed doses, or errors.  

### Notes for Data Analysts

- Always consider pairing with **immunization status reason** for clarity when status = `not-done`.  
- `entered-in-error` should be excluded from analytic aggregations.  

## Tuva Seed File Update Process

This is the process for updating the terminology in Tuva’s package:

1. Navigate to the [HL7 v3 CodeSystem ImmunizationStatus page](https://hl7.org/fhir/R4/valueset-immunization-status.html).
2. Navigate to **Content Logical Definition**
3. Download or extract the data.  
4. Save the file locally and extract the coding system elements.  
5. Ensure the following fields are retained and mapped as:  
   - `Code` → **STATUS_CODE**  
   - `Display` → **STATUS**  
6. Convert to a UTF-8 encoded CSV file. 
7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example for Snowflake
copy into s3://tuva-public-resources/terminology/immunization_status.csv
from [your_table]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [your_s3_integration]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the immunization_status file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [Immunization Status](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__immunization_status.csv)
3. Submit a pull request

