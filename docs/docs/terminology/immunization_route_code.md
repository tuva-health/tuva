---
id: immunization-route-code
title: "Immunization Route Code"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 08-21-2025</em></small>
</div>

## Data Dictionary

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm jsonPath="nodes.seed\.the_tuva_project\.terminology__immunization_route_code.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/immunization_route_code.csv_0_0_0.csv.gz">Download CSV</a>

## What is Immunization Route Code?

**Immunization Route Code** defines the route of administration used to deliver an immunization (e.g., intramuscular, subcutaneous, oral).  

- **Maintained by**: HL7 International (FHIR Infrastructure Work Group)  
- **Purpose**: Provides a standardized set of codes to describe administration routes in immunization records.  
- **Usage**: Applied in FHIR Immunization resources to specify how a vaccine was administered.  

📎 [HL7 FHIR ValueSet: Immunization Route Code (R4)](https://hl7.org/fhir/R4/valueset-immunization-route.html)

## Who Maintains Immunization Route Code?

- The **HL7 FHIR Infrastructure Work Group** maintains the value set.  
- Published as part of the HL7 FHIR R4 specification.  

## Code Structure

Each **Immunization Route Code**:

- Has a **Code** representing the route.  
- A **Display** string for human readability.  
- A **Definition** describing the administration method.  


## Key Use Cases

- **EHRs**: Standardizing vaccine administration routes.  
- **Public Health Reporting**: Ensures consistent interpretation across systems.  
- **Analytics**: Supports research into vaccine effectiveness by route.  

### Notes for Data Analysts

- Often paired with anatomical site codes (e.g., `ActSite`) for precise documentation.  
- Local route codes may need mapping to HL7/FHIR standard codes.  

## Tuva Seed File Update Process

This is the process for updating the terminology in Tuva’s package:

1. Navigate to the [HL7 v3 CodeSystem ImmunizationRouteCodes page](https://hl7.org/fhir/R4/valueset-immunization-route.html).
2. Navigate to **Content Logical Definition**
3. Download or extract the data.  
4. Save the file locally and extract the coding system elements.  
5. Ensure the following fields are retained and mapped as:  
   - `Code` → **ROUTE_CODE**  
   - `Display` → **DESCRIPTION**  
6. Convert to a UTF-8 encoded CSV file. 
7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example for Snowflake
copy into s3://tuva-public-resources/terminology/immunization_route_code.csv
from [your_table]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [your_s3_integration]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the immunization_route_code file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [Immunization Route Codes](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__immunization_route_code.csv)
3. Submit a pull request

