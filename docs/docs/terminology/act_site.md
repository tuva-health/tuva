---
id: act-site
title: "Act Site"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 08-21-2025</em></small>
</div>

## Data Dictionary

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm jsonPath="nodes.seed\.the_tuva_project\.terminology__act_site.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/act_site.csv_0_0_0.csv.gz">Download CSV</a>

## What is ActSite?

**ActSite** refers to the anatomical location on an organism that can be the focus of a healthcare act (e.g., swallowing, administering medication, injection). The code system is part of **HL7 v3** and used in FHIR contexts.

- **Maintained by**: HL7 International (Vocabulary Work Group)  
- **Purpose**: Defines a standardized set of anatomical site codes to support clinical documentation, administration sites logging, and interoperability in FHIR resources.  
- **Usage**: Captures anatomical sites in healthcare record systems, particularly as part of FHIR resources like MedicationAdministration or Immunization resources, using standardized codes.  

📎 [HL7 v3 Code System: ActSite (FHIR R4)](https://hl7.org/fhir/R4/v3/ActSite/cs.html) 

## Who Maintains ActSite?

- The **HL7 International Vocabulary/Terminology Work Group** is responsible for maintaining the ActSite code system.  
- The code system is published as part of HL7 v3 standards and included in FHIR releases via R4 and broader HL7 terminology services. :contentReference[oaicite:2]{index=2}

## Code Structure

Each **ActSite code**:

- Is an **alphanumeric** code (e.g., "LA" for left arm, "BE" for bilateral ears).  
- Displays as a human-readable anatomical site.  
- Has a definition summarizing the anatomical location.

Sample structure within the HL7 FHIR CodeSystem:

- **Code System URL**: `http://terminology.hl7.org/CodeSystem/v3-ActSite`  
- **Version**: 2018-08-12  
- **Name**: v3.ActSite  
- **Title**: v3 Code System ActSite  
- **Definition**: "An anatomical location on an organism which can be the focus of an act."
### Examples of codes within the system:

| Code | Display              | Definition              |
|------|----------------------|--------------------------|
| LA   | left arm             | left arm                 |
| BE   | bilateral ears       | bilateral ears           |
| BU   | buttock              | buttock                  |
| LACF | left antecubital fossa | left antecubital fossa |
| RD   | right deltoid        | right deltoid            |

These examples represent just a subset from the HumanSubstanceAdministrationSite branch of the hierarchy.

## Key Use Cases for ActSite Codes

- **Clinical Documentation**: Precisely captures anatomical sites of administration or procedures in EHRs.  
- **Interoperability**: Supports standardized data exchange in FHIR Immunization and MedicationAdministration resources.  
- **Analytics & Reporting**: Enables consistent aggregations, e.g., tracking injection site trends.  

###  Notes for Data Analysts

- ActSite codes are often paired with other coding systems for comprehensive context (e.g., route of administration codes).  
- Abstract codes should be excluded from direct data capture but are useful for conceptual hierarchies.  
- Mapping to local or national administration site codes may be needed depending on interoperability requirements.

## Tuva Seed File Update Process

This is the process for updating the terminology in Tuva’s package:

1. Navigate to the [HL7 v3 CodeSystem ActSite page](https://hl7.org/fhir/R4/v3/ActSite/cs.html).
2. Navigate to **Code System Content**
3. Download or extract the data.  
4. Save the file locally and extract the coding system elements.  
5. Ensure the following fields are retained and mapped as:  
   - `Code` → **BODY_CODE**  
   - `Display` → **DESCRIPTION**  
6. Convert to a UTF-8 encoded CSV file. 
7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example for Snowflake
copy into s3://tuva-public-resources/terminology/act_site.csv
from [your_table]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [your_s3_integration]
OVERWRITE = TRUE;
```
7. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
8. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the act_site file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [Act Site file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__act_site.csv)
3. Submit a pull request
