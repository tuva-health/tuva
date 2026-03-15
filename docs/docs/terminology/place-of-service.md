---
id: place-of-service
title: "Place of Service"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 06-16-2025</em></small>
</div>

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__place_of_service.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/place_of_service.csv_0_0_0.csv.gz">Download CSV</a>

## What are Place of Service (POS) Codes?

Place of Service (POS) codes are two-digit codes reported on professional claims to indicate the specific location where healthcare services were provided. They help determine payment eligibility and reimbursement amounts based on the setting of care.

These codes are defined by the Centers for Medicare & Medicaid Services (CMS) and are used nationally in the processing of Medicare, Medicaid, and private insurance claims.

## On what kind of claims are Place of Service Codes found?

Place of Service codes are found **only on professional claims**, typically submitted via the CMS-1500 form or in electronic format using the 837P transaction. These codes are **not reported on institutional (UB-04 or 837I) claims**.

## How often are Place of Service Codes updated?

The POS code set is maintained by CMS and updated **periodically**. Updates may include the addition of new codes, deactivation of outdated ones, or modifications to descriptions. CMS typically publishes these updates on their official website and through transmittals.

## Code Structure

Each POS code is a **two-digit numeric code** (e.g., `11`, `22`, `31`) that corresponds to a particular care setting.

| Code | Description                     |
|------|---------------------------------|
| 11   | Office                          |
| 22   | On Campus-Outpatient Hospital   |
| 02   | Telehealth Provided Other than in Patient’s Home |
| 10   | Telehealth Provided in Patient’s Home |
| 21   | Inpatient Hospital              |
| 31   | Skilled Nursing Facility        |
| ...  | ... (see full CMS list)         |

Codes are typically listed alongside service lines on professional claims.

## Notes for Analysts

- **POS codes affect reimbursement**: For example, Medicare pays different rates for services delivered in a physician’s office (`POS 11`) vs. a hospital outpatient department (`POS 22`).
- **Telehealth billing**: Codes like `02` and `10` are crucial in identifying remote services, especially post-COVID.
- **Ensure POS codes align with provider specialty and NPI type** when doing audit or attribution work.
- **Claims missing or with inconsistent POS codes** may need review or cleansing depending on the analytic context.

## Key Use Cases

- **Cost comparison by site of service**: Analyze trends in spending for procedures performed in office vs. hospital vs. telehealth settings.
- **Utilization tracking**: Understand where members are receiving care most frequently.
- **Telehealth analysis**: Isolate virtual visits and compare them to in-person equivalents.
- **Quality and outcomes analysis**: Evaluate whether outcomes vary based on care setting.

## External Resources

- [CMS POS Code Set](https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set)


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [Place of Service Code Set](https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets)
2. Scroll through the page and find the **Place of Service Codes for Professional Claims section**    
3. Copy and paste the code list into any text editor or spreadsheet
4. **Clean the data:** Remove the row with the "Place of Service Name" as *“unassigned”* from the data
5. Format the codes as a CSV and save
6. Import the CSV file into any data warehouse
7. Upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/place_of_service.csv
from [table_created_in_step_6]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
8. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
9. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents of the place_of_service file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [place of service file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__place_of_service.csv)
3. Submit a pull request
