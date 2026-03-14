---
id: icd-10-pcs
title: "ICD-10-PCS"
---
<div style={{ marginTop: "-2rem", marginBottom: "1.5rem" }}>
  <small><em>Last updated: 05-21-2025</em></small>
</div>


import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_10_pcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_10_pcs.csv_0_0_0.csv.gz">Download CSV</a>

## What is ICD-10-PCS?

**ICD-10-PCS** stands for *International Classification of Diseases, 10th Revision, Procedure Coding System*. It is the U.S. procedural coding system developed by the **Centers for Medicare & Medicaid Services (CMS)** for use in **inpatient hospital settings**.

- **Maintained by**: Centers for Medicare & Medicaid Services (CMS)
- **Purpose**: Enables standardized coding of medical procedures performed in inpatient facilities
- **Usage**: Hospital billing, DRG assignment, reimbursement, clinical research, utilization review, and quality reporting

## Who Maintains ICD-10-PCS?

The **ICD-10 Coordination and Maintenance Committee** coordinates and maintains the ICD-10-PCS code sets, with final decisions on code revisions made by the **Department of Health and Human Services (HHS)**

- Updates are released bi-annually, on **April 1** and **October 1**


📎 [ICD-10-PCS Updates and Information](https://www.cms.gov/medicare/coding-billing/icd-10-codes)

📎 [ICD-10-PCS Official Guidelines for Coding and Reporting](https://www.cms.gov/files/document/2025-official-icd-10-pcs-coding-guidelines.pdf)

## Code Structure

Each ICD-10-PCS code:

- Is **alphanumeric** and **always 7 characters long**
- Each character position represents a specific axis of classification:
  1. **Section** (e.g., Medical and Surgical)
  2. **Body System** (e.g., Heart and Great Vessels)
  3. **Root Operation** (e.g., Bypass, Resection)
  4. **Body Part**
  5. **Approach**
  6. **Device**
  7. **Qualifier**

📎 [ICD-10-PCS Web-Based Training](https://www.cms.gov/Outreach-and-Education/MLN/WBT/MLN4151758-ICD-10-PCS/ICD10PCS/index.html)

### Example:

`027034Z`  
→ Dilation of coronary artery, one artery, with drug-eluting intraluminal device, percutaneous approach

## Key Use Cases for ICD-10-PCS Codes

- **Inpatient Billing & Reimbursement**: Required for hospital claims submitted to Medicare and most commercial payers
- **DRG Assignment**: Used to group hospital stays into MS-DRGs for payment
- **Utilization & Quality Analysis**: Helps track surgical procedures and outcomes
- **Clinical Research**: Enables studies based on coded interventions

📌 **Notes for Data Analysts**

- ICD-10-PCS codes are **always 7 characters** and should not contain periods
- Hierarchical classification often requires using the **Section**, **Root Operation**, and **Body System** positions

## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [CMS ICD 10 website](https://www.cms.gov/medicare/coding-billing/icd-10-codes)
2. Go to the ICD-10 Files section, click the section for ICD-10-CM & PCS files of the current fiscal year (e.g. 2025 ICD-10 CM & PCS)
3. After that go to the ICD-10-PCS files section, and click the hyperlink for "ICD-10-PCS Codes File"
4. Unzip the downloaded file and open “icd10pcs_codes_\{year}”
5. Save the "icd10pcs_codes_\{year}" as a text file
6. Load the text file into the below python script:

*Since the text file contains fixed-length fields, the following Python script can be used to convert it into a CSV file.*
    ```python
    import pandas as pd

    #This function converts the text file into a dataframe with single field 'Text'
    def text_to_dataframe(input_file):
        with open(input_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        df = pd.DataFrame({'Text': lines})
        df['Text'] = df['Text'].str.strip('\n')
        return df

    def main():
        input_filename = "your text file path"
        output_filename = "output csv file path"     
        
        df = text_to_dataframe(input_filename)

        #Slices the single column dataframe into dataframe with required columns
        df['icd_10_pcs'] = df['Text'].str.slice(0,7)
        df['description'] = df['Text'].str.slice(7,)
        print(df[['icd_10_pcs','description']].head())

        df2 = df[['icd_10_pcs','description']]
        # Remove leading/trailing whitespace from all string columns
        df2 = df2.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        
        df2.to_csv(output_filename, index=False)

    if __name__ == "__main__":
        main()
    ```
    **Note**: *You might need to adjust the slicing indexes according to the length of your data field*

7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/icd_10_pcs.csv
from [table_created_in_step_6]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
8. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
9. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents
of the ICD-10-PCS file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [ICD-10-PCS file](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__icd_10_pcs.csv)
3. Submit a pull request