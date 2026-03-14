---
id: hcpcs-to-rbcs
title: "HCPCS to RBCS"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__hcpcs_to_rbcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_to_rbcs.csv_0_0_0.csv.gz">Download CSV</a>


## Maintenance Instructions

1. Navigate to the [HCPCS to RBCS](https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system/data?query=%7B%22filters%22:%7B%22rootConjunction%22:%7B%22label%22:%22And%22,%22value%22:%22AND%22%7D,%22list%22:%5B%5D%7D,%22keywords%22:%22%22,%22offset%22:0,%22limit%22:10,%22sort%22:%7B%22sortBy%22:%22RBCS_Release_Year%22,%22sortOrder%22:%22ASC%22%7D,%22columns%22:%5B%5D%7D)
2. Download the csv file from the export button
3. Perform the below cleaning and transformation steps:
    - Select only the required columns:
        ```python
        required_columns = [
            "RBCS_FAMILY_DESC",
            "RBCS_FAMNUMB",
            "HCPCS_CD_END_DT",
            "RBCS_CAT",
            "RBCS_CAT_DESC",
            "RBCS_CAT_SUBCAT",
            "RBCS_MAJOR_IND",
            "RBCS_ID",
            "HCPCS_CD",
            "RBCS_SUBCAT_DESC",
            "HCPCS_CD_ADD_DT"
        ]
        ```
    - Add two new columns "RBCS_ASSIGNMENT_EFF_DT" and     "CURRENT_FLAG".

    **Note**: *You can refer to this python script for the above transformation:*
        ```python
        import pandas as pd

        input_file = 'input file path'
        output_file = 'output file path'

        df = pd.read_csv(input_file)
        columns = [col.upper() for col in df.columns]  
        print("Downloaded columns are: ", columns) 
        required_columns = [
            "RBCS_FAMILY_DESC",
            "RBCS_FAMNUMB",
            "HCPCS_CD_END_DT",
            "RBCS_CAT",
            "CURRENT_FLAG",
            "RBCS_CAT_DESC",
            "RBCS_CAT_SUBCAT",
            "RBCS_MAJOR_IND",
            "RBCS_ID",
            "RBCS_ASSIGNMENT_EFF_DT",
            "HCPCS_CD",
            "RBCS_SUBCAT_DESC",
            "HCPCS_CD_ADD_DT"
        ]
        required_columns = [col.upper() for col in required_columns]  # Convert required columns to uppercase

        new_df = pd.DataFrame()

        for col in required_columns:
            if col in columns:
                # Map the uppercase column name back to the original column name in df
                original_col = df.columns[columns.index(col)]
                new_df[col] = df[original_col]  
            else:
                new_df[col] = None  

        new_df['rbcs_assignment_eff_dt'] = None  
        new_df['current_flag'] = None  

        new_df.to_csv(output_file, index=False)
        print(new_df.head())
        ```
4. Import the CSV file into any data warehouse 
5. Upload the CSV file from the data warehouse to **S3** (credentials with write permissions to the S3 bucket are required)
```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/hcpcs_to_rbcs.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
8. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date.
9. Submit a pull request

**The below steps are only required if the headers of the file need to be changed.  The Tuva Project does not store the contents
of the hcpcs_to_rbcs file in GitHub.**


10. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
11. Alter the headers as needed in [hcpcs to rbcs](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__hcpcs_to_rbcs.csv)
12. Submit a pull request

## Usage
Use `current_flag = 1` to filter the table to the most recent categorizations.