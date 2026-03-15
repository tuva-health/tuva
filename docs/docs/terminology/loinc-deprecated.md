---
id: loinc-deprecated
title: "LOINC Deprecated"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__loinc_deprecated_mapping.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc_deprecated_mapping.csv_0_0_0.csv.gz">Download CSV</a>

## What is LOINC Deprecated?

**LOINC Deprecated** specifies LOINC codes which are no longer recommended for use and should be replaced with recommended alternative code. LOINC often depricates the codes due to change in clinical practice, updated terminology, or to address errors in the original codes. LOINC deprecates codes when a more appropriate or accurate code is available. The deprecated codes is not deleted but its usage is discouraged.

## TUVA Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [LOINC Website](https://loinc.org/downloads/)
2. Go to **Download**
3. Login if you have account otherwise complete free signup
4. Checkin the term and conditions and start download with a click in download button
5. Once the files are downloaded, extract the files
6. Pre-process the loinc data using following python script:

```python
import pandas as pd

# Combine current row's comment with previous comments
def combine_comments(row):
    current = f"{row['loinc']} to {row['map_to']}: {row['comment']}" if pd.notna(row['comment']) and row['comment'].strip() else ''
    if current and row['all_comments']:
        return current + '; ' + row['all_comments']
    return current or row['all_comments']

def begin_formatting(df: pd.DataFrame):
    df.columns = df.columns.str.strip().str.lower()

    # Get all target LOINC codes (i.e., "map_to" entries)
    mapped_targets = set(df['map_to'])

    # Start with LOINC codes that are not mapped from any others (root entries)
    base_df = df[~df['loinc'].isin(mapped_targets)].copy()

    # Initialize results with basic info
    base_df['final_map_to'] = base_df['map_to']
    base_df['all_comments'] = base_df.apply(lambda row: f"{row['loinc']} to {row['map_to']}: {row['comment']}" if pd.notna(row['comment']) and row['comment'].strip() else '', axis=1)
    base_df['depth'] = 0
    results = base_df.copy() 
    map_df = df.copy()

    while True:
        merged = map_df.merge(results, left_on='map_to', right_on='loinc', suffixes=('', '_parent'))
        if merged.empty:
            break
        # Carry forward the deepest mapping and increment depth
        merged['final_map_to'] = merged['final_map_to']
        merged['depth'] = merged['depth'] + 1
        # Update comments by combining with previous ones
        merged['all_comments'] = merged.apply(combine_comments, axis=1)
        new_rows = merged[['loinc', 'map_to', 'comment', 'final_map_to', 'all_comments', 'depth']]

        new_rows = new_rows[~new_rows['loinc'].isin(results['loinc'])]
        if new_rows.empty:
            break
        # Add new rows to results and sort by loinc and depth
        results = pd.concat([results, new_rows], ignore_index=True)
        results = results.sort_values(by=['loinc', 'depth'])
    return results

if __name__ == '__main__':
    '''
    LOINC
    ├── LoincTableCore
    │   ├── MapTo.csv
    '''
    source_file_path = f"path to MapTo.csv file"
    output_file_path = f"path to output file with csv filename"
    # Load the mapping CSV file
    df = pd.read_csv(source_file_path, dtype=str)
    final_df = begin_formatting(df)

    final_df.to_csv(output_file_path, index=False, encoding='utf-8', sep=',')
```

7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/loinc_deprecated_mapping.csv
from [table_created_in_step_7]
file_format = (type = csv field_optionally_enclosed_by = '"')
storage_integration = [integration_with_s3_write_permissions]
OVERWRITE = TRUE;
```
8. Create a branch in [docs](https://github.com/tuva-health/docs).  Update the `last_updated` column in the table above with the current date
9. Submit a pull request

**The below steps are only required if the headers of the file need to be changed. The Tuva Project does not store the contents
of the loinc file in GitHub.**

1. Create a branch in [The Tuva Project](https://github.com/tuva-health/tuva)
2. Alter the headers as needed in [Loinc Deprecated File](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__loinc_deprecated_mapping.csv)
3. Submit a pull request
