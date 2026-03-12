---
id: loinc
title: "LOINC"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__loinc.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc.csv_0_0_0.csv.gz">Download CSV</a>

## What is LOINC?

**LOINC** stands for *Logical Observation Identifiers Names and Codes*. It provides a universal code system for identifying laboratory and clinical observations, survey instruments, and documents in electronic health records.

LOINC enables consistent electronic exchange and pooling of clinical data such as lab test results or vital signs across different healthcare systems and organizations.

It helps translate diverse local coding systems into a common vocabulary, facilitating interoperability and accurate data sharing in healthcare, research, and public health.


## Tuva Seed File Update Process

Note: This is the maintenance process used by Tuva to maintain the current codeset in the Tuva package. Tuva users do not need to complete this step unless they are leveraging a different version of codes or are no longer updating to the current version of the project, but need an updated terminology set. 

1. Navigate to the [LOINC Website](https://loinc.org/downloads/)
2. Go to **Download**
3. Login if you have account otherwise complete free signup
4. Checkin the term and conditions and start download with a click in download button
5. Once the files are downloaded, extract the files
6. Pre-process the loinc data using following python script:

```python
import pandas as pd

def load_data(loinc_path, part_path):
    '''Load the required data into DataFrame.'''
    loinc_df = pd.read_csv(loinc_path, dtype=str)
    part_df = pd.read_csv(part_path, dtype=str)
    loinc_df.columns = loinc_df.columns.str.strip().str.lower()
    part_df.columns = part_df.columns.str.strip().str.lower()
    class_type_df = pd.DataFrame({
        'class_type': ['1', '2', '3', '4'],
        'class_type_description': ['Laboratory', 'Clinical', 'Claims attachments', 'Surveys'] }, dtype=str)
    return loinc_df, part_df, class_type_df

def split_part_table(part_df):
    """Split Part table into types."""
    part_types = ['property', 'component', 'time', 'system', 'scale', 'method', 'class']
    return {
        name: part_df[part_df['parttypename'] == name][['partname', 'partdisplayname']]
        for name in part_types
    }

def merge_loinc_parts(loinc_df, part_types, class_types):
    """Merge LOINC table with part tables and class types."""
    df = loinc_df.merge(class_types, how='left', left_on='classtype', right_on='class_type')
    merge_mappings = {
        'class': ('class', 'class_partname', 'class_description'),
        'component': ('component', 'component_partname', 'component_display'),
        'property': ('property', 'property_partname', 'property_display'),
        'time': ('time_aspct', 'time_aspect_partname', 'time_aspect_display'),
        'system': ('system', 'system_partname', 'system_display'),
        'scale': ('scale_typ', 'scale_type_partname', 'scale_type_display'),
        'method': ('method_typ', 'method_type_partname', 'method_type_display')
    }

    for key, (left_on_col, partname_col, display_col) in merge_mappings.items():
        df = df.merge(
            part_types[key].rename(columns={'partname': partname_col, 'partdisplayname': display_col}),
            how='left',
            left_on=left_on_col,
            right_on=partname_col
        )
    return df

def enhance_fields(df):
    """Create derived/enhanced fields."""
    df['short_name'] = df['shortname'].replace('', pd.NA).combine_first(df['long_common_name'])
    df['component'] = df['component_display'].combine_first(df['component'])
    df['property'] = df['property_display'].combine_first(df['property'])
    df['time_aspect'] = df['time_aspect_display'].combine_first(df['time_aspct'])
    df['system'] = df['system_display'].combine_first(df['system'])
    df['scale_type'] = df['scale_type_display'].combine_first(df['scale_typ'])
    df['method_type'] = df['method_type_display'].combine_first(df['method_typ'])
    return df

def finalize_and_export(df, output_path):
    """Select final columns and export to CSV."""
    final_cols = {
        'loinc_num': 'loinc',
        'short_name': 'short_name',
        'long_common_name': 'long_common_name',
        'component': 'component',
        'property': 'property',
        'time_aspect': 'time_aspect',
        'system': 'system',
        'scale_type': 'scale_type',
        'method_type': 'method_type',
        'class': 'class_code',
        'class_description': 'class_description',
        'classtype': 'class_type_code',
        'class_type_description': 'class_type_description',
        'paneltype': 'paneltype',
        'order_obs': 'order_obs',
        'example_units': 'example_units',
        'external_copyright_notice': 'external_copyright_notice',
        'status': 'status',
        'versionfirstreleased': 'version_first_released',
        'versionlastchanged': 'version_last_changed'
    }

    final_df = df[list(final_cols.keys())].rename(columns=final_cols)
    final_df.to_csv(output_path, index=False, sep=',')

if __name__ == "__main__":
    '''
    LOINC
    ├── LoincTable
    │   ├── Loinc.csv

    Loinc
    ├── AccessoryFile
    │   ├── PartFile
    │   │   ├── Part.csv
    '''
    loinc_path = '<<path to Loinc.csv file>>'
    part_path = '<<path to Part.csv file>>'
    output_path = '<<path to output file with csv file name>>'

    loinc_df, part_df, class_types = load_data(loinc_path, part_path)
    part_types = split_part_table(part_df)
    merged_df = merge_loinc_parts(loinc_df, part_types, class_types)
    enhanced_df = enhance_fields(merged_df)
    finalize_and_export(enhanced_df, output_path)
```
7. Import the CSV file into any data warehouse and upload the CSV file from the data warehouse to S3 (credentials with write permissions to the S3 bucket are required)

```sql
-- example code for Snowflake
copy into s3://tuva-public-resources/terminology/loinc.csv
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
2. Alter the headers as needed in [Loinc File](https://github.com/tuva-health/tuva/blob/main/seeds/terminology/terminology__loinc.csv)
3. Submit a pull request
