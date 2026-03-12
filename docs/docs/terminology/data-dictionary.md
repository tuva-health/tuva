---
id: data-dictionary
title: "Data Dictionary"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';
import { JsonDataTableNoTerm } from '@site/src/components/JsonDataTableNoTerm';

## Admit Source

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__admit_source.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_source.csv_0_0_0.csv.gz">Download CSV</a>

## Admit Type

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__admit_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/admit_type.csv_0_0_0.csv.gz">Download CSV</a>

## ANSI FIPS State

[FIPS state codes](https://www.census.gov/library/reference/code-lists/ansi.html) assigned by ANSI and used by the US Census Bureau.

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.reference_data__ansi_fips_state.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ansi_fips_state.csv_0_0_0.csv.gz">Download CSV</a>

## APR-DRG

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__apr_drg.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/apr_drg.csv_0_0_0.csv.gz">Download CSV</a>

## Bill Type 

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.terminology__bill_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/bill_type.csv_0_0_0.csv.gz">Download CSV</a>

## Calendar

The calendar table contains every calendar day from 1/1/1900 through 1/12/2119, along with other helpful metadata like day of week and first/last day of month

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.reference_data__calendar.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/calendar.csv_0_0_0.csv.gz">Download CSV</a>

## Census Shape Files

The U.S. Census provides geographic shape files for different grains of census areas.  We use three primary areas: County, Tract and Block Group.  You can find the original file downloads from the U.S. Census [here](https://www.census.gov/cgi-bin/geo/shapefiles/index.php).

For census tract and block groups, the U.S. Census provides shapefiles on a state by state basis.  We have preprocessed these state files to create single shape files for the entire U.S.  You can find the County, Tract and Block Group shapefiles in our S3 reference bucket here:
- [County](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-counties.zip)
- [Tract](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-tracts.zip)
- [Block Group](https://tuva-public-resources.s3.amazonaws.com/reference-data/2022+Census+Shapefiles/us-census-block-groups.zip)

The full documentation of data fields can be found [here](https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2023/TGRSHP2023_TechDoc.pdf).  Here is a brief description of the fields taken from the above full documentation: 

**Block Groups:**

| Field     | Length | Type   | Description                                                                                     |
|-----------|--------|--------|-------------------------------------------------------------------------------------------------|
| STATEFP   | 2      | String | Current state FIPS code                                                                         |
| COUNTYFP  | 3      | String | Current county FIPS code                                                                        |
| TRACTCE   | 6      | String | Current census tract code                                                                       |
| BLKGRPCE  | 1      | String | Current block group number                                                                      |
| GEOID     | 12     | String | Census block group identifier; a concatenation of the current state FIPS code, county FIPS code, census tract code, and block group number. |
| NAMELSAD  | 13     | String | Current translated legal/statistical area description and the block group number                |
| MTFCC     | 5      | String | MAF/TIGER Feature Class Code (G5030)                                                            |
| FUNCSTAT  | 1      | String | Current functional status                                                                       |
| ALAND     | 14     | Number | Current land area                                                                               |
| AWATER    | 14     | Number | Current water area                                                                              |
| INTPTLAT  | 11     | String | Current latitude of the internal point                                                          |
| INTPTLON  | 12     | String | Current longitude of the internal point                                                         |

**Census Tracts:**

| Field     | Length | Type   | Description                                                                                     |
|-----------|--------|--------|-------------------------------------------------------------------------------------------------|
| STATEFP   | 2      | String | Current state FIPS code                                                                         |
| COUNTYFP  | 3      | String | Current county FIPS code                                                                        |
| TRACTCE   | 6      | String | Current census tract code                                                                       |
| GEOID     | 12     | String | Census block group identifier; a concatenation of the current state FIPS code, county FIPS code, census tract code, and block group number. |
| NAMELSAD  | 13     | String | Current translated legal/statistical area description and the block group number                |
| MTFCC     | 5      | String | MAF/TIGER Feature Class Code (G5030)                                                            |
| FUNCSTAT  | 1      | String | Current functional status                                                                       |
| ALAND     | 14     | Number | Current land area                                                                               |
| AWATER    | 14     | Number | Current water area                                                                              |
| INTPTLAT  | 11     | String | Current latitude of the internal point                                                          |
| INTPTLON  | 12     | String | Current longitude of the internal point                                                         |

## Claim Type

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__claim_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/claim_type.csv_0_0_0.csv.gz">Download CSV</a>

## Code Type

A list of all standardized code type names used in Tuva.  Input layer tables should use these values for source_code_type or normalized_code_type in order for codes to be recognized in marts.

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.reference_data__code_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/code_type.csv_0_0_0.csv.gz">Download CSV</a>

## Discharge Disposition

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__discharge_disposition.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/discharge_disposition.csv_0_0_0.csv.gz">Download CSV</a>

## Encounter Type

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__encounter_type.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/encounter_type.csv_0_0_0.csv.gz">Download CSV</a>

## Ethnicity

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__ethnicity.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/ethnicity.csv_0_0_0.csv.gz">Download CSV</a>

## FIPS County

Here's a brief explanation of how FIPS works:
- Each State has a unique two digit FIPS code. For example, California is 06 and New York is 36.
- Each county has a unique five digit FIPS code. The first two digits are the state FIPS code and the last three are
the county. 
- The next level we care about is the census tract. Each tract has a unique 11 digit FIPS code. The first two digits
are the state FIPS code, the next three are the county FIPS code and the last six are the tract FIPS code.
- Finally, we have the census block group. Each block group has a unique 12 digit FIPS code. The first two digits
are the state FIPS code, the next three are the county FIPS code, the next six are the tract FIPS code and the last
digit is the block group FIPS code.

<JsonDataTable  jsonPath="nodes.seed\.the_tuva_project\.reference_data__fips_county.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/fips_county.csv_0_0_0.csv.gz">Download CSV</a>

## Gender

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__gender.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/gender.csv_0_0_0.csv.gz">Download CSV</a>

## HCPCS Level 2

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__hcpcs_level_2.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_level_2.csv_0_0_0.csv.gz">Download CSV</a>

## HCPCS to RBCS

Use `current_flag = 1` to filter the table to the most recent categorizations.

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__hcpcs_to_rbcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/hcpcs_to_rbcs.csv_0_0_0.csv.gz">Download CSV</a>

## ICD-9-CM

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_9_cm.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_cm.csv_0_0_0.csv.gz">Download CSV</a>

## ICD-9-PCS

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_9_pcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_9_pcs.csv_0_0_0.csv.gz">Download CSV</a>

## ICD-10-CM

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_10_cm.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_10_cm.csv_0_0_0.csv.gz">Download CSV</a>

## ICD-10-PCS

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__icd_10_pcs.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/icd_10_pcs.csv_0_0_0.csv.gz">Download CSV</a>

## LOINC

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__loinc.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc.csv_0_0_0.csv.gz">Download CSV</a>

## LOINC Deprecated

<JsonDataTableNoTerm  jsonPath="nodes.seed\.the_tuva_project\.terminology__loinc_deprecated_mapping.columns" />

<a href="https://tuva-public-resources.s3.amazonaws.com/versioned_terminology/latest/loinc_deprecated_mapping.csv_0_0_0.csv.gz">Download CSV</a>

## MDC

