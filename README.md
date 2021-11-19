[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=0.21.x&color=orange)

# Tuva

Check out the latest [DOCS](https://tuva-health.github.io/tuva/#!/overview).  Explore the latest [DAG](https://tuva-health.github.io/tuva/#!/overview?g_v=1).

Tuva transforms your healthcare data so that it's ready for machine learning and analytics.  In particular it does three things:

1. Models healthcare data into a common format
2. Tests healthcare data for common data quality problems
3. Creates high-level clinical concepts (e.g. which patients have type 2 diabetes)

## Use Cases
Tuva creates data that supports the most common healthcare analytics and machine learning use cases:

| **use case** | **user** | **context** |
| --------------- | -------------------- | ------------------------- |
| Population Analytics (e.g. spend, utilization, outcomes) | Healthcare administrator (e.g. chief medical officer, chief financial officer, etc.) | n=large analysis to identify sub-populations of patients where care can be delivered at lower cost and higher quality |
| Risk Stratification (e.g. ML model to predict patients likely to be readmitted) | Clinician (e.g. nurse, care manager, physician) | n=1 analysis to determine which patients should receive a higher level of care |
| Patient Analytics (e.g. patient portal analytics) | Patient | n=1 analysis to review recent lab work and understand my trends |

## Pre-requisites
1. You have healthcare data (EHR or claims data) in a data warehouse
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Configuration
Executing the following steps will load all seed files into your data warehouse, create all models (tables/views) in your data warehouse, and run all tests on your data.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine
3. Create a database called 'tuva' in your data warehouse
    - note: this is where data from the project will be generated
    - note: if you would like data to be created in a different database you can edit this in the dbt_project.yml file
4. Create source data tables in your data warehouse
    - note: these tables should match the columns and data types in the tables in [staging](models/staging)
    - note: for more details on the required source data tables see [sources.yml](models/sources.yml)
2. Configure [dbt_project.yml](/dbt_project.yml)
    - profile: 'tuva' by default - change this to an active profile in the profile.yml file
    - source_database: 'hcup' by default - change this to the database where you created the source data tables
    - source_schema: 'public' by default - change this to the schema where you created the source data tables
4. Run project
    1. Navigate to the project directory in the command line
    2. Execute "dbt build"

## Modules
Tuva is designed for use by a data practitioner with healthcare data (EHR or claims) in a data warehouse.  The following modules are either currently available or under development:

| **modules** | **description** | **status** |
| --------------- | -------------------- | ------------------- |
| [chronic_conditions](#chronic-conditions) | Each patient is flagged for having any of 69 chronic conditions within 9 clinical areas (definitions based on CMS Chronic Condition Warehouse). | Available |
| clinical_classification_software | Diagnosis grouper (over 70,000 ICD-10-CM are grouped into 530 clinical categories across 21 clinical domains) and procedure grouper (over 80,000 ICD-10-PCS codes are grouped into 320 procedure categories across 31 clinical domains). | Planned Release: Nov 2021 |
| readmissions | All 7 CMS readmission measures, LACE index, and pre-processed tables ready to train ML readmission models. | Planned: Nov 2021 |
| cms_and_hhs_hccs | Condition categories, hierarchies, and risk scores at the patient-level. | Planned Release: Dec 2021 |

#### Chronic Conditions
For several types of analyses (e.g. utilization, spend, outcomes, risk-adjustment, etc.) it's necessary to know if a patient has been diagnosed with any chronic conditions.  The models in this part of the project create 69 chronic conditions flags at the patient-level (i.e. one record per patient).  A 'long' version of the table includes metrics related to each condition for each patient, such as date of onset, most recent diagnosis date, and total number of encounters with the chronic condition.

| **model** | **description** |
| --------------- | -------------------- |
| [condition_logic_simple](models/chronic_conditions/condition_logic_simple.sql) | Joins diagnosis and procedure codes from stg_diagnoses and stg_procedures to the proper codes in [chronic_conditions](data/chronic_conditions.csv). |
| [condition_logic](models/chronic_conditions/condition_logic.sql) | Joins diagnosis and procedure codes from stg_diagnoses and stg_procedures to the proper codes in [chronic_conditions](data/chronic_conditions.csv).  Conditions identified using this logic require additional criteria (e.g. only consider primary diagnosis). |
| [stroke_transient_ischemic_attack](models/chronic_conditions/stroke_transient_ischemic_attack.sql) | This logic specifically identifies patients who have experienced a stroke or TIA (mini-stroke) by joining diagnosis codes to [chronic_conditions](data/chronic_conditions.csv). |
| [benign_prostatic_hyperplasia](models/chronic_conditions/benign_prostatic_hyperplasia.sql) | This logic specifically identifies patients who have experience benign prostatic hyperplasia (also known as prostate gland enlargement) by joining to diagnosis codes in [chronic_conditions](data/chronic_conditions.csv). |
| [union_calculations](models/chronic_conditions/union_calculations.sql) | Unions the four condition logic models together and calculates measures (i.e. date of onset, most recent diagnosis date, and number of distinct encounters with the diagnosis). |
| [condition_pivot](models/chronic_conditions/condition_pivot.sql) | Pivots union_calculations to create a 'wide' table, i.e. one record per patient with 69 columns, one for each chronic condition (the values of these columns are either 1 if the patient has the condition or 0 otherwise). |

## Contributions
Don't see a model or specific metric you would have liked to be included? Notice any bugs when installing 
and running the package? If so, we highly encourage and welcome contributions to this package! 
Please create issues or open PRs against `master`. See [the Discourse post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) for information on how to contribute to a package.

## Database Support
This package has been tested on Snowflake.  We are planning to expand testing to BigQuery and Redshift in the near future.
