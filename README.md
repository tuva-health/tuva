[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=0.21.x&color=orange)

# Tuva

Check out the latest [Docs](https://tuva-health.github.io/tuva/#!/overview).  Explore the latest [DAG](https://tuva-health.github.io/tuva/#!/overview?g_v=1).

Tuva transforms your healthcare data to make it ready for machine learning and analytics.  This is done through a set of data marts that apply 
transformations to accomplish 3 things:

1. Model healthcare data into a common format
2. Test healthcare data for common data quality problems
3. Create high-level clinical concepts

## Use Cases
Tuva creates data that supports the most common healthcare analytics and machine learning use cases:

| **Use Case** | **Context** |
| --------------- | -------------------- |
| Population Analytics | n=large analytics typically used by the CMO and CFO organizations to identify ways to deliver care more efficiently and effectively (e.g. utilization, cost and outcomes) |
| Risk Stratification | Machine learning models for a clinician (e.g. nurse, care manager, physician) to identify patients that need a higher level of care |
| Patient Analytics | n=1 analytics used by patients diagnosis and treatment options (e.g. to understand new lab results, normal ranges, trends, etc.) |

## Pre-requisites
1. You have healthcare data (EHR or claims data) in a data warehouse
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Configuration
Executing the following steps will load all seed files into your data warehouse, create all models (tables/views) in your data warehouse, and run all tests on your data.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine
2. Create a database called 'tuva' in your data warehouse
    - note: this is where data from the project will be generated
    - note: if you would like data to be created in a different database you can edit this in the dbt_project.yml file
3. Create source data tables in your data warehouse
    - note: these tables should match the columns and data types in the tables in [staging](models/staging)
    - note: for more details on the required source data tables see [sources.yml](models/sources.yml)
4. Configure [dbt_project.yml](/dbt_project.yml)
    - profile: set to 'tuva' by default - change this to an active profile in the profile.yml file
    - vars: configure source database name, schema name, and table names
5. Run project
    1. Navigate to the project directory in the command line
    2. Execute "dbt build"

## Data Marts
Tuva is designed for use by a data practitioner with healthcare data (EHR or claims) in a data warehouse.  Tuva creates the following data marts in your data warehouse:

| **Data Mart** | **Description** | **Status** |
| --------------- | -------------------- | ------------------- |
| [Chronic Conditions](/models/chronic_conditions/) | Each patient is flagged for having any of 69 chronic conditions within 9 clinical areas (definitions based on CMS Chronic Condition Warehouse) | Available |
| [CCSR Categories](/models/ccsr/) | Diagnosis grouper (over 70,000 ICD-10-CM are grouped into 530 clinical categories across 21 clinical domains) | Available |
| Readmissions | All 7 CMS readmission measures, LACE index, and pre-processed tables ready to train ML readmission models. | Planned: Nov 2021 |
| CMS and HHS HCCs | Condition categories, hierarchies, and risk scores at the patient-level. | Planned Release: Dec 2021 |

## Contributions
Don't see a model or specific metric you would have liked to be included? Notice any bugs when installing 
and running the package? If so, we highly encourage and welcome contributions to this package! 
Please create issues or open PRs against `master`. See [the Discourse post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) for information on how to contribute to a package.

## Database Support
This package has been tested on Snowflake.  We are planning to expand testing to BigQuery and Redshift in the near future.
