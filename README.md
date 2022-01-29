[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=0.21.x&color=orange)

# Tuva

Check out the Tuva [DAG](https://tuva-health.github.io/tuva/#!/overview?g_v=1)
View the Tuva [Data Models](https://docs.google.com/spreadsheets/d/1X69EX9L27oSlopV18oLk86QCJG_-Q4XqP8xvzlIAE84/edit?usp=sharing)

Tuva cleans and transforms your messy healthcare data.  It does 3 things:

1. Tests data for healthcare-specific data quality issues
2. Creates high-level healthcare concepts on top of raw data
3. Creates data marts designed to answer key healthcare questions

## Pre-requisites
1. You have healthcare data (e.g. EHR, claims, lab, HIE, etc.) in a data warehouse
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Configuration
Execute the following steps to load all seed files, build all data marts, and run all data quality tests in your data warehouse:

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine
2. Create a database called 'tuva' in your data warehouse
    - note: this is where data from the project will be generated
3. Create source data tables in your data warehouse
    - note: these tables must match table names and column names exactly as in [source.yml](models/source.yml)
4. Configure [dbt_project.yml](/dbt_project.yml)
    - profile: set to 'tuva' by default - change this to an active profile in the profile.yml file that connects to your data warehouse
    - vars: configure source_name, source database name, and source schema name
5. Run project
    1. Navigate to the project directory in the command line
    2. Execute "dbt build" to create all tables/views in your data warehouse

## Contributions
Don't see a model or specific metric you would have liked to be included? Notice any bugs when installing 
and running the package? If so, we highly encourage and welcome contributions to this package! 
Please create issues or open PRs against `master`. See [the Discourse post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) for information on how to contribute to a package.

## Database Support
This package has been tested on Snowflake.  We are planning to expand testing to BigQuery and Redshift in the near future.
