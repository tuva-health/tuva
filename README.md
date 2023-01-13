[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)
# The Tuva Project

## 🔗  Quick Links
- [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview): Learn about the Tuva Project data model
- [Knowledge Base](https://thetuvaproject.com/docs/intro): Learn about claims data fundamentals and how to do claims data analytics
<br/><br/>

## 🧰  What is the Tuva Project?

The Tuva Project is a collection of dbt packages that clean and transform healthcare claims data so that it's ready for analytics. Currently, the Tuva Project consists of the following 7 dbt packages, each of which is a separate GitHub repository.  This repository is the main dbt package you use to run any one or all of the packages below:

- [data_profiling](https://github.com/tuva-health/data_profiling): Runs data quality tests to check for common problems specific to healthcare claims data.
- [claims_preprocessing](https://github.com/tuva-health/claims_preprocessing): Groups overlapping claims into a single encounter, assigns every claim to 1 of 15 different encounter types and populates core data tables.
- [cms_chronic_conditions](https://github.com/tuva-health/chronic_conditions): Implements a chronic condition grouper based on ICD-10-CM codes. As a result, it is possible to know whether each patient in your population has any of ~70 different chronic conditions defined for the grouper.
- [tuva_chronic_conditions](https://github.com/tuva-health/tuva_chronic_conditions): implements a chronic condition grouper created by the Tuva Project which creates ~40 homogeneous and mutually exclusive chronic condition groups on your patient.
- [pmpm](https://github.com/tuva-health/pmpm): Calculates spend and utilization metrics for your patient population on a per-member-per-month (pmpm) basis.
- [readmissions](https://github.com/tuva-health/readmissions): Calculates hospital readmission measures.
- [terminology](https://github.com/tuva-health/terminology): Makes the latest version of many useful healthcare terminology datasets available as tables in your data warehouse. This package is different from the others because it does not build healthcare concepts on top of your data.
<br/><br/>

## 🔌  Supported Databases and dbt Versions

This package has been tested on: 
- Snowflake
- Redshift
- BigQuery

This package supports dbt version `1.2.x` or higher.
<br/><br/>

## ✅  Quick Start Instructions

### Step 1: Map Your Claims Data to the Tuva Claims Data Model

In step 1 you need to map your claims data to the Tuva Claims Data Model.  You need to create each of these tables as models within your dbt project so that the Tuva Project dbt package can reference them using ref() functions.  The Tuva Claims Data Model consists of 3 tables: 
- [medical_claim](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.medical_claim)
- [pharmacy_claim](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.pharmacy_claim)
- [eligibility](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.eligibility)
<br/><br/>

### Step 2: Import the Tuva Project package into Your dbt Project

In step 2 you need to import the `the_tuva_project` dbt package.  To import the `the_tuva_project` package, you need to include the yaml below in your `packages.yml` file.  Once you've done this you can run `dbt deps`.  Check the latest release of the Tuva Project in GitHub to know the latest version number to use (i.e. the latest version won't always be 0.2.4 as shown in the yaml below).

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: 0.2.4
```


### Step 3: Configure dbt Variables

The easiest way to accomplish the steps in this section is by copying and pasting the yaml code below into your `dbt_project.yml` file and then changing any of the preset configurations from the yaml below as needed.  To configure the dbt variables for the project you need to complete the following steps:

1. Configure the `Package Enabled Variables`.  These variables tell the Tuva Project which packages should be turned on or off.  This is the first set of variables shown in the yaml below.
2. Configure the target database, i.e. the database where dbt will write the output from the Tuva Project.  This variable is called `tuva_database` in the yaml below.  Note that you must create this database in your data warehouse before running the Tuva Project.  
3. We also recommend adding the `dispatch` configuration at the end of the yaml below to ensure your schema names are not prefixed with the target schema name from your dbt `profile.yml`.

The Tuva Project already knows where your source data is located, because it references the models you created in step 1 via ref() statements, so no additional configuration of source data location is needed before running the Tuva Project.

```yaml
vars:

## Package Enabled Variables:
## These variables tell the Tuva Project which packages you want
## to enable.  To enable a package set it to true, to disable a 
## package set it to false.
  claims_preprocessing_enabled: true
  cms_chronic_conditions_enabled: true
  data_profiling_enabled: true 
  pmpm_enabled: true
  readmissions_enabled: true
  terminology_enabled: true
  tuva_chronic_conditions_enabled: true


## Target Database Variable:
## This variable tells the Tuva Project where to write the 
## output data to.  You must create this database in your
## data warehouse before running the Tuva Project.
  tuva_database: tuva  


## Optional Configuration Variables:
## If you named the 3 tables in the Tuva Claims Data Model
## something other than the default names (i.e. medical_claim,
## pharmacy_claim, and eligibility), you can edit the names
## here.
  # medical_claim_override:   "{{ref('medical_claim')}}"
  # eligibility_override: "{{ref('eligibility')}}"
  # pharmacy_claim_override: "{{ref('pharmacy_claim')}}"

## If you want to add a prefix to every schema that the
## Tuva Project will write data to, set this prefix in
## this variable (it is commented out by default):
  # tuva_schema_prefix: test

## Use these variables to write the output of any specific 
## package to a specific database and schema:
  # claims_preprocessing_database: tuva
  # claims_preprocessing_schema: core
  # cms_chronic_conditions_database: tuva
  # cms_chronic_conditions_schema: cms_chronic_conditions
  # data_profiling_database: tuva
  # data_profiling_schema: data_profiling
  # pmpm_database: tuva
  # pmpm_schema: pmpm
  # readmissions_database: tuva
  # readmissions_schema: readmissions
  # terminology_database: tuva
  # terminology_schema: terminology
  # tuva_chronic_conditions_database: tuva
  # tuva_chronic_conditions_schema: tuva_chronic_conditions


## By default, dbt prefixes schema names with the target 
## schema in your profile. Including the dispatch variable
## will fix this.
dispatch:
  - macro_namespace: dbt
    search_order: [ 'the_tuva_project', 'dbt']
```

After completing the above steps you’re ready to run your project.  `cd` into your root dbt project directory and execute `dbt build` to run the entire project.  You now have all the Tuva tables in your database and are ready to do analytics!
<br/><br/>

## 🙋🏻‍♀️ How is this package maintained and how do I contribute?

The Tuva Project team maintaining this package **only** maintains the latest version of the package. We highly recommend you stay consistent with the latest version.

Have an opinion on the mappings? Notice any bugs when installing and running the package? If so, we highly encourage and welcome feedback! While we work on a formal process in Github, we can be easily reached in our Slack community.
<br/><br/>

## 🤝 Join our community!

Join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
