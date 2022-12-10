[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)
## 🧰 What does this dbt package do?

To understand what this dbt package does, we must first understand what The Tuva Project is. The Tuva Project is a collection of dbt packages that builds healthcare concepts (measures, groupers, data quality tests) on top of your raw healthcare claims data. Currently, the Tuva Project consists of the following 5 dbt packages, each of which is a separate GitHub repo that does something specific:

- [data_profiling](https://github.com/tuva-health/data_profiling): Runs data quality tests to check for common problems specific to healthcare claims data.
- [claims_preprocessing](https://github.com/tuva-health/claims_preprocessing): Groups overlapping claims into a single encounter, assigns every claim to 1 of 18 different encounter types and populates core concept tables.
- [chronic_conditions](https://github.com/tuva-health/chronic_conditions): Implements a chronic condition grouper based on ICD-10-CM codes. As a result, it is possible to know whether each patient in your population has any of ~70 different chronic conditions defined for the grouper.
- [readmissions](https://github.com/tuva-health/readmissions): Calculates hospital readmission measures.
- [terminology](https://github.com/tuva-health/terminology): Makes the latest version of many useful healthcare terminology datasets available as tables in your data warehouse. This package is different from the others because it does not build healthcare concepts on top of your data.

It is possible to run any one of these packages in isolation. For example, if you are only interested in calculating readmission measures, you may run the `readmissions` package without having to run any other package. Each of the above dbt packages (except `terminology`, which does not need input data to run) has its own input layer, which consists of a set of specific tables with specific columns in them. Each package uses the raw data in its input layer as a starting point and then builds healthcare concepts with it. The input layer for each package contains the minimum necessary data elements required for the package to do what it needs to do. The description of the input layer for each package is found in the package’s README. To run any of these packages, the basic idea is the same:

1. You create the necessary input tables (the package’s input layer) as models within your dbt project so that the Tuva package of interest can reference them using ref() functions.
2. You import the Tuva package you are interested in into your dbt project and tell it where to find the relevant input tables as well as what database and schema to dump its output into.

Teams that work with healthcare claims data from multiple sources (e.g. different commercial payers, Medicare, Medicaid) typically get access to different claims datasets in different formats (different schemas with terminology that is normalized differently). When these teams want to do analytics using data from all sources, they typically map all their data to a common data model where all the data from different sources can live in a uniform format. It is helpful to use a common data model that is well-designed for doing analytics with claims data. The Tuva Project has a [Claims Common Data Model](https://www.notion.so/Claims-Preprocessing-9f511ae8edac47fa9b7a11b10971e2c2) ****(Claims CDM) that has been specifically designed for this purpose. Although each dbt package that is part of the Tuva Project may be run in isolation by mapping data to the package’s input layer, you can also run all packages by mapping your entire claims dataset to the Tuva Claims CDM. 

Organizations that have mapped all their healthcare claims data to the Tuva Claims CDM can easily run the entire Tuva Project (all 5 dbt packages) by using the `the_tuva_project` package. You can think of this package (`the_tuva_project` package) as a meta-package that is used to run all dbt packages that are part of the Tuva Project with one command. This package calls all 5 packages and runs them all while handling the dependencies between them. The input layer for `the_tuva_project` is the full Tuva Claims CDM. This means that to run the entire Tuva Project, you need to map all of your claims data into the [Tuva Claims CDM](https://thetuvaproject.com/docs/category/claims-data-model).

For a detailed overview of the methodology used in the package check out our [Knowledge Base](https://thetuvaproject.com/docs/intro).  

For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview).

## 🔌 What databases are supported?

This package has been tested on **Snowflake** and **Redshift**.

## 📚 What versions of dbt are supported?

This package requires you to have dbt installed and a functional dbt project running on dbt version `1.2.x` or higher.

## ✅ How do I use this dbt package?

Below are the steps to run this individual dbt package, which runs all packages that are part of the Tuva Project.

### Overview

As mentioned above, each dbt package that is part of the Tuva Project (except `terminology`) expects you to have data in a certain format (specific tables with specific columns in them) and uses that as an input to then build healthcare concepts. To run all packages (the entire Tuva Project), the necessary input layer is the full [Tuva Claims Common Data Model](https://thetuvaproject.com/docs/category/claims-data-model), which means that in order to run this package (`the_tuva_project` package) you must map all of your healthcare claims data into the Tuva Claims CDM.

### **Step 1:**

First, you must map all your claims data to the [Tuva Claims CDM](https://thetuvaproject.com/docs/category/claims-data-model), which consists of 3 tables. You need to create these 3 tables as models within your dbt project so that the Tuva dbt packages can reference them using ref() functions. This is typically done by writing SQL select statements within your dbt project to create 3 models (that constitute the 3 tables in the Tuva Claims CDM) containing your healthcare claims data within your dbt project.  Use the link above to view the full data dictionary for the Tuva Claims CDM.

### **Step 2:**

Once you have created the necessary 3 input tables as models within your dbt project, the next step is to import the `the_tuva_project` dbt package and tell it where to find the input tables as well as what database and schema to dump its output into. If you are only interested in running a subset of the Tuva Project, you may also tell the `the_tuva_project` package which packages to run. These things are done by editing 2 different files in your dbt project: `packages.yml` and `dbt_project.yml`. 

To import the `the_tuva_project` package, you need to include the following in your `packages.yml`:

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: 0.2.1
```

To tell the `the_tuva_project` package where to find the necessary input tables, what databases and schemas to dump its output into, and what subset of the Tuva Project to run, you must add the following in your `dbt_project.yml:`

```yaml
# These variables tell the_tuva_project package what
# packages to run. If there are some packages you 
# are not interested in running, this is where you
# need to set the relevant variable(s) to 'false':
vars:
  tuva_packages_enabled: true  
  data_profiling_enabled: true  
  claims_preprocessing_enabled: true
  chronic_conditions_enabled: true
  readmissions_enabled: true
  terminology_enabled: true

# These variables point to the 3 input tables you created 
# in your dbt project which constitutes the
# Tuva Claims CMD. 
# If you named these 3 models anything other than 'medical_claim',
# 'eligibility', 'pharmacy_claim', you must modify the
# refs here:
  medical_claim_override:   "{{ref('medical_claim')}}"
  eligibility_override: "{{ref('eligibility')}}"
  pharmacy_claim_override: "{{ref('pharmacy_claim')}}"

# This variable sets the name of the database
# where the output of the Tuva Project will be 
# dumped into. Make sure this database exists
# in your data warehouse before you run this package, 
# since dbt can create schemas in your data warehouse, 
# but it cannot create databases. Note that further 
# down you may choose separate databases for the 
# output of different dbt packages within the Tuva Project:
  tuva_database: tuva  

# If you want to add a prefix to every schema that the
# Tuva Project will write data to, set this prefix in
# this variable (it is commented out by default):
# tuva_schema_prefix: test

# If you want to write the output of any package 
# that is part of the Tuva Project to a database
# or schema different than the default names we
# suggest here, change the name here:
  data_profiling_database: tuva
  data_profiling_schema: data_profiling
  claims_preprocessing_database: tuva
  claims_preprocessing_schema: core
  chronic_conditions_database: tuva
  chronic_conditions_schema: chronic_conditions
  readmissions_database: tuva
  readmissions_schema: readmissions
  terminology_database: tuva
  terminology_schema: terminology

      

# By default, dbt prefixes schema names with the target 
# schema in your profile. We recommend including this 
# here so that dbt does not prefix the output schemas
# of the Tuva Project with anything:
dispatch:
  - macro_namespace: dbt
    search_order: [ 'the_tuva_project', 'dbt']
```

After completing the above steps you’re ready to run your project.

- Run `dbt deps` to install the package
- Run `dbt build` to run the entire project

You now have all the Tuva tables in your database and are ready to do analytics!

## 🙋🏻‍♀️ ****How is this package maintained and how do I contribute?****

The Tuva Project team maintaining this package **only** maintains the latest version of the package. We highly recommend you stay consistent with the latest version.

Have an opinion on the mappings? Notice any bugs when installing and running the package? If so, we highly encourage and welcome feedback! While we work on a formal process in Github, we can be easily reached in our Slack community.

## 🤝 Join our community!

Join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
