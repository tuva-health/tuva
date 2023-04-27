[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)
# The Tuva Project

## 🔗  Quick Links
- [Knowledge Base](https://thetuvaproject.com/): Learn about claims data fundamentals and how to do claims data analytics
- [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview): Learn about the Tuva Project data model

<br/><br/>

## 🧰  What is the Tuva Project?

The Tuva Project a package that clean and transform healthcare claims data so that it's ready for analytics. Currently, the Tuva Project consists of the following 7 dbt packages, each of which is a separate GitHub repository.  This repository is the main dbt package you use to run any one or all of the packages below:

- [data_profiling](https://thetuvaproject.com/data-marts/data-profiling/about): Runs data quality tests to check for common problems specific to healthcare claims data.
- [claims_preprocessing](https://thetuvaproject.com/data-marts/claims-preprocessing/about): Groups overlapping claims into a single encounter, assigns every claim to 1 of 15 different encounter types and populates core data tables.
- [chronic_conditions](https://thetuvaproject.com/data-marts/chronic-conditions/about): Two different chronic condition groupers based on ICD-10-CM codes, one using grouping methodology defined by CMS, and another developed by Tuva. 
- [pmpm](https://thetuvaproject.com/data-marts/pmpm/about): Calculates spend and utilization metrics for your patient population on a per-member-per-month (pmpm) basis.
- [readmissions](https://thetuvaproject.com/data-marts/readmissions/about): Calculates hospital readmission measures based on CMS methodology.
- [terminology](https://thetuvaproject.com/terminology/about): Makes the latest version of many useful healthcare terminology datasets available as tables in your data warehouse. This package is different from the others because it does not build healthcare concepts on top of your data.
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

See our [Quickstart Guide](https://thetuvaproject.com/quickstart) for detailed instructions.

The first step is mapping your claims data to the Tuva Claims Data Model.  You can map your claims data to the Tuva Claims Data Model yourself (i.e. by writing SQL inside your dbt project).  Or if you have Medicare CCLF or Medicare SAF (LDS) claims data you can use our connectors, which are separate repos that you can find on our GitHub page.  You need to create each of the tables in the Tuva Claims Data Model as models within your dbt project so that the Tuva Project dbt package can reference them using ref() functions.

The Tuva Claims Data Model consists of 3 tables: 
- [medical_claim](https://thetuvaproject.com/data-marts/input-layer/data-dictionary/eligibility)
- [pharmacy_claim](https://thetuvaproject.com/data-marts/input-layer/data-dictionary/medical-claim)
- [eligibility](https://thetuvaproject.com/data-marts/input-layer/data-dictionary/pharmacy-claim)
<br/><br/>

These three models should named `medical_claim`, `pharmacy_claim`, and `eligibility` respectively in your project

### Step 2: Import the Tuva Project package into Your dbt Project

In step 2 you need to import the `the_tuva_project` dbt package.  To import the `the_tuva_project` package, you need to include the yaml below in your `packages.yml` file.  Once you've done this you can run `dbt deps`.  Check the latest release of the Tuva Project in GitHub to know the latest version number to use (i.e. the latest version won't always be 0.2.4 as shown in the yaml below).

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: 0.3.0
```


### Step 3: Configure dbt 

The Tuva Project will write to the database used in the profile of your project. 

Optionally, to override dbt's default schema naming behavior and to write to schemas defined by the mart name without a prefix defined in your profile, add the following code to your dbt_project.yml  

```yaml
vars:
dispatch:
  - macro_namespace: dbt
    search_order: [ 'the_tuva_project', 'dbt']
```

### Step 4: Run the Project

After completing the above steps you’re ready to run your project.  Execute `dbt build` while in the root folder  run the entire project.  You now have all the Tuva tables in your database and are ready to do analytics!
<br/><br/>

## 🙋🏻‍♀️ How is this package maintained and how do I contribute?

The Tuva Project team maintaining this package **only** maintains the latest version of the package. We highly recommend you stay consistent with the latest version.

Have an opinion on the mappings? Notice any bugs when installing and running the package? If so, we highly encourage and welcome feedback! While we work on a formal process in Github, we can be easily reached in our Slack community.
<br/><br/>

## 🤝 Join our community!

Join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
