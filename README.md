[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)
# The Tuva Project 

## 🧰  What is the Tuva Project?
The Tuva Project code base includes a core data model, data marts, terminology sets, and data quality tests for doing healthcare analytics.

[Core Data Model](models/core)

**Data Marts:**
- [CCSR](models/ccsr)
- [Chronic Conditions](models/chronic_conditions)
- [Claims Preprocessing](models/claims_preprocessing)
- [CMS-HCCs](models/cms_hcc)
- [ED Classification](models/ed_classification)
- [Financial PMPM](models/financial_pmpm)
- [Quality Measures](models/quality_measures)
- [Readmissions](models/readmissions)

[Terminology Sets](seeds/terminology)

In many cases the actual terminology code sets are too large to maintain on GitHub, so we main them in an AWS S3 bucket.
<br/><br/>

## 🔗  Links
[Knowledge Base](https://thetuvaproject.com/) is our open source book for working with healthcare data and doing healthcare analytics and machine learning.

[Docs](https://tuva-health.github.io/the_tuva_project/) is an up to date version of dbt docs for the Tuva Project which includes data dictionaries and a DAG.
<br/><br/>

## 🔌  Supported Data Warehouses and dbt Versions
- BigQuery
- Databricks (community supported)
- Redshift
- Snowflake


This package supports dbt version `1.3.x` or higher.
<br/><br/>

## Loading Terminology via SQL

You can load all the terminology sets via SQL directly to your database.  Check out the SQL for doing this [here](terminology_sql).
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
We created the Tuva Project to be a place where healthcare data practitioners can share their knowledge about doing healthcare analytics.  If you have ideas for improvements or find bugs, we highly encourage and welcome feedback! Feel free to create an issue or ping us on Slack.

Check out our contribution guide [here](./CONTRIBUTING.md).
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data people in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
