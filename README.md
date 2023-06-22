[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)
# The Tuva Project 

## 🧰  What is the Tuva Project?
The Tuva Project is a collection of data marts and terminology code sets that transform healthcare data for analytics.

**Data Marts:**
- [Acute Inpatient](models/acute_inpatient)
- [CCSR](models/ccsr)
- [Chronic Conditions](models/chronic_conditions)
- [CMS-HCCs](models/cms_hcc)
- [Core](models/core)
- [Data Profiling](models/data_profiling)
- [Member Months](models/member_months)
- [PMPM](models/pmpm)
- [Readmissions](models/readmissions)
- [Service Category](models/service_category)

You can find all the terminology sets **[here](seeds/terminology)**.  In many cases the actual terminology code sets are too large to maintain on GitHub, so we main them in an AWS S3 bucket.  However when you run the Tuva Project they are loaded into data warehouse just as they would be as if they were seed files.
<br/><br/>

## 🔗  Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the Tuva Project and how to use it with your healthcare data.
<br/><br/>

## 🔌  Supported Data Warehouses and dbt Versions
- Snowflake
- Redshift
- BigQuery

This package supports dbt version `1.3.x` or higher.
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
We created the Tuva Project to be a place where healthcare data practitioners can share their knowledge about doing healthcare analytics.  If you have ideas for improvements or find bugs, we highly encourage and welcome feedback! Feel free to create an issue or ping us on Slack.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
