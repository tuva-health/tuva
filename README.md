[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)

![diagram](./the_tuva_project.jpg)

## 🧰  What is the Tuva Project?
The Tuva Project code base includes a core data model, data marts, terminology 
sets, and data quality tests for doing healthcare analytics.

**Explore the project:**

- [Code](./models)
- [Terminology Sets](seeds/terminology)*
- [Value Sets](seeds/value_sets)
- [Data Dictionaries / DAG](https://tuva-health.github.io/tuva/#!/overview/)
- [Knowledge Base](https://thetuvaproject.com/)

*Note: In many cases the actual terminology sets are too large to maintain on 
GitHub, so we host them in a public AWS S3 bucket. Executing dbt build will 
load the terminology sets from S3.

Check out our Quickstart guide [here](./QUICKSTART.md).
<br/><br/>

## 🔌  Supported Data Warehouses and dbt Versions
- BigQuery
- Databricks (community supported)
- DuckDB (community supported)
- Redshift
- Snowflake


This package supports dbt version `1.3.x` or higher.
<br/><br/>

## 🙋🏻‍♀️ Contributing
We created the Tuva Project to be a place where healthcare data practitioners 
can share their knowledge about doing healthcare analytics.  If you have ideas 
for improvements or find bugs, we highly encourage and welcome feedback! Feel 
free to create an issue or ping us on Slack. 

Check out our Contribution guide [here](./CONTRIBUTING.md).
<br/><br/>

## 🤝 Community
Join our growing community of healthcare data people in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
<br/><br/>
