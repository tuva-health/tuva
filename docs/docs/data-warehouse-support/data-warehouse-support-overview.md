---
id: data-warehouse-support-overview
title: "Supported Data Warehouses"
hide_title: false
---

Tuva officially supports the following data warehouses:

- Snowflake
- Databricks
- Google Bigquery
- Amazon Redshift
- Microsoft Fabric
- Databricks

Official support means we have testing of these data warehouses built into our CI/CD pipelines, so every change we commit to the project is tested on these data warehouses for SQL syntax issues, dbt-related issues, etc.

The Tuva Community unofficially supports several other data warehouses.  This means Tuva will run on these data warehouses, but we don't yet have them added to our CI/CD pipelines for automated testing.  These include:

- Postgres
- AWS Athena
- Microsoft Azure Synapse

If you're using an unofficially supported data warehouse and have a problem, submit an issue.