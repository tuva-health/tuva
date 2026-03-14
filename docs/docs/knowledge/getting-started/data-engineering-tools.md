---
id: data-engineering-tools
title: "Data Engineering Tools"
---

The modern data engineering stack includes a number of tools that are essential for building and operating a data platform. In this section, we provide a brief overview of some of the most important ones—what they’re used for and resources to help you get started quickly.

## dbt

[dbt](https://docs.getdbt.com/docs/get-started-dbt) is an open-source data transformation tool that makes it easy to build data marts in a version-controlled, well-documented way. 

Before dbt, teams often wrote huge `.sql` files with thousands of lines of code—or scattered logic across multiple files. That setup was messy, error-prone, and hard to run (which file do you even execute first?), and files were rarely version-controlled, making team collaboration difficult to impossible.

With dbt, you can break code into smaller, modular pieces, and it automatically figures out the correct execution order, and every dbt project is a git repo. It has quickly become one of the most important tools in data engineering today, with more than 10,000 GitHub stars.

## Git

Git is essential to modern data engineering. Without version control, it’s nearly impossible to develop collaboratively as a team. While Git can feel tricky at first, it’s worth the investment—nearly every engineering team relies on it.  If your team is not using git, you will struggle to build a scalable data platform.

[Intro to Git](https://product.hubspot.com/blog/git-and-github-tutorial-for-beginners)

## Cloud Data Warehouses

Tuva is designed to run inside a cloud data warehouse. While it can also run in other databases (e.g. DuckDB, Postgres), most enterprises use one of the five major cloud data warehouses listed below:

- [Snowflake](https://docs.snowflake.com/en/learn-tutorials)  
- [Databricks](https://docs.databricks.com/aws/en/getting-started/)  
- [Google BigQuery](https://cloud.google.com/bigquery/docs/quickstarts)  
- [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/get-started/)  
- [Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html)  

Each warehouse has its own nuances from both a DBA (database administrator), data engineer, and data analyst perspective. It’s important to spend time learning the details of the one you’ll be working with. The linked tutorials are a good place to start.

## Workflow Orchestration

While dbt is often enough for orchestrating transformations inside a data warehouse, once pipelines start moving data between systems—into cloud storage, out to machine learning models, and back again—you’ll need a dedicated workflow orchestration tool.  

A common Tuva pipeline might look like this:

1. Raw data is staged in cloud storage (e.g. AWS S3).  
2. Raw data is loaded into the data warehouse with minimal transformation.  
3. Data is mapped to the Tuva Input Layer.  
4. Patient data from the Input Layer is sent to the EMPI for patient matching.  
5. Master IDs from EMPI are loaded back into the warehouse.  
6. dbt runs to build all Tuva pipelines and the final data model.  
7. Data is exported for machine learning in Python.  
8. Predictions are loaded back into the warehouse.  
9. The semantic layer is processed to prepare data for analytics.  

At Tuva, we use [Prefect](https://docs.prefect.io/v3/get-started/quickstart#open-source), which emphasizes Python-first workflows and simple cloud/local deployment. Other popular tools include:

- **Airflow** – The most widely used orchestrator, backed by the Apache community. Great for complex DAGs and enterprise adoption, but can feel heavyweight for smaller teams.  
- **Dagster** – Strong focus on data quality and observability, with “software-defined assets” to make pipelines more testable and maintainable.  
- **Luigi** – An older tool from Spotify, still useful for lightweight batch workflows but less common in modern stacks.  