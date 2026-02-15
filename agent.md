# The Tuva Project — AI Agent Guide

The Tuva Project is an open-source healthcare data model and analytics framework built using dbt.

Its purpose is to transform raw healthcare data (claims, clinical, eligibility, pharmacy)
into a standardized data model that supports analytics such as:

- Risk adjustment (CMS-HCC)
- Quality measures (HEDIS)
- Cost and utilization analytics
- Clinical analytics
- Population health analytics

The Tuva Project runs entirely in SQL using dbt.

# Architecture

The Tuva Project follows this layered architecture:

## Input Layer

The Input Layer is the data model raw claims and clinical datasets must conform to before they can be processed by the Tuva package.  Users map (i.e. write SQL that transforms their raw data) their raw data to the Input Layer.  The Tuva package refs the Input Layer tables.

See tuva/models/input_layer for the exact specification of these models.  The user defines these models in their own dbt project.  Then when they import Tuva as a package into their dbt project it's the tuva/models/input_layer that refs the models they created inside their project.

## Claims Preprocessing

Runs immediately after the Input Layer, normalizing claims, running service categories and encounter groupers.

See tuva/models/claims_preprocessing for all the models related to claims_preprocessing.

## Core Data Model

The Core Data Model includes cleaned claims and clinical tables.  It's important that the Core Data Model stays relatively stable over time because users are using it as their common data model in their organization.  Doesn't mean it can't change; but changes must be thoughtfully implemented and communicated.

See tuva/models/core for all the models related to the Core Data Model.

## Data Marts

Advanced data marts for measures, groupers, and risk models.

These are all stored in their own folders in tuva/models.

## Terminology & Value Sets

Terminology and value sets (and reference data) are stored in AWS S3 and loaded via post hooks.  

See seeds/ for definitions of all these files (i.e. headers) althought the actual data is stored in S3 (this speeds up loading into data warehouses).

# Development

Development should occur from within the integration_tests folder.

The best workflow for development occurs locally using duckdb.

The integration_tests/seeds folder includes dev data that already conforms to the input layer.  If any code changes require changes to these files check with the human in charge first prior to making any changes.  If you're going to generate new data (e.g. a new column) ask for instructions for how to create this column (e.g. it should have dates between this year and that year).

All SQL should be written in the most general purpose syntax because Tuva has to run on the following data warehouses:
- Snowflake
- Databricks
- BigQuery
- Microsoft Fabric
- Redshift
- DuckDB

Build the project requires loading seed files from a public s3 repo.  So you will need access to the internet to do `dbt seed` or `dbt build`.

Prior to pushing any changes to GitHub, you need to make sure the integration_tests/dbt_project.yml profile is set to "default".  This is because GitHub CI uses this, so if it's set to something else then CI will fail.