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

- input layer: raw source data that conforms (i.e. is mapped to) Tuva's standard input layer data model
- claims preprocessing: claims normalization, service categories, and encounter groups
- core: core claims and clinical data tables
- data marts: higher level measures and groupers for analytics

# Development

Develop should occur from within the integration_tests folder.

Develop should occur on a local duckdb instance.

The integration_tests/seeds folder includes dev data that already conforms to the input layer.  If any code changes require changes to these files check with the human in charge first prior to making any changes.  If you're going to generate new data (e.g. a new column) ask for instructions for how to create this column (e.g. it should have dates between this year and that year).

All SQL should be written in the most general purpose syntax because Tuva has to run on the following data warehouses:
- Snowflake
- Databricks
- BigQuery
- Microsoft Fabric
- Redshift
- DuckDB