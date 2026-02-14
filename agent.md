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

## Claims Preprocessing

Runs immediately after the Input Layer, normalizing claims, running service categories and encounter groupers.

## Core

The Core Data Model includes cleaned claims and clinical tables.

## Data Marts

Advanced data marts for measures, groupers, and risk models.

## Terminology & Value Sets

Terminology and value sets (and reference data) are stored in AWS S3 and loaded via post hooks.  

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