---
id: overview
title: "Overview"
hide_title: true
---

# Data Marts

Data marts are composed of logic (i.e. SQL) and value sets (i.e. lookup tables).  They create higher-level concepts that are useful for analytics, such as measures, groupers, cohort definitions, treatment definitions, and risk scores.

You can find the code for every data mart in the [models](https://github.com/tuva-health/the_tuva_project/tree/main/models) folder of our dbt package.

Every data mart has its own **staging layer**.  A staging layer is the set of models (i.e. tables and columns) that are necessary to run a data mart.  For example, here's where you can find the staging layer for the Acute Inpatient data mart.

![staging-layer](/img/staging-layer-example.jpg)

Every data mart has a set of final tables that are designed for analytics.  These tables are the final product of the data mart.