---
id: data-quality-overview
title: "Overview"
hide_title: true
---

# 3. Data Quality

Data Quality is one of the main focus areas of the Tuva Project.  Every healthcare dataset contains numerous data quality issues, so it's imperative that you have a strong suite of data quality tests and tools to diagnose these issues and understand their impact on analytics.

There are 3 main components in the Tuva approach to data quality:

1. **Data Pipeline Tests:** These are dbt tests that are used at run-time to determine if the source data running through Tuva has data quality problems.  
2. **Data Quality Metrics:** These are statistics that are calculated on tables throughout the project--both intermediate and final tables--that tell us whether there are analytic issues with the data.
3. **Data Quality Dashboard:** This is a dashboard that sits on top of the data quality results to make it easier to pinpoint issues.