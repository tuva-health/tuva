---
id: fhir-preprocessing
title: "FHIR Preprocessing"
toc_min_heading_level: 2
toc_max_heading_level: 4
---

## Overview

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/fhir_preprocessing)

In the Tuva Project, we have built a FHIR preprocessing mart designed to streamline the transformation of healthcare data into a FHIR-ready format. This mart transforms data from the Tuva Core Data Model into standardized tables aligned with FHIR resources,
ready to be exported as CSVs, making it easier to generate valid FHIR patient bundles.

To support this pipeline, we will be releasing a companion open-source Python library that helps convert the exported CSVs into fully structured FHIR patient bundles.

Together, these tools are built for flexibility, enabling seamless integration into ETL workflows for anyone adopting FHIR for healthcare data exchange and interoperability.

Stay tuned for more updates on the open-source initiative and an announcement regarding our soon-to-be-released turnkey solution for NCQA-certified HEDIS measures.

## How to run the mart

This data mart is disabled by default. To run the data mart, simply add the variable `fhir_preprocessing_enabled` 
to your dbt_project.yml file or use the `--vars` dbt command.

dbt_project.yml:

```yaml
vars:
    fhir_preprocessing_enabled: true
```

dbt command:

```bash
dbt build --select tag:fhir_preprocessing --vars '{fhir_preprocessing_enabled: true}'
```

The FHIR Preprocessing mart outputs a table per FHIR resource.

