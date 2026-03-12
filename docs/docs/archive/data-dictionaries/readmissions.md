---
id: readmissions
title: "Readmissions"
---

import { JsonDataTable } from '@site/src/components/JsonDataTable';

The Readmissions data model is designed to enable analysis of hospital readmissions in an acute inpatient care setting.  The data model includes concepts related to all-cause 30-day readmissions and dimensions useful to stratify readmission metrics.

## readmission_summary

**Primary Key:** encounter_id

**Foreign Keys:**
- encounter_id (join to core.encounter)
- patient_id (join to core.patient)

Each record in this table represents a unique acute inpatient admission.  Acute inpatient encounters are excluded from this table if they don't meet certain data quality requirements.  This table is the primary table that should be used for analyzing readmissions.

<JsonDataTable  jsonPath="nodes.model\.the_tuva_project\.readmissions__readmission_summary.columns"  />

## encounter_augmented

**Primary Key:** encounter_id

Each record in this table represents a unique acute inpatient admission.  However, the main difference between this table and `readmission_summary` is that this table contains _every_ acute inpatient encounter found in `core.encounter` whereas `readmission_summary` filters out acute inpatient encounters that have data quality problems which prevent them from being included in readmission analytics.  A table detailing all encounters with extra information related to the encounter, and flags for information that might affect the readmission calculations.

This table includes columns for data quality tests related to readmissions, so you can see why admissions that are not in `readmission_summary` were excluded.

<JsonDataTable  jsonPath="nodes.model\.the_tuva_project\.readmissions__encounter_augmented.columns"  />

## value sets

See the value set data dictionaries [here](../value-sets/readmissions).
