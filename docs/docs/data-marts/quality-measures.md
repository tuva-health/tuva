---
id: quality-measures
title: "Quality Measures"
---

## Overview

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/quality_measures)

The Quality Measures data mart is where we are building publicly available quality measures. The following measures are currently built into the data mart, in addition to readmission and AHRQ QIs which are their own data mart.

| Measure Name                                                                   | Measure ID                               | Specification                                                                 | Status                           | 
|--------------------------------------------------------------------------------|------------------------------------------|-------------------------------------------------------------------------------|----------------------------------|
| Documentation of Current Medications in the Medical Record                     | CMS Star C06, MIPS CQM 130               | [Link](https://qpp.cms.gov/docs/QPP_quality_measure_specifications/CQM-Measures/2023_Measure_130_MIPSCQM.pdf) | **Released**                     |
| Hospital-Wide All-Cause Readmission (HWR)                                      | CMS Star C15, MIPS CQM 479               | [Link](https://qualitynet.cms.gov/inpatient/measures/readmission/methodology) | **Released** (Readmissions mart) |
| Medication Adherence for Cholesterol (Statins)                                 | CMS Star D10, NQF 0541                   | [Link](https://www.cms.gov/files/document/2024-star-ratings-technical-notes.pdf#page=104) | **Released**                     |
| Medication Adherence for Diabetes Medications                                  | CMS Star D08, NQF 0541                   | [Link](https://www.cms.gov/files/document/2024-star-ratings-technical-notes.pdf#page=98) | **Released**                     |
| Medication Adherence for Hypertension (RAS antagonists)                        | CMS Star D09, NQF 0541                   | [Link](https://www.cms.gov/files/document/2024-star-ratings-technical-notes.pdf#page=101) | **Released**                     |
| Pain Assessment and Follow-Up                                                  | CMS Star C07, MIPS CQM 131               | [Link](https://qpp.cms.gov/docs/QPP_quality_measure_specifications/CQM-Measures/2019_Measure_131_MIPSCQM.pdf) | **Released**                     |
| Statin Therapy for the Prevention and Treatment of Cardiovascular Disease      | CMS Star C16, MIPS CQM 438               | [Link](https://mdinteractive.com/files/uploaded/file/CMS2024/2024_Measure_438_MIPSCQM.pdf) | **Released**                     |
| Statin Use in Persons with Diabetes (SUPD)                                     | CMS Star D12                             | [Link](https://www.cms.gov/files/document/2024-star-ratings-technical-notes.pdf#page=109) | **Released**                     |

The data mart includes logic that allows you to choose a measurement period end date.

- `quality_measures_period_end` defaults to the current year-end
- `snapshots_enabled` is an *optional* variable that can be enabled to allow
  running the mart for multiple years

To run the data mart without the default, simply add the `quality_measures_period_end` variable to your dbt_project.yml file or use the `--vars` dbt command. See examples below.

dbt_project.yml:

```yaml
vars:
    quality_measures_period_end: "2020-12-31"
    snapshots_enabled: true
```

Quality measures have many standard sections:

- **Measure ID:** Measures can have several different identifiers. These are 
  created by the measure steward (i.e., the organization that authored and 
  maintains the measure). For example, the identifiers for Breast Cancer 
  Screening are NQF 2372, MIPS CQM Quality ID #112, and eCQM CMS125.
- **Measure Description:** A brief description of the purpose of the measure.
- **Denominator:** The population to which the measure applies (i.e., the number 
  of people who should have received a service or action such as a screening). 
  The denominator is the lower part of a fraction used to calculate a rate.
- **Numerator:** The portion of the denominator population that received the 
  service or action for which the measure is quantifying. The numerator is the 
  upper part of a fraction used to calculate a rate.
- **Exclusions/Exceptions:** An exclusion is a reason that removes a patient 
  from both the numerator and denominator because the measure would not 
  appropriately apply to them. Exceptions are due to medical reasons (e.g., 
  patient is comatose), patient reasons (e.g., patient refuses), and system 
  reasons (e.g., shortage of a vaccine).
- **Measure Period:** The timeframe in which the service or action should have 
  occurred.
- **Value Sets:** The healthcare codes used to define the clinical concepts used 
  in the measure. These codes are from standard systems such as ICD-10, CPT, 
  LOINC, RxNorm, SNOMED, etc.

## Example SQL

<details>
  <summary>Quality Measure Performance</summary>

```sql
select
      measure_id
    , measure_name
    , performance_period_end
    , performance_rate
from quality_measures.summary_counts
order by performance_rate desc
```
</details>

<details>
  <summary>Exclusion Reason Breakdown</summary>

```sql
select
      measure_id
    , exclusion_reason
    , count(person_id) as patient_count
from quality_measures.summary_long
where exclusion_flag = 1
group by
      measure_id
    , exclusion_reason
order by
      measure_id
    , exclusion_reason
```
</details>

<details>
  <summary>Patient Pivot</summary>

```sql
select * from quality_measures.summary_wide
```
</details>
