---
id: hcc-suspecting
title: "HCC Suspecting"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/hcc_suspecting)

The HCC Suspecting data mart identifies patients suspected of having a chronic 
condition but doesn't have the HCC recorded in the payment year. We use 
the following methods to identify these suspected conditions:

**HCC Recapture:** uses billed claims history to evaluate whether recurring 
diagnoses from prior years were captured during the current payment year.

**HCC Capture/Discovery:** uses all available sources from clinical and claims data to 
evaluate a patient's medical history, problems, comorbidities, lab results, 
medications, or observations to capture new HCCs that have not been coded 
before.

The 2024 CMS HCC model has 115 HCCs. Each condition category requires careful 
logic to identify suspecting conditions for capture. So far, we have built out 
the logic for the following conditions:

* Chronic Kidney Disease (HCC 326-329) using eGFR lab results.
* Depression (HCC 155) using medications and PHQ-9 assessments.
* Diabetes (HCC 37) using comorbidity of CKD Stage 1 or 2.
* Morbid Obesity (HCC 48) using a combination of vital signs and 
  comorbidities Diabetes, Hypertension, or  Obstructive Sleep Apnea.

**Coding System Map:** the terminology set SNOMED-CT to ICD-10-CM Map is used to 
capture additional suspecting conditions coded in a system not part of the CMS 
HCC model. This use case follows the default mapping guidance from NLM, which 
specifies that the map priority rule of “TRUE” or “OTHERWISE TRUE” should be 
applied if nothing further is known about the patient’s condition.

## Example SQL

<details>
  <summary>Total Suspected HCCs</summary>

```sql
select
      hcc_code
    , hcc_description
    , count(*) as gap_count
from hcc_suspecting.list
group by
      hcc_code
    , hcc_description
order by
      hcc_code
    , hcc_description;
```
</details>

<details>
  <summary>Total Suspected HCCs by Reason Category</summary>

```sql
select
      reason
    , count(*) as gap_count
from hcc_suspecting.list
group by reason
order by reason;
```
</details>

<details>
  <summary>Actionable Patient List</summary>

```sql
select
      person_id
    , patient_birth_date
    , patient_age
    , patient_sex
    , suspecting_gaps
from hcc_suspecting.summary
order by suspecting_gaps desc;
```
</details>
