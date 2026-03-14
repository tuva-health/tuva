---
id: ahrq-measures
title: "AHRQ Measures"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/ahrq_measures/)

The Agency for Healthcare Research and Quality (AHRQ) develops and maintains various measures to assess the quality, safety, and effectiveness of healthcare services [(AHRQ QIs)](https://qualityindicators.ahrq.gov/measures/qi_resources). These measures include the Prevention Quality Indicators [(PQIs)](https://qualityindicators.ahrq.gov/measures/pqi_resources), Inpatient Quality Indicators [(IQIs)](https://qualityindicators.ahrq.gov/measures/iqi_resources), Patient Safety Indicators [(PSIs)](https://qualityindicators.ahrq.gov/measures/psi_resources), and Pediatric Quality Indicators [(PDIs)](https://qualityindicators.ahrq.gov/measures/pdi_resources). They are used by healthcare providers, policymakers, and researchers to identify issues, monitor progress, and compare performance to improve patient outcomes and reduce costs. 

Full documentation for these measures can be found on AHRQ's website via the links above.

This data mart computes the PQIs. The individual measures and definitions as of the 2023 update are:

<table class="ahrq-table">
  <thead>
    <tr>
      <th>PQI Number</th>
      <th>PQI Name</th>
      <th>PQI Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>01</td>
      <td>Diabetes Short-Term Complications Admission Rate</td>
      <td>Hospitalizations for a principal diagnosis of diabetes with short-term complications (ketoacidosis, hyperosmolarity, or coma) per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>03</td>
      <td>Diabetes Long-Term Complications Admission Rate</td>
      <td>Hospitalizations for a principal diagnosis of diabetes with long-term complications (renal, eye, neurological, circulatory, other specified, or unspecified) per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>05</td>
      <td>Chronic Obstructive Pulmonary Disease (COPD) or Asthma in Older Adults</td>
      <td>Hospitalizations with a principal diagnosis of chronic obstructive pulmonary disease (COPD) or asthma per 100,000 population, ages 40 years and older.</td>
    </tr>
    <tr>
      <td>07</td>
      <td>Hypertension Admission Rate</td>
      <td>Hospitalizations with a principal diagnosis of hypertension per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>08</td>
      <td>Heart Failure Admission Rate</td>
      <td>Hospitalizations with a principal diagnosis of heart failure per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>11</td>
      <td>Community Acquired Pneumonia Admission Rate</td>
      <td>Hospitalizations with a principal diagnosis of community-acquired bacterial pneumonia per 100,000 population, ages 18 years or older.</td>
    </tr>
    <tr>
      <td>12</td>
      <td>Urinary Tract Infection Admission Rate</td>
      <td>Hospitalizations with a principal diagnosis of urinary tract infection per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>14</td>
      <td>Uncontrolled Diabetes Admission Rate</td>
      <td>Hospitalizations for a principal diagnosis of uncontrolled diabetes without mention of short-term (ketoacidosis, hyperosmolarity, or coma) or long-term (renal, eye, neurological, circulatory, other specified, or unspecified) complications per 100,000 population, ages 18 years and older.</td>
    </tr>
    <tr>
      <td>15</td>
      <td>Asthma in Younger Adults Admission Rate</td>
      <td>Hospitalizations for a principal diagnosis of asthma per 100,000 population, ages 18 to 39 years.</td>
    </tr>
    <tr>
      <td>16</td>
      <td>Lower-Extremity Amputation Among Patients with Diabetes Rate</td>
      <td>Hospitalizations for diabetes and a procedure of lower-extremity amputation (except toe amputations) per 100,000 population, ages 18 years and older.</td>
    </tr>
  </tbody>
</table>

## Example SQL

### PQIs Summary
To summarize and view the various locations of encounters that qualify for each PQI measure, we can start with the summary table below: 

<details>
  <summary>Summary Encounters</summary>

```sql
select *
from ahrq_measures.pqi_summary
```
</details>

<details>
  <summary>Summary by Name and Description</summary>

We can aggregate across years and join in the name and description of each measure.

```sql
  select p.data_source
  , p.pqi_number
  , m.pqi_name
  , m.pqi_description
  , sum(num_count) as pqi_encounters
  from ahrq_measures.pqi_rate p
  left join ahrq_measures._value_set_pqi_measures m on p.pqi_number = m.pqi_number
  group by 
    p.data_source
  , p.pqi_number
  , m.pqi_name
  , m.pqi_description
  order by pqi_encounters desc
```
</details>

<details>
  <summary>Summary by Facility</summary>

To view the number of PQIs at each facility in our claims dataset, we can group the summary table by facility.

```sql
  select p.data_source
  , p.facility_npi
  , l.name
  , count(*) as pqi_encounters_count
  from ahrq_measures.pqi_summary p
  left join core.location l on p.facility_npi = l.npi
  group by 
    p.data_source
  , p.facility_npi
  , l.name
  order by pqi_encounters_count desc
```
</details>

### PQIs by Rate
When calculated as a rate, PQIs are typically calculated per 100,000 population in a metropolitan area or county. When used on a claims dataset, it can be helpful to view the rates per 100,000 members instead. The numerator and denominator for each measure and year is precalculated as shown below.

<details>
  <summary>Rate</summary>

```sql
select *
from ahrq_measures.pqi_rate
```
</details>

<details>
  <summary>Aggregate by Rate</summary>

If you would like to aggregate the rate to a different level, we can use the numerator and denominator tables and calculate the rate.

```sql

with num as (
    select
        data_source
      , year_number
      , pqi_number
      , count(encounter_id) as num_count
    from ahrq_measures.pqi_num_long
    group by
        data_source
      , year_number
      , pqi_number
)

, denom as (
    select
        data_source
      , year_number
      , pqi_number
      , count(person_id) as denom_count
    from ahrq_measures.pqi_denom_long 
    group by
        data_source
      , year_number
      , pqi_number
)

select
    d.data_source
  , d.year_number
  , d.pqi_number
  , d.denom_count
  , coalesce(num.num_count, 0) as num_count
  , coalesce(num.num_count, 0) / d.denom_count * 100000 as rate_per_100_thousand
from denom as d
left join num
    on d.pqi_number = num.pqi_number
    and d.year_number = num.year_number
    and d.data_source = num.data_source
order by d.data_source
  , d.year_number
  , d.pqi_number
```
</details>

### Exclusions
Each of the PQI measures has a list of codes that exclude a encounter from a the measure. These codes are summarized in value sets which can be queried as well.

<details>
  <summary>Exclusion Value Sets</summary>

To view the list of value sets that are excluded in each of the measures, we can query the value set table. 
```sql
select distinct value_set_name
  , pqi_number
  from ahrq_measures._value_set_pqi
  order by pqi_number
```
</details>

<details>
  <summary>Exclusions by PQI Number</summary>

To summarize the number of encounters excluded by each measure, use the code below. Note that if in encounter was excluded in this logic it does not necessarily mean that it would have been in the numerator, just that it is excluded regardless of whether or not the encounter qualified for each measure.

```sql
  select data_source
  , pqi_number
  , count(*) as excluded_encounters
  from ahrq_measures.pqi_exclusion_long
  group by data_source
  , pqi_number
  order by pqi_number
```
</details>

