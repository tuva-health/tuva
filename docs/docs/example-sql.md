---
id: example-sql
title: "Example SQL"
toc_max_heading_level: 2
---

The following SQL queries run against the Tuva data model.  These queries were built and tested using **Snowflake** and **Tuva version 0.15.4**.

## Acute Inpatient

The acute inpatient care setting is one of the biggest drivers of health care expenditure and as a result a primary target for research and analysis.

### Acute Inpatient Visits
Here we show a variety of different ways to analyze the total number of acute inpatient visits.

<details>
  <summary>Total Number of Acute IP Visits</summary>

```sql
select count(1)
from core.encounter
where encounter_type = 'acute inpatient'
```
</details>

<details>
  <summary>Total Number of Acute IP Visits by Month</summary>

```sql
select 
  date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, count(1) as count
from core.encounter
where encounter_type = 'acute inpatient'
group by 1
order by 1
```
</details>

<details>
  <summary>Total Number of Acute IP Visits by Admit Type</summary>

```sql
select
  admit_type_code
, admit_type_description
, count(1) as count
, cast(100 * count(distinct encounter_id)/sum(count(distinct encounter_id)) over() as numeric(38,1)) as percent
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2
order by 1,2
```
</details>

<details>
  <summary>Total Number of Acute IP Visits by Discharge Disposition</summary>

```sql
select
  discharge_disposition_code
, discharge_disposition_description
, count(1) as count
, cast(100 * count(distinct encounter_id)/sum(count(distinct encounter_id)) over() as numeric(38,1)) as percent
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2
order by 1,2
```
</details>

<details>
  <summary>Total Number of Acute IP Visits by DRG</summary>

```sql
select
  drg_code
, drg_description
, drg_code_type
, count(1) as count
, cast(100 * count(distinct encounter_id)/sum(count(distinct encounter_id)) over() as numeric(38,1)) as percent
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2,3
order by 5 desc
```
</details>

<details>
  <summary>Total Number of Acute IP Visits by Facility</summary>

```sql
select
  facility_id
, facility_name
, facility_type
, count(1) as count
, cast(100 * count(distinct encounter_id)/sum(count(distinct encounter_id)) over() as numeric(38,1)) as percent
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2,3
order by 5 desc
```
</details>

### Acute Inpatient Visits PKPY
If you have claims data, and specifically eligibility and enrollment data, you can calculate acute inpatient visits per 1,000 members per year (PKPY).  This metric normalizes the visit metric with the total number of eligible members each month.  

<details>
  <summary>Total Number of Acute IP Visits PKPY</summary>

```sql
with acute_inpatient as (
select 
  data_source
, date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, count(1) as acute_inpatient_visits
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2
)
, member_months as (
select
  data_source
, year_month
, count(1) as member_months
from core.member_months
group by 1,2
)
select
  a.data_source
, a.year_month
, b.member_months
, acute_inpatient_visits
, cast(acute_inpatient_visits / member_months *12000 as numeric(38,2)) as aip_visits_pkpy
from acute_inpatient a
inner join member_months b
  on a.year_month = b.year_month
  and a.data_source = b.data_source
order by 1,2
```
</details>

### Acute Inpatient Days PKPY 
Besides looking at the total number of visits normalized for eligibility, it's common to analyze the number of acute inpatient days per 1,000 members per year (PKPY).

<details>
  <summary>Trending Visits, Length of Stay, and Total Cost</summary>

```sql
with acute_inpatient as (
select 
  data_source
, date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, sum(length_of_stay) as sum_length_of_stay
from core.encounter
where encounter_type = 'acute inpatient'
group by 1,2
)
, member_months as (
select
  data_source
, year_month
, count(1) as member_months
from core.member_months
group by 1,2
)
select
  a.data_source
, a.year_month
, b.member_months
, cast(sum_length_of_stay / member_months *12000 as numeric(38,2)) as aip_days
from acute_inpatient a
inner join member_months b
  on a.year_month = b.year_month
  and a.data_source = b.data_source
order by 1,2
```
</details>

### Paid and Allowed Amounts
If you have claims data, you can calculate the paid and allowed amounts spent on acute inpatient visits.  Because the encounter grouper in [Encounter Types](../data-marts/encounter-types) groups multiple claims into distinct visits, this allows you to analyze the paid and allowed amounts per visit, as opposed to per claim.

<details>
  <summary>Total Paid and Allowed Amounts</summary>

```sql
select
  sum(paid_amount) as paid_amount
, sum(allowed_amount) as allowed_amount
from core.encounter
where encounter_type = 'acute inpatient'
```
</details>

<details>
  <summary>Total Paid and Allowed Amounts by Month</summary>

```sql
select
  date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, sum(paid_amount) as paid_amount
, sum(allowed_amount) as allowed_amount
from core.encounter
where encounter_type = 'acute inpatient'
group by 1
order by 1
```
</details>

### Length of Stay
Length of stay is computed as the difference between discharge date and admission date and typically reported as an average.

<details>
  <summary>Average Length of Stay by Month</summary>

```sql
select 
  date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, avg(length_of_stay) as alos
from core.encounter
where encounter_type = 'acute inpatient'
group by 1
order by 1
```
</details>

### Mortality
Mortality is computed by counting the number of discharges with a discharge disposition = 20 (the numerator) and dividing this number by the total number of acute inpatient visits (the denominator).  It's important to exclude patients that have not been discharged or for which a discharge disposition is not available.

<details>
  <summary>Mortality Rate by Month</summary>

```sql
with mortality_flag as (
select
  data_source
, date_part(year, encounter_end_date) || lpad(date_part(month, encounter_end_date),2,0) as year_month
, case
    when discharge_disposition_code = 20 then 1
    else 0
  end mortality_flag
from core.encounter
where encounter_type = 'acute inpatient'
  and discharge_disposition_code is not null
  and encounter_end_date is not null
)

select
  data_source
, year_month
, count(1) as acute_inpatient_visits
, sum(mortality_flag) as mortality_count
, sum(mortality_flag) / count(1) as mortality_rate
from mortality_flag
group by 1,2
order by 1,2
```
</details>

### Readmissions
The 30-day readmission rate is calculated by following CMS's readmission methodology which is computed via the [Readmission data mart](../data-marts/readmissions).

<details>
  <summary>30-day Readmission Rate by Month</summary>

```sql
with readmit as 
(
select
to_char(discharge_date, 'YYYYMM') as year_month
, sum(case when index_admission_flag = 1 then 1 else 0 end) as index_admissions
, sum(case when index_admission_flag = 1 and unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
from readmissions.readmission_summary
group by to_char(discharge_date, 'YYYYMM')
)

select 
year_month
,index_admissions
,readmissions
,case when index_admissions = 0 then 0 else readmissions / index_admissions end as readmission_rate
from readmit
order by year_month
```
</details>

<details>
  <summary>30-day Readmission Rate by MS-DRG</summary>

```sql
with readmit as 
(
select
  drg_code
, sum(case when index_admission_flag = 1 then 1 else 0 end) as index_admissions
, sum(case when index_admission_flag = 1 and unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
from readmissions.readmission_summary
group by 1
)

select 
  drg_code
, index_admissions
, readmissions
, case when readmissions = 0 then 0 else readmissions / index_admissions end as readmission_rate
from readmit
order by index_admissions desc
```
</details>

### Readmissions Data Quality
CMS's readmission methodology excludes certain encounters from the calculation if they are missing certain fields. Here we break these down to show the different reasons encounters were excluded.


<details>
  <summary>Disqualified Encounters</summary>

Let's find how many encounters were disqualified.

```sql
select count(*) encounter_count
from readmissions.encounter_augmented
where disqualified_encounter_flag = 1
```
</details>

<details>
  <summary>Disqualification Reason</summary>
  
We can see the reason(s) why an encounter was disqualified by unpivoting the disqualification reason column.

```sql
with disqualified_unpivot as (
    select encounter_id
    , disqualified_reason
    , flagvalue
    from readmissions.encounter_augmented
    unpivot(
        flagvalue for disqualified_reason in (
            invalid_discharge_disposition_code_flag
            , invalid_drg_flag
            , invalid_primary_diagnosis_code_flag
            , missing_admit_date_flag
            , missing_discharge_date_flag
            , admit_after_discharge_flag
            , missing_discharge_disposition_code_flag
            , missing_drg_flag
            , missing_primary_diagnosis_flag
            , no_diagnosis_ccs_flag
            , overlaps_with_another_encounter_flag
        )
    ) as unpvt
)


select encounter_id
, disqualified_reason
, row_number () over (partition by encounter_id order by disqualified_reason) as disqualification_number
from disqualified_unpivot
where flagvalue = 1
```
</details>


### Discharge Location

Based on the discharge disposition field, it is often helpful to group these into the most common locations for analysis.

<details>
  <summary>Discharge Location</summary>

```sql
select case when discharge_disposition_code = '01' then 'Home'
            when discharge_disposition_code = '03' then 'SNF'
            when discharge_disposition_code = '06' then 'Home Health'
            when discharge_disposition_code = '62' then 'Inpatient Rehab'
            when discharge_disposition_code = '20' then 'Expired'
            else 'Other'
            end as discharge_location
        ,count(*) as encounters
        ,cast(sum(paid_amount) as decimal(18,2)) as total_paid_amount
        ,cast(sum(paid_amount)/count(*) as decimal(18,2)) as paid_per_encounter
from core.encounter
group by 
case when discharge_disposition_code = '01' then 'Home'
            when discharge_disposition_code = '03' then 'SNF'
            when discharge_disposition_code = '06' then 'Home Health'
            when discharge_disposition_code = '62' then 'Inpatient Rehab'
            when discharge_disposition_code = '20' then 'Expired'
            else 'Other'
            end 
order by count(*) desc
```
</details>


## AHRQ PQIs

The Agency for Healthcare Research and Quality (AHRQ) develops and maintains various measures to assess the quality, safety, and effectiveness of healthcare services. These measures include the Prevention Quality Indicators (PQIs), Inpatient Quality Indicators (IQIs), Patient Safety Indicators (PSIs), and Pediatric Quality Indicators (PDIs). They are used by healthcare providers, policymakers, and researchers to identify issues, monitor progress, and compare performance to improve patient outcomes and reduce costs.

The Prevention Quality Indicators (PQIs) are a set of measures developed by AHRQ that focus on ambulatory care-sensitive conditions, which are health issues that can often be effectively managed or prevented through timely and appropriate primary care interventions.

### PQIs Summary
To summarize and view the various location of encounters that qualify for each PQI measure, we can start with the summary table below: 

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
  , p.facility_id
  , l.name
  , count(*) as pqi_encounters_count
  from ahrq_measures.pqi_summary p
  left join core.location l on p.facility_id = l.npi
  group by 1,2,3
  order by 3 desc
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

## Chronic Conditions

Chronic diseases are one of the biggest drivers of healthcare utilization and expenditure.  Here we provide an examples of the types of analytics you can do with Tuva related to chronic conditions.

<details>
  <summary>Prevalence of Chronic Conditions</summary>

In this query we show how often each chronic condition occurs in the patient population.

```sql
select
  condition
, count(distinct person_id) as total_patients
, cast(count(distinct person_id) * 100.0 / (select count(distinct person_id) from core.patient) as numeric(38,2)) as percent_of_patients
from chronic_conditions.tuva_chronic_conditions_long
group by 1
order by 2 desc
```

</details>

<details>
  <summary>Distribution of Chronic Conditions</summary>

In this query we show how many patients have 0 chronic conditions, how many patients have 1 chronic condition, how many patients have 2 chronic conditions, etc.

```sql
with patients as (
select person_id
from core.patient
)

, conditions as (
select distinct
  a.person_id
, b.condition
from patients a
left join chronic_conditions.tuva_chronic_conditions_long b
 on a.person_id = b.person_id
)

, condition_count as (
select
  person_id
, count(distinct condition) as condition_count
from conditions
group by 1
)

select 
  condition_count
, count(1)
, cast(100 * count(distinct person_id)/sum(count(distinct person_id)) over() as numeric(38,1)) as percent
from condition_count
group by 1
order by 1
```

</details>

## CMS-HCCs

CMS-HCC is the risk adjustment model used by CMS.  Analyzing risk scores based on the output of this model is an important use case for value-based care analytics.

<details>
  <summary>Average CMS-HCC Risk Scores</summary>

```sql
select
    count(distinct person_id) as patient_count
    , avg(blended_risk_score) as average_blended_risk_score
    , avg(normalized_risk_score) as average_normalized_risk_score
    , avg(payment_risk_score) as average_payment_risk_score
from cms_hcc.patient_risk_scores
```
</details>

<details>
  <summary>Average CMS-HCC Risk Scores by Patient Location</summary>

```sql
select
      patient.state
    , patient.city
    , patient.zip_code
    , avg(risk.payment_risk_score) as average_payment_risk_score
from cms_hcc.patient_risk_scores as risk
inner join core.patient as patient
    on risk.person_id = patient.person_id
group by
      patient.state
    , patient.city
    , patient.zip_code
```
</details>


<details>
  <summary>Distribution of CMS-HCC Risk Factors</summary>

```sql
select
      risk_factor_description
    , count(*) as total
    , cast(100 * count(*)/sum(count(*)) over() as numeric(38,1)) as percent
from cms_hcc.patient_risk_factors
group by risk_factor_description
order by 2 desc
```
</details>

<details>
  <summary>Risk Weighted by Member Months</summary>

```sql
select sum(payment_risk_score_weighted_by_months) / sum(member_months) as weighted_risk_total
from cms_hcc.patient_risk_scores;
```
</details>

<details>
  <summary>Stratified CMS-HCC Risk Scores</summary>

```sql
select
      (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score <= 1.00) as low_risk
    , (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score = 1.00) as average_risk
    , (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score > 1.00) as high_risk
    , (select avg(payment_risk_score) from cms_hcc.patient_risk_scores) as total_population_average;
```
</details>

<details>
  <summary>Top 10 CMS-HCC Conditions</summary>

```sql
select
      risk_factor_description
    , count(*) patient_count
from cms_hcc.patient_risk_factors
where factor_type = 'Disease'
group by risk_factor_description
order by count(*) desc
limit 10;
```
</details>

## Demographics

Here we demonstrate the different types of patient demographics in Tuva and how you can use them in analysis.

<details>
  <summary>Age Distribution</summary>

```sql
with patient_age as (
select
  data_source
, person_id
, floor(datediff(day, birth_date, current_date)/365) as age
from core.patient
)

, age_groups as (
select
  data_source
, person_id
, age
, case 
    when age >= 0 and age < 2 then '00-02'
    when age >= 2 and age < 18 then '02-18'
    when age >= 18 and age < 30 then '18-30'
    when age >= 30 and age < 40 then '30-40'
    when age >= 40 and age < 50 then '40-50'
    when age >= 50 and age < 60 then '50-60'
    when age >= 60 and age < 70 then '60-70'
    when age >= 70 and age < 80 then '70-80'
    when age >= 80 and age < 90 then '80-90'
    when age >= 90 then '>= 90'
    else 'Missing Age' 
  end as age_group
from patient_age
)

select
  data_source
, age_group
, count(distinct person_id) as patient_count
, cast(100 * count(distinct person_id)/sum(count(distinct person_id)) over() as numeric(38,1)) as percent
from age_groups
group by 1,2
order by 1,2
```
</details>

<details>
  <summary>Sex Distribution</summary>

```sql
select
  sex
, count(distinct person_id) as count
, cast(100 * count(distinct person_id)/sum(count(distinct person_id)) over() as numeric(38,1)) as percent
from core.patient
group by 1
order by 1
```
</details>

<details>
  <summary>Race Distribution</summary>

```sql
select
  race
, count(distinct person_id) as count
, cast(100 * count(distinct person_id)/sum(count(distinct person_id)) over() as numeric(38,1)) as percent
from core.patient
group by 1
order by 1
```
</details>

<details>
  <summary>Members by State and Zip Code</summary>

```sql
select state
,zip_code
,count(*) as member_count
from core.patient
group by 
state
,zip_code
order by count(*) desc
```
</details>

## ED Visits

Analyzing ED claims data helps identify high utilizers of emergency services, often indicating overuse of EDs for conditions that can be managed with proper primary care. 

### ED Visits Trended
<details>
  <summary>Trending ED Visit Volume, PKPY, and Cost</summary>

```sql

with ed as (
select 
  data_source
  ,TO_CHAR(encounter_end_date, 'YYYYMM') AS year_month
  ,COUNT(*) AS ed_visits
  ,AVG(paid_amount) as avg_paid_amount
  ,sum(paid_amount) as total_paid_amount
from core.encounter
where encounter_type = 'emergency department'
group by data_source
 ,TO_CHAR(encounter_end_date, 'YYYYMM') 
)

, member_months as (
select
  data_source
, year_month
, count(1) as member_months
from core.member_months
group by 
  data_source
, year_month

)
select
  a.data_source
, a.year_month
, b.member_months
, ed_visits
, cast(ed_visits / member_months * 12000 as decimal(18,2)) as ed_visits_pkpy
, cast(avg_paid_amount as decimal(18,2)) as avg_paid_amount
, cast(total_paid_amount as decimal(18,2))as ed_total_paid_amount
from  member_months b
left join ed a
  on a.year_month = b.year_month
  and a.data_source = b.data_source
order by 1,2
;
```
</details>

<details>
  <summary>ED Spend as Percent of Total Spend</summary>

```sql
select data_source
,year_month
,sum(emergency_department_paid) as ed_paid
,sum(total_paid) as total_paid
,cast(sum(emergency_department_paid) as decimal(18,2))/cast(sum(total_paid) as decimal(18,2)) as ed_percent_of_total_paid
from financial_pmpm.pmpm_prep
group by data_source
,year_month
order by data_source
,year_month
```
</details>

<details>
  <summary>ED Visits by Member and Year</summary>

```sql
select 
data_source
, TO_CHAR(encounter_end_date, 'YYYY') AS year_nbr
, person_id
, COUNT(*) AS ed_visits
from core.encounter
where encounter_type = 'emergency department'
group by data_source
, TO_CHAR(encounter_end_date, 'YYYY')
, person_id
ORDER BY ed_visits desc
, year_nbr
, person_id
;
```
</details>

<details>
  <summary>Frequency Distribution of ED Visits</summary>

```sql
with visits as (
select 
data_source
, person_id
, COUNT(*) AS ed_visits
from core.encounter
where encounter_type = 'emergency department'
group by data_source
, person_id
)

,members as (
select distinct person_id
,data_source
from core.member_months
)

,members_total as (
select count(*) as total_member_count
from members
)

,members_with_visits as (
select m.person_id
,m.data_source
,coalesce(v.ed_visits,0) as ed_visits
from members m
left join visits v on m.person_id = v.person_id
and
m.data_source = v.data_source
)

select ed_visits
,count(*) as member_count
,count(*) / cast(max(total_member_count) as real) as percent_of_total_members
from members_with_visits
cross join members_total 
group by ed_visits
order by ed_visits 
;
```
</details>

<details>
  <summary>Count of ED NPIs</summary>

```sql

select data_source
 ,count(distinct facility_id) as ed_facilities_count
from core.encounter e
where encounter_type = 'emergency department'
group by 
 data_source
order by ed_facilities_count desc
```
</details>

<details>
  <summary>Visit by Facility</summary>

```sql
select 
 facility_id
, COUNT(*) AS ed_visits
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e
where encounter_type = 'emergency department'
group by 
 facility_id
ORDER BY ed_visits desc
;
```
</details>

<details>
  <summary>Admit Source and Type</summary>

```sql
select 
admit_source_code
, admit_source_description
, admit_type_code
, admit_type_description
, count(*) AS ed_visits
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e
where encounter_type = 'emergency department'
group by 
admit_source_code
, admit_source_description
, admit_type_code
, admit_type_description
ORDER BY ed_visits desc
;
```

</details>

### ED Classification
The Tuva Project utilizes the NYU algorithm to classify ED visits, helping to identify care patterns that are not being met by primary care providers.

Of the different classifications in the NYU algorithm, the categories usually classified as "potentially preventable" are:

- Emergent, Primary Care Treatable
- Non-Emergent
- Emergent, ED Care Needed, Preventable/Avoidable

<details>
  <summary>ED Classification</summary>

```sql
select coalesce(s.ed_classification_description,'Not Classified') as ed_classification_category
, count(*) as visit_count
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e 
left join ed_classification.summary s on e.encounter_id = s.encounter_id
group by coalesce(s.ed_classification_description,'Not Classified')
order by visit_count desc
```
</details>

<details>
  <summary>Members with at least One Potentially Preventable ED Visit</summary>

```sql
with enc as 
(
select e.person_id
,left(year_month,4) as year_nbr
,data_source
,count(distinct e.encounter_id) as potentially_preventable
,sum(e.paid_amount) as paid_amount
from core.encounter e 
inner join ed_classification.summary s on e.encounter_id = s.encounter_id
where ed_classification_description in ('Emergent, Primary Care Treatable','Non-Emergent','Emergent, ED Care Needed, Preventable/Avoidable')
group by e.person_id
,data_source
,left(year_month,4) 
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(case when enc.potentially_preventable >=1 then 1 else 0 end) as members_with_potentially_preventable
,count(*) as total_members
,sum(case when enc.potentially_preventable >=1 then 1 else 0 end)/count(*) as potentially_preventable_percent_of_total
,sum(enc.paid_amount)/sum(enc.potentially_preventable) as avg_cost_potentially_preventable
from member_year my 
left join enc on my.year_nbr = enc.year_nbr
and
enc.data_source = my.data_source
and
enc.person_id = my.person_id
group by my.data_source
,my.year_nbr
```
</details>

<details>
  <summary>Primary Diagnosis Codes for Avoidable Categories</summary>

```sql
select coalesce(s.ed_classification_description,'Not Classified') as ed_classification_category
, e.primary_diagnosis_code
, e.primary_diagnosis_description
, count(*) as visit_count
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e 
left join ed_classification.summary s on e.encounter_id = s.encounter_id
where ed_classification_description in ('Emergent, Primary Care Treatable','Non-Emergent','Emergent, ED Care Needed, Preventable/Avoidable')
group by coalesce(s.ed_classification_description,'Not Classified')
, e.primary_diagnosis_code
, e.primary_diagnosis_description
order by ed_classification_category
, visit_count desc
;
```
</details>

### ED Diagnosis Grouping
The Tuva Project provides several ways of grouping diagnosis codes. 
CCSR (AHRQ) provides a hierarchy grouping of diagnosis codes, and is useful for recognizing patterns of care by what the patient was diagnosed with at the ED.

Chronic Conditions are a way of grouping members by conditions that they have been diagnosed with (within the relevant timespan, usually the last 1 or 2 years.)

<details>
  <summary>ED Visits by CCSR Category and Body System</summary>

```sql

select     
P.ccsr_category
, P.ccsr_category_description
, P.ccsr_parent_category
, B.body_system
, count(*) as visit_count
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e 
left join ccsr.dx_vertical_pivot P ON e.primary_diagnosis_code = p.Code
    and p.ccsr_category_rank = 1
left join CCSR._value_set_dxccsr_v2023_1_body_systems B ON P.CCSR_PARENT_CATEGORY = B.CCSR_PARENT_CATEGORY
group by P.ccsr_category
, P.ccsr_category_description
, P.ccsr_parent_category
, B.body_system
order by visit_count desc

```
</details>

<details>
  <summary>ED Visits by Chronic Condition Category</summary>

Since members often have more than one chronic condition, encounters are duplicated for each chronic condition causing the total amount to be inflated. The division of encounters by chronic condition is useful for comparision across disease states, and less so from the total standpoint.

```sql


WITH chronic_condition_members as 
(
SELECT DISTINCT 
person_id
FROM chronic_conditions.tuva_chronic_conditions_long
)

,chronic_conditions as (
SELECT person_id
, condition
FROM chronic_conditions.tuva_chronic_conditions_long

UNION 

SELECT p.person_id
, 'No Chronic Conditions' as Condition
FROM core.patient p
LEFT JOIN chronic_condition_members ccm on p.person_id=ccm.person_id
where ccm.person_id is null
)

select cc.condition
, count(*) as visit_count
, sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
, cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e 
left join chronic_conditions cc on e.person_id = cc.person_id
where encounter_type = 'emergency department'
group by 
cc.condition
order by visit_count desc
;
```
</details>

## Medical PMPM

Per Member Per Month (PMPM) spend is the starting point for any claims based analysis. 

<details>
  <summary>Calculate Member Months and Total Medical Spend</summary>

```sql
Select 
data_source
, year_month
, cast(sum(medical_paid) as decimal(18,2)) as medical_paid
, count(*) as member_months
, cast(sum(medical_paid)/count(*) as decimal(18,2)) as pmpm
from tuva_synthetic.financial_pmpm.pmpm_prep
group by 
data_source
, year_month
order by data_source
, year_month
```
</details>

<details>
  <summary>Trending PMPM by Service Category</summary>

The pmpm table already breaks out pmpm by service category and groups it at the member month level.

```sql
select *
from financial_pmpm.pmpm_payer
order by 1
```
</details>

<details>
  <summary>Trending PMPM by Claim Type</summary>

Here we calculate PMPM manually by counting member months and joining payments by claim type to them.

```sql
with mm as 
(
select 
data_source
,year_month
,count(*) as member_months
from core.member_months
group by 
data_source
,year_month
)

,medical_claims as (
select 
  mc.data_source
  , to_char(claim_start_date, 'YYYYMM') AS year_month
  , claim_type
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.medical_claim mc
inner join core.member_months mm on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
and
to_char(mc.claim_start_date, 'YYYYMM') = mm.year_month
group by mc.data_source
, to_char(claim_start_date, 'YYYYMM')
, claim_type
)

select mm.data_source
,mm.year_month
,medical_claims.claim_type
,medical_claims.paid_amount
,mm.member_months
,cast(medical_claims.paid_amount / mm.member_months as decimal(18,2)) as pmpm_claim_type
from mm
left join medical_claims on mm.data_source = medical_claims.data_source
and
mm.year_month = medical_claims.year_month
order by mm.data_source
,mm.year_month
,medical_claims.claim_type
```
</details>


<details>
  <summary>PMPM by Chronic Condition</summary>

Here we calculate PMPM by chronic condition. Since members can and do have more than one chronic condition, payments and members months are duplicated. This is useful for comparing spend across chronic conditions, but should be used with caution given the duplication across conditions.

```sql

WITH chronic_condition_members as 
(
select distinct 
person_id
from chronic_conditions.tuva_chronic_conditions_long
)

,chronic_conditions as (
select person_id
, condition
from chronic_conditions.tuva_chronic_conditions_long

UNION 

select p.person_id
, 'No Chronic Conditions' as Condition
from core.patient p
left join chronic_condition_members ccm on p.person_id=ccm.person_id
where ccm.person_id is null
)

,medical_claims as (
select 
  mc.data_source
  , mc.person_id
  , to_char(claim_start_date, 'YYYYMM') AS year_month
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.medical_claim mc
inner join core.member_months mm on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
and
to_char(mc.claim_start_date, 'YYYYMM') = mm.year_month
group by mc.data_source
, mc.person_id
, to_char(claim_start_date, 'YYYYMM')
)

select 
mm.data_source
//,mm.year_month uncomment to view at month level
,cc.condition
,count(*) as member_months
,sum(mc.paid_amount) as paid_amount
,cast(sum(mc.paid_amount) / count(*) as decimal(18,2)) as medical_pmpm
from core.member_months mm
left join chronic_conditions cc on mm.person_id = cc.person_id
left join medical_claims mc on mm.person_id = mc.person_id
and 
mm.year_month = mc.year_month
and
mm.data_source = mc.data_source
group by 
mm.data_source
//,mm.year_month
,cc.condition
order by member_months desc
```
</details>

### Claims and Enrollment 

Understanding the relationship between enrollment and claims is paramount for in-depth claims and population health analysis. It is important to analyze the proportion of the enrolled population that is actively utilizing healthcare services and to identify the characteristics of those who have not accessed care at all. 


<details>
  <summary>Members with Claims by Month</summary>

```sql

with medical_claim as 
(
select 
  data_source
  , person_id
  , to_char(claim_start_date, 'YYYYMM') AS year_month
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.medical_claim
GROUP BY data_source
, person_id
, to_char(claim_start_date, 'YYYYMM')
)

select mm.data_source
, mm.year_month
, sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
, count(*) as total_member_months
, cast(sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percent_members_with_claims
from core.member_months mm 
left join medical_claim mc on mm.person_id = mc.person_id
and
mm.data_source = mc.data_source
and
mm.year_month = mc.year_month
group by mm.data_source
, mm.year_month
order by data_source
,year_month
```
</details>

<details>
  <summary>Members with Claims</summary>

```sql
with medical_claims as (
select 
  data_source
  , person_id
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.medical_claim
GROUP BY data_source
, person_id
)

, members as (
select distinct person_id
,data_source
from core.member_months
)

select mm.data_source
,sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
,count(*) as members
,sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as percentage_with_claims
from members mm
left join medical_claims mc on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
group by mm.data_source
```
</details>

<details>
  <summary>Claims with Enrollment</summary>
  
  The inverse of the above. Ideally this number will be 100%, but there could be extenuating reasons why not all claims have a corresponding member with enrollment.

  ```sql

select 
  mc.data_source
  , sum(case when mm.person_id is not null then 1 else 0 end) as claims_with_enrollment
  , count(*) as claims
  , cast(sum(case when mm.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percentage_claims_with_enrollment
from core.medical_claim mc
left join core.member_months mm on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
and
to_char(mc.claim_start_date, 'YYYYMM') = mm.year_month
GROUP BY mc.data_source
```
</details>

## Pharmacy

### Pharmacy Claims and Enrollment

<details>
  <summary>Members with Pharmacy Claims by Month</summary>

```sql
with pharmacy_claim as 
(
select 
  data_source
  , person_id
  , to_char(paid_date, 'YYYYMM') AS year_month
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.pharmacy_claim
GROUP BY data_source
, person_id
, to_char(paid_date, 'YYYYMM')
)

select mm.data_source
, mm.year_month
, sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
, count(*) as total_member_months
, cast(sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percent_members_with_claims
from core.member_months mm 
left join pharmacy_claim mc on mm.person_id = mc.person_id
and
mm.data_source = mc.data_source
and
mm.year_month = mc.year_month
group by mm.data_source
, mm.year_month
order by data_source
,year_month
```
</details>

<details>
  <summary>Members with Pharmacy Claims</summary>

```sql
with pharmacy_claim as (
select 
  data_source
  , person_id
  , cast(sum(paid_amount) as decimal(18,2)) AS paid_amount
from core.pharmacy_claim
GROUP BY data_source
, person_id
)

, members as (
select distinct person_id
,data_source
from core.member_months
)

select mm.data_source
,sum(case when mc.person_id is not null then 1 else 0 end) as members_with_claims
,count(*) as members
,sum(case when mc.person_id is not null then 1 else 0 end) / count(*) as percentage_with_claims
from members mm
left join pharmacy_claim mc on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
group by mm.data_source
```
</details>

<details>
  <summary>Pharmacy Claims with Enrollment</summary>
  
  The inverse of the above. Ideally this number will be 100%, but there could be extenuating reasons why not all claims have a corresponding member with enrollment.

  ```sql
select 
  mc.data_source
  , sum(case when mm.person_id is not null then 1 else 0 end) as claims_with_enrollment
  , count(*) as claims
  , cast(sum(case when mm.person_id is not null then 1 else 0 end) / count(*) as decimal(18,2)) as percentage_claims_with_enrollment
from core.pharmacy_claim mc
left join core.member_months mm on mc.person_id = mm.person_id
and
mc.data_source = mm.data_source
and
to_char(mc.paid_date, 'YYYYMM') = mm.year_month
GROUP BY mc.data_source

```
</details>

### Understanding Retail Pharmacy Utilization

<details>
  <summary>Prescribing Providers</summary>

```sql
select 
data_source
,prescribing_provider_id
,sum(paid_amount) as pharmacy_paid_amount
,sum(days_supply) as pharmacy_days_supply
from core.pharmacy_claim
group by 
data_source
,prescribing_provider_id
order by pharmacy_paid_amount desc
```
</details>

<details>
  <summary>Pharmacy Names</summary>

```sql
select 
data_source
,dispensing_provider_id
,sum(paid_amount) as pharmacy_paid_amount
,sum(days_supply) as pharmacy_days_supply
from core.pharmacy_claim
group by dispensing_provider_id
,data_source
order by pharmacy_paid_amount desc
```
</details>

### Brand vs Generic Rx
<details>
  <summary>Brand Generic Dollar Opportunity</summary>
  
We can view the total dollar opportunity from switching brands to generics with this query.

```sql
select
    data_source
  , sum(generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded
group by 
    data_source

```
</details>
<details>
  <summary>Opportunity by Brand Name</summary>
  
To view the drugs that would yield the most savings by switching to generic, we can group by brand name and sort high to low on opportunity.

```sql
select
    data_source
  , brand_name
  , sum(generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded
where 
  generic_available_total_opportunity > 0
group by 
    brand_name
  , data_source
order by generic_available_total_opportunity desc

```
</details>
<details>
  <summary>Generic NDCs Available</summary>
  
To view the generic ndcs that exist for a particular brand drug (Concerta in this example), we can join to the generic_available_list table.

```sql
select
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
  , sum(e.generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded as e
inner join pharmacy.generic_available_list as g
  on e.generic_available_sk = g.generic_available_sk
where 
  e.brand_name = 'Concerta'
group by 
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
order by generic_available_total_opportunity desc

```
</details>
<details>
  <summary>Generics Available in Prescribed History</summary>
  
To view only the generics that have been prescribed in the pharmacy claims data history (for a given data source), we can set a filter in the where clause for the generic_prescribed_history flag.

```sql
select
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
  , sum(e.generic_available_total_opportunity) as generic_available_total_opportunity
from pharmacy.pharmacy_claim_expanded as e
inner join pharmacy.generic_available_list as g
  on e.generic_available_sk = g.generic_available_sk
where 
  e.brand_name = 'Concerta'
  and g.generic_prescribed_history = 1
group by 
    e.data_source
  , e.ndc_code
  , e.ndc_description
  , g.generic_ndc
  , g.generic_ndc_description
  , g.generic_prescribed_history
  , g.brand_paid_per_unit
  , g.generic_cost_per_unit
order by generic_available_total_opportunity desc

```
</details>

## Primary Care

Analyzing primary care utilization in claims data is crucial for understanding healthcare access, quality, and costs, as it provides insights into the frequency and types of services patients receive from their primary care providers. By examining claims data, researchers and policymakers can identify patterns, disparities, and potential areas for improvement in primary care delivery.


### Primary Care Spend and PMPM


<details>
  <summary>Primary Care Spend, % of Total, and PMPM </summary>

```sql
with primary_care as (
select 
m.data_source
,TO_CHAR(claim_start_date, 'YYYYMM') AS year_month
,sum(paid_amount) as primary_care_paid_amount
from core.medical_claim m
inner join core.practitioner p on coalesce(m.rendering_id,m.billing_id) = p.npi
inner join core.member_months mm on m.person_id = mm.person_id
and
m.data_source = mm.data_source
and
to_char(m.claim_start_date, 'YYYYMM') = mm.year_month
where service_category_2 in ('Office Visit','Outpatient Hospital or Clinic')
and
p.specialty in ('Family Medicine','Internal Medicine','Obstetrics & Gynecology','Pediatric Medicine','Physician Assistant','Nurse Practitioner')
group by TO_CHAR(claim_start_date, 'YYYYMM')
,m.data_source
)

,total_cost as 
(
select data_source
,year_month
,sum(total_paid) as total_paid
from financial_pmpm.pmpm_prep
group by data_source
,year_month
)

select pmpm.data_source
,pmpm.year_month
,cast(pc.primary_care_paid_amount as decimal(18,2)) as primary_care_paid_amount
,cast(tc.total_paid as decimal(18,2)) as total_paid
,cast(pc.primary_care_paid_amount/tc.total_paid  as decimal(18,2)) primary_care_percent_of_total
,cast(pc.primary_care_paid_amount/pmpm.member_months as decimal(18,2)) as primary_care_pmpm
from financial_pmpm.pmpm_payer pmpm
left join primary_care pc on pmpm.data_source = pc.data_source
and
pmpm.year_month = pc.year_month
left join total_cost tc on pmpm.data_source = tc.data_source
and
pmpm.year_month = tc.year_month
order by 
pmpm.data_source
,pmpm.year_month
```
</details>


### Primary Care Visits

<details>
  <summary>Average Primary Care Visits per Member </summary>

```sql
with primary_care as 
(
select 
m.data_source
,TO_CHAR(claim_start_date, 'YYYY') AS year_nbr
,count(distinct claim_id) as visit_count
,m.person_id
from core.medical_claim m
inner join core.practitioner p on coalesce(m.rendering_id,m.billing_id) = p.npi
inner join core.member_months mm on m.person_id = mm.person_id
and
m.data_source = mm.data_source
and
to_char(m.claim_start_date, 'YYYYMM') = mm.year_month
where service_category_2 in ('Office Visit','Outpatient Hospital or Clinic')
and
p.specialty in ('Family Medicine','Internal Medicine','Obstetrics & Gynecology','Pediatric Medicine','Physician Assistant','Nurse Practitioner')
group by TO_CHAR(claim_start_date, 'YYYY')
,m.data_source
,m.person_id
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(pc.visit_count) as primary_visit_count
,count(distinct my.person_id) as member_count
,sum(pc.visit_count)/count(distinct my.person_id) as primary_care_visits_per_member
from member_year my
left join primary_care pc on my.data_source = pc.data_source
and
my.year_nbr = pc.year_nbr
and
my.person_id = pc.person_id
group by 
my.data_source
,my.year_nbr
order by my.year_nbr
,my.data_source
```
</details>

<details>
  <summary>Members with at Least One Primary Care Visit</summary>

```sql

with primary_care as 
(
select 
m.data_source
,TO_CHAR(claim_start_date, 'YYYY') AS year_nbr
,count(distinct claim_id) as visit_count
,m.person_id
from core.medical_claim m
inner join core.practitioner p on coalesce(m.rendering_id,m.billing_id) = p.npi
inner join core.member_months mm on m.person_id = mm.person_id
and
m.data_source = mm.data_source
and
to_char(m.claim_start_date, 'YYYYMM') = mm.year_month
where service_category_2 in ('Office Visit','Outpatient Hospital or Clinic')
and
p.specialty in ('Family Medicine','Internal Medicine','Obstetrics & Gynecology','Pediatric Medicine','Physician Assistant','Nurse Practitioner')
group by TO_CHAR(claim_start_date, 'YYYY')
,m.data_source
,m.person_id
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(case when pc.visit_count >= 1 then 1 else 0 end) as at_least_one_pc_visit
,count(*) as member_count
,sum(case when pc.visit_count >= 1 then 1 else 0 end)/count(*)  as percent_at_least_one_pc_visit
from member_year my
left join primary_care pc on my.data_source = pc.data_source
and
my.year_nbr = pc.year_nbr
and
my.person_id = pc.person_id
group by 
my.data_source
,my.year_nbr
order by 
year_nbr
,data_source
;
```

</details>

### Primary Care Providers

<details>
  <summary>Primary Care Providers</summary>

```sql
select 
coalesce(m.rendering_id,m.billing_id) as primary_care_provider_npi
,p.provider_first_name || ' '|| provider_last_name as primary_care_provider_name
,count(distinct claim_id) as visit_count
,sum(paid_amount) as paid_amount
from core.medical_claim m
inner join core.practitioner p on coalesce(m.rendering_id,m.billing_id) = p.npi
where service_category_2 in ('Office Visit','Outpatient Hospital or Clinic')
and
p.specialty in ('Family Medicine','Internal Medicine','Obstetrics & Gynecology','Pediatric Medicine','Physician Assistant','Nurse Practitioner')
group by coalesce(m.rendering_id,m.billing_id) 
,p.provider_first_name || ' '|| provider_last_name
order by visit_count desc
```
</details>

## Urgent Care

Urgent Care serves as a low-cost solution when compared to the Emergency Department. Analyzing the frequency and circumstances of Urgent Care use, and comparing these to Emergency Department statistics, provides a useful perspective on how people use immediate care options.

### Urgent Care Utilization

<details>
  <summary>Urgent Care by Facility</summary>

```sql
select 
mc.billing_id
,l.name
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as visits
,sum(coalesce(mc.paid_amount,0)) as paid_amount
from core.medical_claim mc
left join core.location l on mc.billing_id=l.npi
where service_category_2 = 'Urgent Care'
group by mc.billing_id
,l.name
order by paid_amount desc
```
</details>

<details>
  <summary>Urgent Care PMPM and PKPY</summary>

```sql
with uc as 
(
select mc.person_id
,mc.data_source
,to_char(claim_start_date, 'yyyy') as year_nbr
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as visits
,sum(mc.paid_amount) as paid_amount
from core.medical_claim mc
where service_category_2 = 'Urgent Care'
group by mc.person_id
,mc.data_source
,to_char(claim_start_date, 'yyyy')
)

,member_year as (
select data_source
,person_id
,left(year_month,4) as year_nbr
,count(*) as member_months
from financial_pmpm.pmpm_prep pmpm
group by 
 data_source
,person_id
,left(year_month,4) 
)

select my.data_source
,my.year_nbr
,sum(member_months) as member_months
,cast(sum(uc.visits)/sum(member_months) * 12000 as decimal(18,2)) as urgent_care_pkpy
,cast(sum(uc.paid_amount)/sum(member_months) as decimal(18,2)) as urgent_care_pmpm
,sum(uc.visits) as urgent_care_absolute_visits
,cast(sum(uc.paid_amount) as decimal(18,2)) as urgent_care_absolute_paid
from member_year my 
left join uc on uc.data_source = my.data_source
and
uc.person_id = my.person_id
group by my.data_source
,my.year_nbr
order by data_source
,my.year_nbr

```
</details>

<details>
  <summary>Members with at least One Urgent Care Visit</summary>

```sql

with enc as 
(
select mc.person_id
,to_char(claim_start_date, 'yyyy') as year_nbr
,mc.data_source
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as urgent_care
,sum(mc.paid_amount) as paid_amount
from core.medical_claim mc
where service_category_2 = 'Urgent Care'
group by mc.person_id
,mc.data_source
,to_char(claim_start_date, 'yyyy')
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(case when enc.urgent_care >=1 then 1 else 0 end) as members_with_at_least_one_uc
,count(*) as total_members
,sum(case when enc.urgent_care >=1 then 1 else 0 end)/count(*) as at_least_one_percent_total
,sum(enc.paid_amount)/sum(enc.urgent_care) as avg_cost_urgent_care
from member_year my 
left join enc on my.year_nbr = enc.year_nbr
and
enc.data_source = my.data_source
and
enc.person_id = my.person_id
group by my.data_source
,my.year_nbr
```
</details>



### Urgent Care and ED Comparison

<details>
  <summary>ED and Urgent Care Visits by Year</summary>

```sql

with uc as 
(
select mc.person_id
,to_char(claim_start_date, 'yyyy') as year_nbr
,mc.data_source
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as visits
,sum(mc.paid_amount) as paid_amount
from core.medical_claim mc
where service_category_2 = 'Urgent Care'
group by mc.person_id
,mc.data_source
,to_char(claim_start_date, 'yyyy')
)

,ed as (
select e.person_id
,to_char(encounter_start_date, 'yyyy') as year_nbr
,data_source
,count(distinct e.encounter_id) as visits
,sum(e.paid_amount) as paid_amount
from core.encounter e 
group by e.person_id
,data_source
,to_char(encounter_start_date, 'yyyy')
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(uc.visits) urgent_care_visits
,sum(ed.visits) ed_visits
from member_year my 
left join ed on my.year_nbr = ed.year_nbr
and
ed.data_source = my.data_source
and
ed.person_id = my.person_id
left join uc on my.year_nbr = uc.year_nbr
and
uc.data_source = my.data_source
and
uc.person_id = my.person_id
group by my.data_source
,my.year_nbr
```
</details>

<details>
  <summary>ED and Urgent Care Utilization by Member</summary>

```sql

with uc as 
(
select mc.person_id
,mc.data_source
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as visits
,sum(mc.paid_amount) as paid_amount
from core.medical_claim mc
where service_category_2 = 'Urgent Care'
group by mc.person_id
,mc.data_source
)

,ed as (
select e.person_id
,data_source
,count(distinct e.encounter_id) as visits
,sum(e.paid_amount) as paid_amount
from core.encounter e 
group by e.person_id
,data_source
)

,member_year as (
select distinct data_source
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.person_id
,coalesce(uc.visits,0) as urgent_care_visits
,coalesce(ed.visits,0) as ed_visits
,cast(coalesce(uc.paid_amount,0) as decimal(18,2)) as urgent_care_paid_amount
,cast(coalesce(ed.paid_amount,0) as decimal(18,2)) as ed_paid_amount
from member_year my 
left join ed on ed.data_source = my.data_source
and
ed.person_id = my.person_id
left join uc on uc.data_source = my.data_source
and
uc.person_id = my.person_id
where uc.person_id is not null
OR
ed.person_id is not null
order by ed_visits desc

```
</details>

<details>
  <summary>Members with an ED Visit and no Urgent Care</summary>

```sql

with uc as 
(
select mc.person_id
,to_char(claim_start_date, 'yyyy') as year_nbr
,mc.data_source
,count(distinct concat(mc.person_id,mc.data_source,claim_start_date)) as visits
,sum(mc.paid_amount) as paid_amount
from core.medical_claim mc
where service_category_2 = 'Urgent Care'
group by mc.person_id
,mc.data_source
,to_char(claim_start_date, 'yyyy')
)

,ed as (
select e.person_id
,to_char(encounter_start_date, 'yyyy') as year_nbr
,data_source
,count(distinct e.encounter_id) as visits
,sum(e.paid_amount) as paid_amount
from core.encounter e 
group by e.person_id
,data_source
,to_char(encounter_start_date, 'yyyy')
)

,member_year as (
select distinct data_source
,left(year_month,4) as year_nbr
,person_id
from financial_pmpm.pmpm_prep pmpm
)

select my.data_source
,my.year_nbr
,sum(case when uc.visits >= 1 then 1 else 0 end) members_with_at_least_one_uc
,sum(case when ed.visits >= 1 then 1 else 0 end) members_with_at_least_one_ed
,sum(case when ed.visits >= 1 and coalesce(uc.visits,0) = 0  then 1 else 0 end) members_with_ed_no_uc
from member_year my 
left join ed on my.year_nbr = ed.year_nbr
and
ed.data_source = my.data_source
and
ed.person_id = my.person_id
left join uc on my.year_nbr = uc.year_nbr
and
uc.data_source = my.data_source
and
uc.person_id = my.person_id
group by my.data_source
,my.year_nbr
```
</details>
