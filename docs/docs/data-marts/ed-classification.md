---
id: ed-classification
title: "ED Classification"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/ed_classification)

The ED Classification data mart implements the 2017 update to the original [NYU ED Classification Algorithm](https://wagner.nyu.edu/faculty/billings/nyued-background) to identify potentially preventable ED visits.

This mart builds off of the Core Encounter table and classifies emergency department encounters into the following categories:

|Classification| Description    |
|----|----------------|
|alcohol|Alcohol Related|
|drug|Drug Related (excluding alcohol)|
|emergent_ed_not_preventable|Emergent, ED Care Needed, Not Preventable/Avoidable|
|emergent_ed_preventable|Emergent, ED Care Needed, Preventable/Avoidable|
|emergent_primary_care|Emergent, Primary Care Treatable|
|injury|Injury|
|mental_health|Mental Health Related|
|non_emergent|Non-Emergent|
|unclassified|Not in a Special Category, and Not Classified|

### ED Visits Trended
<details>
  <summary>Trending ED Visit Volume, PKPY, and Cost</summary>

```sql
with ed as (
    select
          data_source
        , TO_CHAR(encounter_end_date, 'YYYYMM') AS year_month
        , COUNT(*) AS ed_visits
        , AVG(paid_amount) as avg_paid_amount
        , sum(paid_amount) as total_paid_amount
    from core.encounter
    where encounter_type = 'emergency department'
    group by
          data_source
        , TO_CHAR(encounter_end_date, 'YYYYMM')
)

, member_months as (
    select
          data_source
        , year_month
        , count(1) as member_months
    from financial_pmpm.member_months
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
order by
      a.data_source
    , a.year_month;
```
</details>

<details>
  <summary>ED Spend as Percent of Total Spend</summary>

```sql
select
      data_source
    , year_month
    , sum(emergency_department_paid) as ed_paid
    , sum(total_paid) as total_paid
    , cast(sum(emergency_department_paid) as decimal(18,2))/cast(sum(total_paid) as decimal(18,2)) as ed_percent_of_total_paid
from financial_pmpm.pmpm_prep
group by
      data_source
    , year_month
order by
      data_source
    , year_month;
```
</details>

<details>
  <summary>ED Visits by Member and Year</summary>

```sql
select
      data_source
    , year_month
    , sum(emergency_department_paid) as ed_paid
    , sum(total_paid) as total_paid
    , cast(sum(emergency_department_paid) as decimal(18,2))/cast(sum(total_paid) as decimal(18,2)) as ed_percent_of_total_paid
from financial_pmpm.pmpm_prep
group by
     data_source
    , year_month
order by
      data_source
    , year_month;
```
</details>

<details>
  <summary>Frequency Distribution of ED Visits</summary>

```sql
with visits as (
    select
          data_source
        , person_id
        , count(*) as ed_visits
    from core.encounter
    where encounter_type = 'emergency department'
    group by
          data_source
        , person_id
)

, members as (
    select distinct
          person_id
        , data_source
    from financial_pmpm.member_months
)

, members_total as (
    select count(*) as total_member_count
    from members
)

, members_with_visits as (
    select
          m.person_id
        , m.data_source
        , coalesce(v.ed_visits,0) as ed_visits
    from members m
        left join visits v
            on m.person_id = v.person_id
            and m.data_source = v.data_source
)

select
      ed_visits
    , count(*) as member_count
    , count(*) / cast(max(total_member_count) as real) as percent_of_total_members
from members_with_visits
cross join members_total
group by ed_visits
order by ed_visits;
```
</details>

<details>
  <summary>Count of ED NPIs</summary>

```sql
select 
      data_source
    , count(distinct facility_id) as ed_facilities_count
from core.encounter e
where encounter_type = 'emergency department'
group by data_source
order by ed_facilities_count desc;
```
</details>

<details>
  <summary>Visit by Facility</summary>

```sql
select
     facility_id
    , count(*) AS ed_visits
    , sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
    , cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e
where encounter_type = 'emergency department'
group by facility_id
order by ed_visits desc;
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
order by ed_visits desc;
```
</details>

### ED Diagnosis Grouping
The Tuva Project provides several ways of grouping diagnosis codes.

CCSR provides a hierarchy grouping of diagnosis codes, and is useful for 
recognizing patterns of care by what the patient was diagnosed with at the ED.

Chronic Conditions are a way of grouping members by conditions that they have 
been diagnosed with (within the relevant timespan, usually the last one or 
two years.)

<details>
  <summary>ED Visits by CCSR Category and Body System</summary>

```sql
select
      p.ccsr_category
    , p.ccsr_category_description
    , p.ccsr_parent_category
    , p.body_system
    , count(*) as visit_count
    , sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
    , cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e
    left join ccsr.long_condition_category p
        on e.primary_diagnosis_code = p.normalized_code
        and p.condition_rank = 1
where e.encounter_type = 'emergency department'
group by
      p.ccsr_category
    , p.ccsr_category_description
    , p.ccsr_parent_category
    , p.body_system
order by visit_count desc;
```
</details>

<details>
  <summary>ED Visits by Chronic Condition Category</summary>

Since members often have more than one chronic condition, encounters are duplicated for each chronic condition causing the total amount to be inflated. The division of encounters by chronic condition is useful for comparision across disease states, and less so from the total standpoint.

```sql
with chronic_condition_members as (
    select distinct
    person_id
    from chronic_conditions.tuva_chronic_conditions_long
)

, chronic_conditions as (
    select person_id
    , condition
    from chronic_conditions.tuva_chronic_conditions_long

    union

    select p.person_id
    , 'No Chronic Conditions' as condition
    from core.patient p
        left join chronic_condition_members ccm
            on p.person_id=ccm.person_id
    where ccm.person_id is null
)

select
      cc.condition
    , count(*) as visit_count
    , sum(cast(e.paid_amount as decimal(18,2))) as paid_amount
    , cast(sum(e.paid_amount)/count(*) as decimal(18,2))as paid_per_visit
from core.encounter e
    left join chronic_conditions cc
        on e.person_id = cc.person_id
where encounter_type = 'emergency department'
group by cc.condition
order by visit_count desc;
```
</details>
