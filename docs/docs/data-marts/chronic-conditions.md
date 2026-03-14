---
id: chronic-conditions
title: "Chronic Conditions"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/chronic_conditions)

The Chronic Conditions data mart implements two different chronic condition groupers: one defined by [CMS](https://www2.ccwdata.org/web/guest/condition-categories-chronic) and the other defined by Tuva.  We started defining chronic conditions in Tuva after struggling to use the CMS logic, either because certain chronic conditions were missing (e.g. non-alcoholic fatty liver disease, MASH, etc.) or because existing definitions were unsatisfactory (e.g. type 1 and type 2 diabetes are considered the same condition by CMS) even though the pathology of the two is distinctly different.

You can find the methods for CMS's methodology using the above link.  You can search exact codes used in the Tuva definition in the clinical concept library in our value sets.

## Example SQL

<details>
  <summary>Prevalence of Tuva Chronic Conditions</summary>

In this query we show how often each chronic condition occurs in the patient population.

```sql
select
  condition
, count(distinct person_id) as total_patients
, cast(count(distinct person_id) * 100.0 / (select count(distinct person_id) from core.patient) as numeric(38,2)) as percent_of_patients
from chronic_conditions.tuva_chronic_conditions_long
group by 1
order by 3 desc
```

</details>

<details>
  <summary>Prevalence of CMS Chronic Conditions</summary>

In this query we show how often each chronic condition occurs in the patient population.

```sql
select
  condition_category
, condition
, count(distinct person_id) as total_patients
, cast(count(distinct person_id) * 100.0 / (select count(distinct person_id) from core.patient) as numeric(38,2)) as percent_of_patients
from chronic_conditions.cms_chronic_conditions_long
group by 1,2
order by 4 desc
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