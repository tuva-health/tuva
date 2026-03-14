---
id: ccsr
title: "CCSR"
---

## Methods
[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/ccsr)

The CCSR data mart implements [AHRQ's Clinical Classifications Software Refined](https://hcup-us.ahrq.gov/toolssoftware/ccsr/ccs_refined.jsp) diagnosis and procedure grouper.  This is a very commonly used tool to group ICD-10-CM and ICD-10-PCS diagnosis and procedure codes into higher-level categories.  Full documentation for this grouper can be found on AHRQ's website via the link above.

## Example SQL

<details>
  <summary>Condition Count by Body System</summary>

```sql
select
      body_system
    , count(*)
from ccsr.singular_condition_category
group by body_system
order by count(*) desc;
```
</details>

<details>
  <summary>Condition Count by CCSR Category</summary>

```sql
select
      ccsr_category_description
    , count(*)
from ccsr.singular_condition_category
group by ccsr_category_description
order by count(*) desc;
```
</details>

<details>
  <summary>Procedure Count by Clinical Domain</summary>

```sql
select
      clinical_domain
    , count(*)
from ccsr.long_procedure_category
group by clinical_domain
order by count(*) desc;
```
</details>

<details>
  <summary>Procedure Count by CCSR Category</summary>

```sql
select
      ccsr_category_description
    , count(*)
from ccsr.long_procedure_category
group by ccsr_category_description
order by count(*) desc;
```
</details>

<details>
  <summary>Acute Inpatient Visits by CCSR Category and Body System</summary>

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
where e.encounter_type = 'acute inpatient'
group by
      p.ccsr_category
    , p.ccsr_category_description
    , p.ccsr_parent_category
    , p.body_system
order by visit_count desc;
```
</details>

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
