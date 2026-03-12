---
id: financial-pmpm
title: "Financial PMPM"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/financial_pmpm)

The Financial PMPM data mart computes member months and stratifies population paid and allowed amounts by member months and service categories across various payers and plans.

## Example SQL

<details>
  <summary>Calculate Member Months and Total Medical Spend</summary>

```sql
select
      data_source
    , year_month
    , cast(sum(medical_paid) as decimal(18,2)) as medical_paid
    , count(*) as member_months
    , cast(sum(medical_paid)/count(*) as decimal(18,2)) as pmpm
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
  <summary>Trending PMPM by Service Category</summary>

The pmpm table already breaks out pmpm by service category and groups it at the member month level.

```sql
select *
from financial_pmpm.pmpm_payer
order by year_month;
```
</details>

<details>
  <summary>Trending PMPM by Claim Type</summary>

Here we calculate PMPM manually by counting member months and joining payments by claim type to them.

```sql
with member_month as (
    select
          data_source
        , year_month
        , count(*) as member_months
    from core.member_months
    group by
          data_source
        , year_month
)

, medical_claims as (
    select
          mc.data_source
        , to_char(mc.claim_start_date, 'yyyymm') as year_month
        , mc.claim_type
        , cast(sum(mc.paid_amount) as decimal(18, 2)) as paid_amount
    from core.medical_claim as mc
    inner join core.member_months as mm
      on mc.person_id = mm.person_id
      and mc.data_source = mm.data_source
      and to_char(mc.claim_start_date, 'yyyymm') = mm.year_month
    group by
          mc.data_source
        , to_char(mc.claim_start_date, 'yyyymm')
        , mc.claim_type
)

select
      mm.data_source
    , mm.year_month
    , mc.claim_type
    , mc.paid_amount
    , mm.member_months
    , cast(mc.paid_amount / mm.member_months as decimal(18, 2)) as pmpm_claim_type
from member_month as mm
left join medical_claims as mc
  on mm.data_source = mc.data_source
  and mm.year_month = mc.year_month
order by
      mm.data_source
    , mm.year_month
    , mc.claim_type;

```
</details>

<details>
  <summary>PMPM by Chronic Condition</summary>

Here we calculate PMPM by chronic condition. Since members can and do have more than one chronic condition, payments and members months are duplicated. This is useful for comparing spend across chronic conditions, but should be used with caution given the duplication across conditions.

```sql
with chronic_condition_members as (
    select distinct
        person_id
    from chronic_conditions.tuva_chronic_conditions_long
)

, chronic_conditions as (
    select
          person_id
        , condition
    from chronic_conditions.tuva_chronic_conditions_long

    union

    select
          p.person_id
        , 'No Chronic Conditions' as condition
    from core.patient as p
    left join chronic_condition_members as ccm
      on p.person_id = ccm.person_id
    where ccm.person_id is null
)

, medical_claims as (
    select
          mc.data_source
        , mc.person_id
        , to_char(mc.claim_start_date, 'yyyymm') as year_month
        , cast(sum(mc.paid_amount) as decimal(18, 2)) as paid_amount
    from core.medical_claim as mc
    inner join core.member_months as mm
      on mc.person_id = mm.person_id
      and mc.data_source = mm.data_source
      and to_char(mc.claim_start_date, 'yyyymm') = mm.year_month
    group by
          mc.data_source
        , mc.person_id
        , to_char(mc.claim_start_date, 'yyyymm')
)

select
      mm.data_source
    , cc.condition
    , count(*) as member_months
    , sum(mc.paid_amount) as paid_amount
    , cast(sum(mc.paid_amount) / count(*) as decimal(18, 2)) as medical_pmpm
from core.member_months as mm
left join chronic_conditions as cc
  on mm.person_id = cc.person_id
left join medical_claims as mc
  on mm.person_id = mc.person_id
  and mm.year_month = mc.year_month
  and mm.data_source = mc.data_source
group by
      mm.data_source
    , cc.condition
order by
    member_months desc;

```
</details>