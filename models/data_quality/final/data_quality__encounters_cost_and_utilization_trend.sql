{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
) }}

with member_months as (
    select 
        year_month,
        count(1) as member_months 
    from {{ ref('core__member_months') }}
    group by year_month
)
,encounters as (
    select
        to_char(encounter_start_date, 'YYYYMM') as year_month,
        encounter_group,
        encounter_type,
        encounter_id,
        paid_amount
    from {{ ref('core__encounter') }}
)
,pkpy_trend as (
    select
        enc.year_month,
        enc.encounter_group,
        enc.encounter_type,
        count(enc.encounter_id) / mm.member_months * 12000 as pkpy,
        sum(enc.paid_amount) / nullif(count(enc.encounter_id), 0) as paid_per
    from encounters as enc
    inner join member_months as mm on enc.year_month = mm.year_month
    group by
        enc.year_month,
        enc.encounter_group,
        enc.encounter_type,
        mm.member_months
)
select
    year_month,
    encounter_group,
    encounter_type,
    pkpy,
    paid_per
from pkpy_trend
order by year_month, encounter_group, encounter_type