{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
) }}

with member_months as (
    select 
        count(1) as member_months 
    from {{ ref('core__member_months') }}
)

select
    enc.encounter_group
    , enc.encounter_type
    , count(enc.encounter_id) / avg(mm.member_months) * 12000 as pkpy
    , sum(enc.paid_amount) / count(enc.encounter_id) as paid_per
from {{ ref('core__encounter') }} as enc
cross join member_months as mm
group by
    enc.encounter_group
    , enc.encounter_type