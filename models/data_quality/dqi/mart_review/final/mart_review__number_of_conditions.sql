{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

with xwalk as (
    select distinct patient_id, data_source
    from {{ ref('core__patient')}}
),
cte as (
    select l.patient_id,
           x.data_source,
           count(*) as numofconditions
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} l
    left join xwalk x on l.patient_id = x.patient_id
    group by l.patient_id, x.data_source
)
select p.patient_id,
       p.data_source,
        {{  dbt.concat([
            'p.patient_id',
            "'|'",
            'p.data_source']) }} as patient_source_key,
       coalesce(cte.numofconditions, 0) as numofconditions
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__patient')}} p
left join cte on p.patient_id = cte.patient_id and p.data_source = cte.data_source
