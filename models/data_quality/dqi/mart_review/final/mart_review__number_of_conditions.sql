{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

with xwalk as (
    select distinct person_id, data_source
    from {{ ref('core__patient')}}
),
cte as (
    select l.person_id,
           x.data_source,
           count(*) as numofconditions
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} l
    left join xwalk x on l.person_id = x.person_id
    group by l.person_id, x.data_source
)
select p.person_id,
       p.data_source,
        {{  concat_custom([
            'p.person_id',
            "'|'",
            'p.data_source']) }} as patient_source_key,
       coalesce(cte.numofconditions, 0) as numofconditions
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__patient')}} p
left join cte on p.person_id = cte.person_id and p.data_source = cte.data_source
