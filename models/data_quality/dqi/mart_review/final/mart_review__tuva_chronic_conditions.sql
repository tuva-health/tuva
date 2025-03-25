{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

with cte as (
    select distinct
        person_id
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
)

, patientxwalk as (
    select distinct
        person_id
      , data_source
    from {{ ref('core__patient') }}
)

, result as (
    select
        l.person_id
      , p.data_source
      , l.condition
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} as l
    inner join patientxwalk as p
      on l.person_id = p.person_id

    union all

    select
        p.person_id
      , p.data_source
      , 'No Chronic Conditions' as condition
    from {{ ref('core__patient') }} as p
    left outer join cte
      on p.person_id = cte.person_id
    where cte.person_id is null
)

select *
   , {{ concat_custom([
        'person_id',
        "'|'",
        'data_source']) }} as patient_source_key
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from result
