{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

-- Staging model for the input layer:
-- stg_diagnosis input layer model.
-- This contains one row for every unique diagnosis each patient has.

select distinct
    cast(a.encounter_id as {{ dbt.type_string() }}) as encounter_id
,   cast(a.code as {{ dbt.type_string() }}) as diagnosis_code
,   cast(a.diagnosis_rank as integer) as diagnosis_rank
, '{{ var('last_update')}}' as last_update
from {{ ref('readmissions__stg_core__condition') }} a
inner join  {{ ref('readmissions__stg_acute_inpatient__summary') }} b
  on a.encounter_id = b.encounter_id
where code_type = 'icd-10-cm'
