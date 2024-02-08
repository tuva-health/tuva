{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__stg_core__condition') }}

)

/*
    Default mapping guidance: Most map groups terminate with an unconditional
    rule – a rule whose predicate is “TRUE” or, equivalently, “OTHERWISE TRUE”.
    This rule is considered a “default” because it should be applied if
    nothing further is known about the patient’s condition.
*/
, seed_snomed_icd_10_map as (

    select
          referenced_component_id as snomed_code
        , map_target as icd_10_code
    from {{ ref('terminology__snomed_icd_10_map') }}
    where lower(map_rule) in ('true', 'otherwise true')
    and map_group = '1'

)

, snomed_conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , 'icd-10-cm' as code_type
        , icd_10_code as code
        , data_source
    from conditions
         inner join seed_snomed_icd_10_map
         on conditions.code = seed_snomed_icd_10_map.snomed_code
    where conditions.code_type = 'snomed-ct'

)

, other_conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from conditions
    where conditions.code_type <> 'snomed-ct'

)

, union_conditions as (

    select * from snomed_conditions
    union all
    select * from other_conditions

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(code_type as {{ dbt.type_string() }}) as code_type
        , cast(code as {{ dbt.type_string() }}) as code
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from union_conditions

)

select
      patient_id
    , recorded_date
    , condition_type
    , code_type
    , code
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types