{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

with chronic_conditions as (

    select * from {{ ref('chronic_conditions__cms_chronic_conditions_hierarchy') }}

),

patient_encounters as (

    select
          encounter.patient_id
        , encounter.encounter_id
        , encounter.encounter_start_date
        , encounter.ms_drg_code
        , encounter.data_source
        , replace(condition.code,'.','') as condition_code
        , condition.code_type as condition_code_type
        , replace(procedure.code,'.','') as procedure_code
        , procedure.code_type as procedure_code_type
    from {{ ref('core__encounter') }} as encounter
         left join {{ ref('core__condition') }} as condition
             on encounter.encounter_id = condition.encounter_id
         left join {{ ref('core__procedure') }}  as procedure
             on encounter.encounter_id = procedure.encounter_id

),

inclusions_diagnosis as (

    select
          patient_encounters.patient_id
        , patient_encounters.encounter_id
        , patient_encounters.encounter_start_date
        , patient_encounters.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_encounters
         inner join chronic_conditions
             on patient_encounters.condition_code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'ICD-10-CM'
    and chronic_conditions.additional_logic = 'None'

),

inclusions_procedure as (

    select
          patient_encounters.patient_id
        , patient_encounters.encounter_id
        , patient_encounters.encounter_start_date
        , patient_encounters.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_encounters
         inner join chronic_conditions
             on patient_encounters.procedure_code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system in ('ICD-10-PCS', 'HCPCS')
    and chronic_conditions.additional_logic = 'None'

),

inclusions_ms_drg as (

    select
          patient_encounters.patient_id
        , patient_encounters.encounter_id
        , patient_encounters.encounter_start_date
        , patient_encounters.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_encounters
         inner join chronic_conditions
             on patient_encounters.ms_drg_code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'MS-DRG'
    and chronic_conditions.additional_logic = 'None'

),

exclusions_diagnosis as (

    select distinct
          patient_encounters.encounter_id
        , chronic_conditions.condition
    from patient_encounters
         inner join chronic_conditions
             on patient_encounters.condition_code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Exclude'
    and chronic_conditions.code_system = 'ICD-10-CM'

),

inclusions_unioned as (

    select * from inclusions_diagnosis
    union distinct
    select * from inclusions_procedure
    union distinct
    select * from inclusions_ms_drg

)

select distinct
      cast(inclusions_unioned.patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(inclusions_unioned.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(inclusions_unioned.encounter_start_date as date)
      as encounter_start_date
    , cast(inclusions_unioned.chronic_condition_type as {{ dbt.type_string() }})
      as chronic_condition_type
    , cast(inclusions_unioned.condition_category as {{ dbt.type_string() }})
      as condition_category
    , cast(inclusions_unioned.condition as {{ dbt.type_string() }}) as condition
    , cast(inclusions_unioned.data_source as {{ dbt.type_string() }}) as data_source
from inclusions_unioned
     left join exclusions_diagnosis
         on inclusions_unioned.encounter_id = exclusions_diagnosis.encounter_id
         and inclusions_unioned.condition = exclusions_diagnosis.condition
where exclusions_diagnosis.encounter_id is null