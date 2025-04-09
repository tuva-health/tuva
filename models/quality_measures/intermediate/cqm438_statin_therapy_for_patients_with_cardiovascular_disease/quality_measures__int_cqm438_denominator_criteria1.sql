{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with ascvd_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
              'atherosclerosis and peripheral arterial disease'
            , 'myocardial infarction'
            , 'pci'
            , 'stable and unstable angina'
            , 'cabg or pci procedure'
            , 'cabg surgeries'
            , 'cerebrovascular disease stroke or tia'
            , 'ischemic heart disease or related diagnoses'
            , 'carotid intervention'
        )

)

, conditions as (

    select
          person_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition') }}

)

, ascvd_conditions as (

    select
          conditions.person_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join ascvd_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = ascvd_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = ascvd_codes.code

)

, procedures as (

    select
          person_id
        , procedure_date
        , coalesce(
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, ascvd_procedures as (

    select
          procedures.person_id
        , procedures.procedure_date as evidence_date
    from procedures
         inner join ascvd_codes
             on procedures.code = ascvd_codes.code
             and procedures.code_type = ascvd_codes.code_system

)

, historical_ascvd as (

    select
          ascvd_conditions.person_id
        , ascvd_conditions.evidence_date
    from ascvd_conditions

    union all

    select
          ascvd_procedures.person_id
        , ascvd_procedures.evidence_date
    from ascvd_procedures

)

, patients_with_ascvd as (

    select
        distinct
          historical_ascvd.person_id
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
    from historical_ascvd
    inner join {{ ref('quality_measures__int_cqm438__performance_period') }} as pp
    on evidence_date <= pp.performance_period_end

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
    from patients_with_ascvd

)

select
      person_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
