{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , discharge_date
    from {{ ref('quality_measures__int_nqf0097_denominator') }}

)

, reconciliation_codes as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
        'medication reconciliation post discharge'
    )

)

, procedures as (

    select
          person_id
        , procedure_date
        , coalesce (
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

, reconciliation_procedures as (

    select
          procedures.person_id
        , procedures.procedure_date
    from procedures
    inner join reconciliation_codes
        on procedures.code = reconciliation_codes.code
            and procedures.code_type = reconciliation_codes.code_system

)

, qualifying_patients_with_denominator as (

    select
        denominator.person_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , reconciliation_procedures.procedure_date as evidence_date
        , 1 as numerator_flag
    from denominator
    inner join reconciliation_procedures
        on denominator.person_id = reconciliation_procedures.person_id
    where {{ datediff('denominator.discharge_date', 'reconciliation_procedures.procedure_date', 'day') }} between 0 and 30

)

, add_data_types as (

     select distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(null as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from qualifying_patients_with_denominator

)

select
      person_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
