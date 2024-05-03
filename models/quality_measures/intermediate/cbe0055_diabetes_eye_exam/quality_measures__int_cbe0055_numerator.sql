{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_cbe0055__denominator') }}

)

, retina_test_code as (

    select
          code
        , code_system
        , concept_name
    From {{ref('quality_measures__value_sets')}}
    where lower(concept_name) in  (
          'diabetic retinopathy'
        , 'ophthalmological services'
        , 'diabetic retinal eye exam met'
        , 'retinal or dilated eye exam'
    )

)

, procedures as (

    select
        patient_id
      , procedure_date
      , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, qualifying_patients_for_procedure as (

    select 
          procedures.patient_id
        , procedure_date
        , procedures.code_type
        , procedures.code
    from procedures
    inner join {{ ref('quality_measures__int_cbe0055__performance_period') }} pp
    on procedure_date between
        performance_period_begin and performance_period_end

    inner join retina_test_code
        on procedures.code = retina_test_code.code
            and procedures.code_type = retina_test_code.code_system

)

, conditions as (

    select
          patient_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__condition') }}

)

, retinopathy_conditions as (

    select
        conditions.patient_id
      , conditions.recorded_date
      , conditions.code
      , conditions.code_type
    from conditions
    inner join retina_test_code
    on conditions.code = retina_test_code.code
    and conditions.code_type = retina_test_code.code_system
    and lower(retina_test_code.concept_name) = 'diabetic retinopathy'
)

, retinopathy_last_year as (

    select 
          denominator.patient_id
        , retinopathy_conditions.recorded_date
        , retinopathy_conditions.code_type
        , retinopathy_conditions.code
        , case
          when
              retinopathy_conditions.recorded_date between
                {{ dbt.dateadd (
                datepart = "year"
                , interval = -1
                , from_date_or_timestamp = "denominator.performance_period_begin"
                )
                }}
                and
                denominator.performance_period_begin
          then
              1
          else
              0
          end as retinopathy_last_year_flag
    from denominator
    left join retinopathy_conditions
    on denominator.patient_id = retinopathy_conditions.patient_id
    left join retina_test_code
    on retinopathy_conditions.code = retina_test_code.code
        and retinopathy_conditions.code_type = retina_test_code.code_system

)

, no_retinopathy_last_year as (

    select 
        retinopathy_last_year.patient_id
      , cast(null as date) as recorded_date
    from retinopathy_last_year
    where retinopathy_last_year_flag = 0

)

, qualifying_patients as (

    select 
          qualifying_patients_for_procedure.patient_id
        , qualifying_patients_for_procedure.procedure_date as evidence_date
    from qualifying_patients_for_procedure

    union all

    select 
          no_retinopathy_last_year.patient_id
        , no_retinopathy_last_year.recorded_date as evidence_date  
    from no_retinopathy_last_year

)

, qualifying_patients_with_denominator as (

    select 
          qualifying_patients.patient_id
        , qualifying_patients.evidence_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , '1' as numerator_flag
    from qualifying_patients
    inner join denominator
    on qualifying_patients.patient_id = denominator.patient_id

)

, add_data_types as (

     select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
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
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
from add_data_types