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
    from {{ ref('quality_measures__int_nqf0059_denominator') }}

)

, hba1c_test_code as (

    select
          code
        , code_system
        , concept_name
    From {{ref('quality_measures__value_sets')}}
    where lower(concept_name) in  (
        'hba1c laboratory test'
    )
)

, labs as (
    select
        patient_id
        , result
        , result_date
        , collection_date
        , source_code_type
        , source_code
        , normalized_code_type
        , normalized_code
    from {{ ref('quality_measures__stg_core__lab_result')}}

)

, qualifying_labs as (
    select
      labs.patient_id
    , labs.result as evidence_value
    , coalesce(collection_date,result_date) as evidence_date
    , hba1c_test_code.concept_name
    , row_number() over(partition by labs.patient_id order by coalesce(collection_date,result_date) desc) as rn
    from labs
    inner join hba1c_test_code
      on ( labs.normalized_code = hba1c_test_code.code
       and labs.normalized_code_type = hba1c_test_code.code_system )
      or ( labs.source_code = hba1c_test_code.code
       and labs.source_code_type = hba1c_test_code.code_system )
    left join denominator
        on labs.patient_id = denominator.patient_id
    where coalesce(collection_date,result_date) <= denominator.performance_period_end
        and {{ apply_regex('labs.result', '[+-]?([0-9]*[.])?[0-9]+') }}

)

, recent_readings as (
    select
          patient_id
        , evidence_date
        , evidence_value
    from qualifying_labs
    where rn = 1 

)

, qualifying_patients as (

    select
          denominator.*
        , recent_readings.evidence_date
        , recent_readings.evidence_value
    from denominator
    left join recent_readings
        on denominator.patient_id = recent_readings.patient_id

)

, test_not_performed as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , evidence_value
        , 1 as numerator_flag
    from qualifying_patients
    where 
        (evidence_date not between performance_period_begin and performance_period_end)
        or evidence_date is null

)

, valid_patients as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , evidence_value
        , case
            when cast(evidence_value as {{ dbt.type_numeric() }}) > 9.0 then 1
            else 0
          end as numerator_flag
    from qualifying_patients
    where evidence_date between performance_period_begin and performance_period_end

)

, numerator as (

    select * from valid_patients

    union all
        
    select * from test_not_performed

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
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

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
