{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{%- set performance_period_end -%}
(

    select 
        performance_period_end
    from {{ ref('quality_measures__int_nqf0059__performance_period') }}

)
{%- endset -%}

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
    where concept_name in  (
        'HbA1c Laboratory Test'
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
    , labs.result
    , coalesce(collection_date,result_date) as evidence_date
    , hba1c_test_code.concept_name
    from labs
    inner join hba1c_test_code
      on ( labs.normalized_code = hba1c_test_code.code
       and labs.normalized_code_type = hba1c_test_code.code_system )
      or ( labs.source_code = hba1c_test_code.code
       and labs.source_code_type = hba1c_test_code.code_system )
    where coalesce(collection_date,result_date) < {{ performance_period_end }}

)

, recent_readings as (
    select
          patient_id
        , evidence_date
        , result
    from qualifying_labs
    qualify row_number() over(partition by patient_id order by evidence_date desc) = 1

)

, qualifying_patients as (

    select
          denominator.*
        , recent_readings.evidence_date
        , recent_readings.result
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
        , 1 as numerator_flag
    from qualifying_patients
    where 
        (evidence_date not between performance_period_begin and performance_period_end)
        or evidence_date is null

)

, valid_patients as (

    select
        *
    from qualifying_patients
    where evidence_date between performance_period_begin and performance_period_end

)

, readings_exceeding_9 as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , 1 as numerator_flag
    from valid_patients
    where result > 9.0

)

, readings_less_than_7 as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , 0 as numerator_flag
    from valid_patients
    where result < 7.0

)

, readings_between_7_and_8 as (

    select
         patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , 0 as numerator_flag
    from valid_patients
    where result between 7.0 and 8.0

)
, readings_between_8_and_9 as (

    select
         patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , 0 as numerator_flag
    from valid_patients
    where result between 8.0 and 9.0

)

, numerator as (

    select * from readings_less_than_7

    union all

    select * from readings_between_7_and_8

    union all

    select * from readings_between_8_and_9

    union all

    select * from readings_exceeding_9

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
    , numerator_flag
from add_data_types
