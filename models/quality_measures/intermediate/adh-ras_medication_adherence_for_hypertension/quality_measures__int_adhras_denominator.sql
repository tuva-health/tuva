{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
     | as_bool
   )
}}

with performance_period as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_end
        , performance_period_begin
    from {{ ref('quality_measures__int_adhras__performance_period') }}

)

, visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) = 'pqa rasa medications'

)

, pharmacy_claim  as (
    select 
          patient_id
        , dispensing_date
        , ndc_code
        , days_supply
    from {{ ref('quality_measures__stg_pharmacy_claim') }}
)

, patient as (

    select
          patient_id
        , sex
        , birth_date
        , death_date
    from {{ ref('quality_measures__stg_core__patient') }}

)

, patient_with_claim as (

    select
          pc.patient_id
        , dispensing_date
        , days_supply
        , ndc_code
    from pharmacy_claim as pc
    inner join visit_codes as vc
    on pc.ndc_code = vc.code
    
)

, patient_within_performance_period as (

    select
          patient_id
        , dispensing_date
        , days_supply
        , performance_period_begin
        , performance_period_end
    from patient_with_claim as pc
    inner join performance_period as pp
    on pc.dispensing_date between pp.performance_period_begin and pp.performance_period_end

)

, patient_with_row_number as (

    select
          patient_id
        , dispensing_date
        , days_supply
        , row_number() over (partition by patient_id order by dispensing_date) as row_number
    from patient_within_performance_period

)

, patient_with_first_dispensing_date as (

    select distinct
          patient_id
        , dispensing_date as first_dispensing_date
    from patient_with_row_number
    where row_number = 1

)

, patient_with_first_fill_greater_than_90 as (

    select
          pp.patient_id
        , pp.dispensing_date
        , fdd.first_dispensing_date
        , pp.days_supply
    from patient_within_performance_period as pp
    inner join patient_with_first_dispensing_date as fdd
    on pp.patient_id = fdd.patient_id
    where {{ datediff('first_dispensing_date', 'performance_period_end', 'day') }} > 89

)


, patient_with_more_than_1_prescriptions as (

    select distinct
      patient_id
      from patient_with_row_number
      where row_number = 2

)

, patient_with_mt_1_prescriptions_and_first_fill_gt_90 as (

    select
          ff.patient_id
        , ff.dispensing_date
        , ff.first_dispensing_date
        , ff.days_supply
    from patient_with_first_fill_greater_than_90 as ff
    inner join patient_with_more_than_1_prescriptions as mt1
    on ff.patient_id = mt1.patient_id
     
)

, patient_with_age as (

    select
          pp.patient_id
    from {{ref('quality_measures__stg_core__patient')}} as p
    inner join patient_within_performance_period as pp
    on p.patient_id = pp.patient_id
    where (floor({{ datediff('birth_date', 'pp.performance_period_begin', 'hour') }} / 8760.0)) > 17

)

, qualifying_patients as (

    select
          pp90.patient_id
        , pp90.dispensing_date
        , pp90.first_dispensing_date
        , pp90.days_supply
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from patient_with_mt_1_prescriptions_and_first_fill_gt_90 as pp90
    inner join patient_with_age as pa
    on pp90.patient_id = pa.patient_id
    cross join performance_period as pp

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(dispensing_date as date) as dispensing_date
        , cast(first_dispensing_date as date) as first_dispensing_date
        , cast(days_supply as integer) as days_supply
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      patient_id
    , dispensing_date
    , first_dispensing_date
    , days_supply
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types