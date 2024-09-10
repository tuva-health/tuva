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
