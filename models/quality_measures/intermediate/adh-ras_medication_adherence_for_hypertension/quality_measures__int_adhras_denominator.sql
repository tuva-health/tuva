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

, pharmacy_claim as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , days_supply
    from {{ ref('quality_measures__stg_pharmacy_claim') }}

)

, patient_with_claim as (

    select
          pharmacy_claim.person_id
        , dispensing_date
        , days_supply
        , ndc_code
    from pharmacy_claim
    inner join visit_codes
        on pharmacy_claim.ndc_code = visit_codes.code

)

, patient_within_performance_period as (

    select
          person_id
        , dispensing_date
        , days_supply
        , ndc_code
        , performance_period_begin
        , performance_period_end
    from patient_with_claim as claim_patient
    inner join performance_period as pp
        on claim_patient.dispensing_date between pp.performance_period_begin and pp.performance_period_end

)

/*
    These patients need to pass two checks
    - First medication fill date should be at least 91 days before the end of measurement period
    - Should have at least two distinct Date of Service (FillDate) for rx
*/

, patient_with_rank as (

    select
          person_id
        , dispensing_date
        , days_supply
        , ndc_code
        , dense_rank() over (partition by person_id
order by dispensing_date) as dense_rank
    from patient_within_performance_period

)

, patient_with_first_dispensing_date as (

    select
          person_id
        , dispensing_date as first_dispensing_date
    from patient_with_rank
    where dense_rank = 1

)

/*
treatment period days is abbreviated as tpd
*/

, patient_with_tpd as (

    select
          patients1.person_id
        , patients1.dispensing_date
        , patients2.first_dispensing_date
        , patients1.days_supply
        , {{ datediff('first_dispensing_date', 'performance_period_end', 'day') }} as tpd
    from patient_within_performance_period as patients1
    inner join patient_with_first_dispensing_date as patients2
        on patients1.person_id = patients2.person_id

)

, first_check_patient as (

    select
          person_id
        , dispensing_date
        , first_dispensing_date
        , days_supply
    from patient_with_tpd
    where tpd > 89

)

, second_check_patient as (

    select
          person_id
        , ndc_code
    from patient_with_rank
    where dense_rank = 2

)

, both_check_patient as (

    select
          valid_patients1.person_id
        , valid_patients1.dispensing_date
        , valid_patients1.first_dispensing_date
        , valid_patients1.days_supply
        , valid_patients2.ndc_code
    from first_check_patient as valid_patients1
    inner join second_check_patient as valid_patients2
        on valid_patients1.person_id = valid_patients2.person_id

)

, patient_with_age as (

    select
          valid_patients1.person_id
        , floor({{ datediff('birth_date', 'pp.performance_period_begin', 'hour') }} / 8760.0) as age
        , dispensing_date
        , first_dispensing_date
        , days_supply
        , ndc_code
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
    from {{ ref('quality_measures__stg_core__patient') }} as patient
    inner join both_check_patient as valid_patients1
        on patient.person_id = valid_patients1.person_id
    cross join performance_period as pp
    where patient.death_date is null

)

, qualifying_patients as (

    select
          person_id
        , dispensing_date
        , first_dispensing_date
        , days_supply
        , ndc_code
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , 1 as denominator_flag
    from patient_with_age
    where age > 17

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(dispensing_date as date) as dispensing_date
        , cast(first_dispensing_date as date) as first_dispensing_date
        , cast(days_supply as integer) as days_supply
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select
      person_id
    , dispensing_date
    , first_dispensing_date
    , days_supply
    , ndc_code
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
