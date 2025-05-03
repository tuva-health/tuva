{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with diabetics_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'pqa diabetes medications'
        )

)

, rx_diabetes as (

    select
        person_id
      , dispensing_date
      , ndc_code
      , days_supply
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claims
    inner join diabetics_codes
        on pharmacy_claims.ndc_code = diabetics_codes.code
            and lower(diabetics_codes.code_system) = 'ndc'

)

, rx_diabetes_in_measurement_period as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , days_supply
        , performance_period_end -- to use in latter cte for days in treatment period calculation
    from rx_diabetes
    inner join {{ ref('quality_measures__int_adh_diabetes__performance_period') }} as pp
        on dispensing_date between pp.performance_period_begin and pp.performance_period_end

)

/*
    These patients need to pass two checks
    - First medication fill date should be at least 91 days before the end of measurement period
    - Should have at least two distinct Date of Service (FillDate) for rx
*/

, rx_fill_order as (

    select
          person_id
        , dispensing_date
        , performance_period_end
        , dense_rank() over (
            partition by
                  person_id
                , performance_period_end
            order by dispensing_date
        ) as dr
    from rx_diabetes_in_measurement_period

)

, rx_first_fill as (

    select
          person_id
        , dispensing_date
        , performance_period_end
    from rx_fill_order
    where dr = 1

)

, timely_fill_check as (

    select
          person_id
        , (1 + {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = 'dispensing_date'
                                , second_date = 'performance_period_end'
                            )
            }})
            as days_in_treatment_period
        /*
            Performance Period end minus dispensing date results in
            second_date non-inclusive difference, so to include both of these days
            1 day is added
        */
    from rx_first_fill

)

, first_check_passed_patients as (

    select
          person_id
        , days_in_treatment_period
    from timely_fill_check
    where days_in_treatment_period > 90

)

, second_check_passed_patients as (

    select
          person_id
    from rx_fill_order
    where dr = 2

)

, qualifying_patients as (

    select
          first_check_passed_patients.person_id
        , first_check_passed_patients.days_in_treatment_period
    from first_check_passed_patients
    inner join second_check_passed_patients
        on first_check_passed_patients.person_id = second_check_passed_patients.person_id

)

, qualifying_patients_with_age as (

    select
          patients.person_id
        , floor({{ datediff('birth_date', 'pp.performance_period_begin', 'hour') }} / 8760.0) as age
        , days_in_treatment_period
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__stg_core__patient') }} as patients
    inner join qualifying_patients
        on patients.person_id = qualifying_patients.person_id
    cross join {{ ref('quality_measures__int_adh_diabetes__performance_period') }} as pp
    where patients.death_date is null

)
/*
    Extracting related fields like dispensing_date, ndc_code and days_supply of qualified patients
    to avoid redundant computations in numerator model
*/

, qualifying_patients_all_claim_info as (

    select
          qualifying_patients_with_age.person_id
        , qualifying_patients_with_age.age
        , rx_diabetes_in_measurement_period.dispensing_date
        , rx_diabetes_in_measurement_period.ndc_code
        , rx_diabetes_in_measurement_period.days_supply
        , qualifying_patients_with_age.days_in_treatment_period
        , qualifying_patients_with_age.performance_period_begin
        , qualifying_patients_with_age.performance_period_end
        , qualifying_patients_with_age.measure_id
        , qualifying_patients_with_age.measure_name
        , qualifying_patients_with_age.measure_version
    from qualifying_patients_with_age
    inner join rx_diabetes_in_measurement_period
        on qualifying_patients_with_age.person_id = rx_diabetes_in_measurement_period.person_id

)

, denominator as (

    select
          person_id
        , age
        , dispensing_date
        , ndc_code
        , days_supply
        , days_in_treatment_period
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , 1 as denominator_flag
    from qualifying_patients_all_claim_info
    where age >= 18

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(age as integer) as age
        , cast(dispensing_date as date) as dispensing_date
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(days_supply as integer) as days_supply
        , cast(days_in_treatment_period as integer) as days_in_treatment_period
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from denominator

)

select
      person_id
    , age
    , dispensing_date
    , ndc_code
    , days_supply
    , days_in_treatment_period
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
