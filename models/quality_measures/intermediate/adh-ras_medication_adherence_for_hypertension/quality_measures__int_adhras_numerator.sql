{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

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
    from {{ ref('quality_measures__int_adhras_denominator') }}

)

, performance_end as (

    select
      performance_period_end
    from {{ ref('quality_measures__int_adhras__performance_period') }}

)

/*
The below 3 cte identifies periods of continuous medication use for each patient by:
1. Assigning a row number and tracking the previous medication per patient.
2. Flagging when a medication change occurs.
3. Grouping consecutive periods of the same medication by assigning a group ID.
*/

, ranked_patient as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , days_supply
        , dense_rank() over (partition by person_id
order by dispensing_date) as dense_rank
        , lag(ndc_code) over (partition by person_id
order by dispensing_date) as previous_ndc
    from denominator

)

, grouped_meds as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , days_supply
        , dense_rank
        , case
            when (ndc_code != previous_ndc) or previous_ndc is null then 1
            else 0
          end as med_change_flag --to increment group when medication changes
    from ranked_patient

)

, final_groups as (

    select
          person_id
        , ndc_code
        , dispensing_date
        , days_supply
        , sum(med_change_flag) over (
              partition by person_id
              order by dense_rank
              rows between unbounded preceding and current row
          ) as group_id
    from grouped_meds

)

/*
This cte theoretical_end_dates to final_fills calculates adjusted medication fill dates,
groups fills by continuous periods of use and ensures accurate start and end dates based
on previous fills and the performance period.
*/

, theoretical_end_dates as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , {{ dbt.dateadd (
              datepart = "day"
            , interval = -1
            , from_date_or_timestamp =
                dbt.dateadd (
                      datepart = "day"
                    , interval = "days_supply"
                    , from_date_or_timestamp = "dispensing_date"
            )
          ) }} as theoretical_end_date
    from final_groups

)

/*
Adjust start dates based on the previous fill's end date + 1,
or use the current rx_fill_date
*/

, previous_fill_end_dates as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , theoretical_end_date
        , lag(theoretical_end_date)
          over (partition by
                person_id
              , group_id
            order by
                dispensing_date
          ) as previous_fill_end_date
    from theoretical_end_dates

)

, adjusted_fill_dates as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , theoretical_end_date
        , coalesce(
            greatest(
                  dispensing_date
                , {{ dbt.dateadd (
                      datepart = "day"
                    , interval = +1
                    , from_date_or_timestamp = "previous_fill_end_date"
                  ) }}
                )
            , dispensing_date
        ) as adjusted_fill_date
    from previous_fill_end_dates

)


, actual_end_dates as (

    select
        person_id
      , group_id
      , dispensing_date
      , days_supply
      , adjusted_fill_date
      , least(
            {{ dbt.dateadd (
                datepart = "day"
              , interval = -1
              , from_date_or_timestamp =
                  dbt.dateadd (
                        datepart = "day"
                      , interval = "days_supply"
                      , from_date_or_timestamp = "adjusted_fill_date"
              )
            ) }}
          , performance_period_end
      ) as actual_end_date
    from adjusted_fill_dates
    inner join performance_end
      on adjusted_fill_dates.adjusted_fill_date <= performance_end.performance_period_end

)

, grouped_fill_ranges as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , adjusted_fill_date
        , actual_end_date
        , min(adjusted_fill_date) over (partition by person_id, group_id) as group_first
        , max(adjusted_fill_date) over (partition by person_id, group_id) as group_last
    from actual_end_dates

)

, final_fills as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , adjusted_fill_date
        , actual_end_date
        , group_first
        , group_last
        , max(
            case
              when adjusted_fill_date = group_last
              then days_supply
              else 0
            end) over (partition by person_id, group_id) as group_last_days_supply
    from grouped_fill_ranges

)

/*
1. Calculates the total covered days per every medication group per patient
2. Then, calculates the overlap between groups of medication per patient.
3. Then, calculates the actual total covered days for each patient.
*/

, covered_days_per_groups as (

    select
          person_id
        , group_id
        , group_first
        , group_last
        , group_last_days_supply
        , sum(1 + {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = 'adjusted_fill_date'
                                , second_date = 'actual_end_date'
                            )
            }}) as covered_days_per_group
    from final_fills
    group by
          person_id
        , group_id
        , group_first
        , group_last
        , group_last_days_supply

)

, with_lag as (

    select
          person_id
        , group_id
        , group_first
        , group_last
        , covered_days_per_group
        , lag(group_last) over (partition by person_id
order by group_first) as lag_date
        , lag(group_last_days_supply) over (partition by person_id
order by group_first) as lag_days_supply
    from covered_days_per_groups

)

, overlap_days as (

    select
          person_id
        , group_id
        , group_first
        , group_last
        , covered_days_per_group
        , lag_date
        , case
            when group_first <
                {{ dbt.dateadd (
                    datepart = "day"
                  , interval = "lag_days_supply"
                  , from_date_or_timestamp = "lag_date" 
                ) }}
            then
                {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = "group_first"
                                , second_date = 
                                              dbt.dateadd (
                                                datepart = "day"
                                              , interval = "lag_days_supply"
                                              , from_date_or_timestamp = "lag_date" 
                                            )
                ) }}
            else 0
          end as overlap
    from with_lag

)


, final_covered_days as (

    select
          person_id
        , sum(covered_days_per_group) - sum(overlap) as actual_covered_days
    from overlap_days
    group by person_id

)

, patient_with_treatment_period_days as (
    select
          person_id
        , {{ datediff('first_dispensing_date', 'performance_period_end', 'day') }} as treatment_period_days
    from denominator

)

, patient_with_pdc as (

    select
          final_covered_days.person_id
        , round(cast(actual_covered_days * 100 / treatment_period_days as {{ dbt.type_numeric() }}), 4) as adherence
    from final_covered_days
    inner join patient_with_treatment_period_days
        on final_covered_days.person_id = patient_with_treatment_period_days.person_id

)

/*
Selects only the patient whose pdc is greater than 80%.
*/

, valid_patients as (

    select
          patient_with_pdc.person_id
        , adherence
        , denominator.dispensing_date as evidence_date
        , denominator.days_supply as evidence_value
        , 1 as numerator_flag
    from patient_with_pdc
    inner join denominator
        on patient_with_pdc.person_id = denominator.person_id
    where patient_with_pdc.adherence >= 80.00

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
        , cast(adherence as {{ dbt.type_numeric() }}) as adherence
        , cast(numerator_flag as integer) as numerator_flag
    from valid_patients

)

select
      person_id
    , evidence_date
    , evidence_value
    , adherence
    , numerator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
