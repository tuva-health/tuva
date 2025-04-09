{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , days_supply
        , days_in_treatment_period
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_adh_diabetes_denominator') }}

)

, performance_end as (

    select
      performance_period_end
    from {{ ref('quality_measures__int_adh_diabetes__performance_period') }}

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
order by dispensing_date) as dr
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
        , dr
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
              order by dr
              rows between unbounded preceding and current row
          ) as group_id
    from grouped_meds

)

/*
  The ctes below calculates adjusted medication fill dates,
  groups fills by continuous periods of use and ensures accurate start and end dates based
  on previous fills and the performance period.
*/

, fills as (

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

, previous_fill_end_date as (

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
          ) as previous_end_date
    from fills

)

, adjusted_fills as (

    /* Adjust start dates based on the previous fill's end date + 1,
    or use the current rx_fill_date */
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
                    , from_date_or_timestamp = "previous_end_date"
                  ) }}
                )
            , dispensing_date
        ) as adjusted_start_date
    from previous_fill_end_date

)

, final_fills as (

    select
        person_id
      , group_id
      , dispensing_date
      , days_supply
      , adjusted_start_date
      , least(
            {{ dbt.dateadd (
                datepart = "day"
              , interval = -1
              , from_date_or_timestamp =
                  dbt.dateadd (
                        datepart = "day"
                      , interval = "days_supply"
                      , from_date_or_timestamp = "adjusted_start_date"
              )
            ) }}
          , performance_period_end
      ) as final_end_date
    from adjusted_fills
    inner join performance_end
      on adjusted_fills.adjusted_start_date <= performance_end.performance_period_end

)

, grouped_fill_ranges as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , adjusted_start_date
        , final_end_date
        , min(adjusted_start_date) over (partition by person_id, group_id) as first_disp_date
        , max(adjusted_start_date) over (partition by person_id, group_id) as last_disp_date
    from final_fills

)

, last_med_end_groupwise as (

    select
          person_id
        , group_id
        , dispensing_date
        , days_supply
        , adjusted_start_date
        , final_end_date
        , first_disp_date
        , last_disp_date
        , max(
            case
              when adjusted_start_date = last_disp_date
              then days_supply
              else null
            end) over (partition by person_id, group_id) as last_days_supply
    from grouped_fill_ranges

)

/*
  1. Calculates the total covered days per medication group per patient
  2. Then calculates the overlap between groups of medication per patient
  3. Finally calculates the actual total covered days for each patient
*/

, covered_days_per_group as (

    select
          person_id
        , group_id
        , first_disp_date
        , last_disp_date
        , last_days_supply
        , sum(1 + {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = 'adjusted_start_date'
                                , second_date = 'final_end_date'
                            )
            }}) as covered_days
    from last_med_end_groupwise
    group by
          person_id
        , group_id
        , first_disp_date
        , last_disp_date
        , last_days_supply

)

, final_with_lag as (

    select
          person_id
        , group_id
        , first_disp_date
        , last_disp_date
        , covered_days
        , lag(last_disp_date) over (partition by person_id
order by first_disp_date) as lag_date
        , lag(last_days_supply) over (partition by person_id
order by first_disp_date) as lag_days_supply
    from covered_days_per_group

)

, overlap_days as (

    select
          person_id
        , group_id
        , first_disp_date
        , last_disp_date
        , covered_days
        , lag_date
        , case
            when first_disp_date <
                {{ dbt.dateadd (
                    datepart = "day"
                  , interval = "lag_days_supply"
                  , from_date_or_timestamp = "lag_date" 
                ) }}
            then
                {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = "first_disp_date"
                                , second_date = 
                                              dbt.dateadd (
                                                datepart = "day"
                                              , interval = "lag_days_supply"
                                              , from_date_or_timestamp = "lag_date" 
                                            )
                ) }}
            else 0
          end as overlap
    from final_with_lag

)

, final_covered_days as (

    select
          person_id
        , sum(covered_days) - sum(overlap) as actual_covered_days
    from overlap_days
    group by person_id

)

, relevant_patients_from_deno as (

    select
          final_covered_days.person_id
        , round(cast(actual_covered_days / days_in_treatment_period as {{ dbt.type_numeric() }}), 4) as adherence
        , dispensing_date as evidence_date
        , days_supply as evidence_value
    from final_covered_days
    inner join denominator
      on final_covered_days.person_id = denominator.person_id

)

, numerator as (

    select
          person_id
        , adherence * 100 as adherence --percent conversion
        , evidence_date
        , evidence_value
        , 1 as numerator_flag
    from relevant_patients_from_deno
    where adherence >= 0.8

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
        , cast(adherence as {{ dbt.type_numeric() }}) as adherence
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

)

select
      person_id
    , evidence_date
    , evidence_value
    , adherence
    , numerator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
