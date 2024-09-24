{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          patient_id
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

, cte2 as (

    select
          patient_id
        , dispensing_date
        , ndc_code
        , days_supply
        , dense_rank() over (partition by patient_id order by dispensing_date) as rn
        , lag(ndc_code) over (partition by patient_id order by dispensing_date) as previous_ndc
    from denominator

)

, grouped_meds as (

    select
          patient_id
        , dispensing_date
        , ndc_code
        , days_supply
        , rn
        , case
            when (ndc_code != previous_ndc) or previous_ndc is null then 1
            else 0
          end as med_change_flag --to increment group when medication changes
    from cte2

)

, final_groups as (

    select
          patient_id
        , ndc_code
        , dispensing_date
        , days_supply
        , sum(med_change_flag) over (partition by patient_id order by rn) as group_id
    from grouped_meds

)

, fills as (

    select
          patient_id
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

, adjusted_fillsv1 as (
    
    /* Adjust start dates based on the previous fill's end date + 1,
    or use the current rx_fill_date */
    select
          patient_id
        , group_id
        , dispensing_date
        , days_supply
        , theoretical_end_date
        , lag(theoretical_end_date)
          over (partition by 
                patient_id
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
          patient_id
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
    from adjusted_fillsv1

)

, final_fills as (

    select
        patient_id
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

, final_final_fills as (

    select
          patient_id
        , group_id
        , dispensing_date
        , days_supply
        , adjusted_start_date
        , final_end_date
        , min(adjusted_start_date) over(partition by patient_id, group_id) as first_disp_date
        , max(adjusted_start_date) over(partition by patient_id, group_id) as last_disp_date
    from final_fills

)

, last_med_end_groupwise as (

    select
          patient_id
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
            end) over (partition by patient_id, group_id) as last_days_supply
    from final_final_fills

)

, covered_days as (
    
    select
          patient_id
        , group_id
        , first_disp_date
        , last_disp_date
        , last_days_supply
        , sum( 1 + {{ dbt.datediff (
                                  datepart = 'day'
                                , first_date = 'adjusted_start_date'
                                , second_date = 'final_end_date'
                            )
            }} ) as covered_days
    from last_med_end_groupwise
    group by 
          patient_id
        , group_id
        , first_disp_date
        , last_disp_date
        , last_days_supply

)

, final_with_lag as (

    select
          patient_id
        , group_id
        , first_disp_date
        , last_disp_date
        , covered_days
        , lag(last_disp_date) over(partition by patient_id order by first_disp_date) as lag_date
        , lag(last_days_supply) over(partition by patient_id order by first_disp_date) as lag_days_supply
    from covered_days

)

, overlap_days as (

    select
          patient_id
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
          patient_id
        , sum(covered_days) - sum(overlap) as actual_covered_days
    from overlap_days
    group by patient_id

)  

, relevant_patients_from_deno as (

    select
          final_covered_days.patient_id
        , round(cast(actual_covered_days / days_in_treatment_period as {{ dbt.type_numeric() }}), 4) as adherence
        , dispensing_date as evidence_date
        , days_supply as evidence_value
    from final_covered_days
    inner join denominator
      on final_covered_days.patient_id = denominator.patient_id
        
)

, numerator as (

    select
          patient_id
        , adherence
        , evidence_date
        , evidence_value
        , 1 as numerator_flag
    from relevant_patients_from_deno
    where adherence >= 0.8

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(evidence_date as date) as evidence_date
        , cast(evidence_value as {{ dbt.type_string() }}) as evidence_value
        , cast(adherence as {{ dbt.type_numeric() }}) as adherence
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

)

select
      patient_id
    , evidence_date
    , evidence_value
    , adherence
    , numerator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
