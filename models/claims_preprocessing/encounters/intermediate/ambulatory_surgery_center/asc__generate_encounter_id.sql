{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with anchor as (
    select distinct
        m.patient_data_source_id
      , m.start_date
      , m.end_date
      , m.claim_id
    from {{ ref('encounters__stg_medical_claim') }} as m
    inner join {{ ref('asc__anchor_events') }} as u
      on m.claim_id = u.claim_id
)

, sorted_data as (
    select
        patient_data_source_id
      , start_date
      , end_date
      , claim_id
      , lag(end_date) over (partition by patient_data_source_id order by start_date, end_date) as previous_end_date
    from anchor
)

, grouped_data as (
    select
        patient_data_source_id
      , start_date
      , end_date
      , claim_id
      , case
            when previous_end_date is null or previous_end_date < start_date then 1
            else 0
        end as is_new_group
    from sorted_data
)

, encounters as (
    select
        patient_data_source_id
      , start_date
      , end_date
      , claim_id
      , sum(is_new_group) over (
            partition by patient_data_source_id
            order by start_date
            rows between unbounded preceding and current row  -- Frame clause required for Redshift
          ) as old_encounter_id
    from grouped_data
)

select
    patient_data_source_id
  , start_date
  , end_date
  , claim_id
  , old_encounter_id
from encounters
