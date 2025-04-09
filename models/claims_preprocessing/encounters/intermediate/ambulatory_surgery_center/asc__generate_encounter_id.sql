{{ config(
    enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with base_data as (
    select distinct
        m.patient_data_source_id
      , m.start_date
      , m.end_date
      , m.claim_id
    from {{ ref('encounters__stg_medical_claim') }} as m
    inner join {{ ref('asc__anchor_events') }} as u
      on m.claim_id = u.claim_id
)

-- Determine Previous Maximum End Date
, grouped_data as (
    select
        bd.*
      , max(end_date) over (
            partition by patient_data_source_id
            order by start_date, claim_id
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end_date
    from base_data as bd
)

-- Flag New Encounter Groups
, flagged_data as (
    select
        gd.*
      , case
            when start_date > coalesce(previous_max_end_date, {{ dbt.cast("'1900-01-01'", api.Column.translate_type('date')) }} ) then 1
            else 0
        end as new_group_flag
    from grouped_data as gd
)

-- Assign Encounter Groups per Patient
, numbered_data as (
    select
        fd.*
      , sum(new_group_flag) over (
            partition by patient_data_source_id
            order by start_date, claim_id
            rows unbounded preceding
        ) as encounter_group
    from flagged_data as fd
)

-- Identify Unique Encounters
, unique_encounters as (
    select
        patient_data_source_id
      , encounter_group
      , min(start_date) as encounter_start_date
    from numbered_data
    group by
        patient_data_source_id
      , encounter_group
)

-- Assign asc encounter_id
, numbered_encounters as (
    select
        patient_data_source_id
      , encounter_group
      , row_number() over (
            order by patient_data_source_id, encounter_start_date
        ) as encounter_id
    from unique_encounters
)

-- Merge Encounters with Claims
select
    nd.patient_data_source_id
  , nd.start_date
  , nd.end_date
  , nd.claim_id
  , ne.encounter_id as old_encounter_id
from numbered_data as nd
inner join numbered_encounters as ne
  on nd.patient_data_source_id = ne.patient_data_source_id
  and nd.encounter_group = ne.encounter_group
