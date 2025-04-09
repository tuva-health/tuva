{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with claim_start_end as (
  select
    claim_id
    , patient_data_source_id
    , min(start_date) as start_date
    , max(end_date) as end_date
  from {{ ref('encounters__stg_medical_claim') }}
  group by claim_id, patient_data_source_id
)

, base as (
  select distinct
    enc.claim_id
    , enc.patient_data_source_id
    , c.start_date
    , c.end_date
    , enc.facility_id
    , enc.discharge_disposition_code
  from {{ ref('encounters__stg_medical_claim') }} as enc
  inner join claim_start_end as c
    on enc.claim_id = c.claim_id
    and c.patient_data_source_id = enc.patient_data_source_id
  where
    enc.service_category_2 in ('inpatient hospice')
    and enc.claim_type = 'institutional'
)

, add_row_num as (
  select
    patient_data_source_id
    , claim_id
    , start_date
    , end_date
    , discharge_disposition_code
    , facility_id
    , row_number() over (partition by patient_data_source_id
order by end_date, start_date, claim_id) as row_num
  from base
)

, check_for_merges_with_larger_row_num as (
  select
    aa.patient_data_source_id
    , aa.claim_id as claim_id_a
    , bb.claim_id as claim_id_b
    , aa.row_num as row_num_a
    , bb.row_num as row_num_b
    , case
      when aa.end_date = bb.end_date and aa.facility_id = bb.facility_id then 1
      when {{ dbt.dateadd(datepart= 'day', interval=1, from_date_or_timestamp='aa.end_date') }} = bb.start_date
        and aa.facility_id = bb.facility_id
        and aa.discharge_disposition_code = '30' then 1
      when aa.end_date <> bb.end_date
        and aa.end_date > bb.start_date
        and aa.facility_id = bb.facility_id then 1
      else 0
    end as merge_flag
  from add_row_num as aa
  inner join add_row_num as bb
    on aa.patient_data_source_id = bb.patient_data_source_id
    and aa.row_num < bb.row_num
    and aa.claim_id <> bb.claim_id
)


, merges_with_larger_row_num as (
  select
      patient_data_source_id
    , claim_id_a
    , claim_id_b
    , row_num_a
    , row_num_b
    , merge_flag
  from check_for_merges_with_larger_row_num
  where merge_flag = 1
)

, claim_ids_that_merge_with_larger_row_num as (
  select distinct
      claim_id_a as claim_id
  from merges_with_larger_row_num
)

, claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num as (
  select distinct
      aa.claim_id as claim_id
  from add_row_num as aa
  inner join merges_with_larger_row_num as bb
    on aa.patient_data_source_id = bb.patient_data_source_id
    and bb.row_num_a < aa.row_num
    and bb.row_num_b > aa.row_num
)

, close_flags as (
  select
      aa.patient_data_source_id
    , aa.claim_id
    , aa.start_date
    , aa.end_date
    , aa.discharge_disposition_code
    , aa.facility_id
    , aa.row_num
    , case
        when bb.claim_id is null
          and cc.claim_id is null then 1
        else 0
      end as close_flag
  from add_row_num as aa
  left outer join claim_ids_that_merge_with_larger_row_num as bb
    on aa.claim_id = bb.claim_id
  left outer join claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num as cc
    on aa.claim_id = cc.claim_id
)

, join_every_row_to_later_closes as (
  select
      aa.patient_data_source_id
    , aa.claim_id
    , aa.row_num
    , bb.row_num as row_num_b
  from close_flags as aa
  inner join close_flags as bb
    on aa.patient_data_source_id = bb.patient_data_source_id
    and aa.row_num <= bb.row_num
  where bb.close_flag = 1
)

, find_min_closing_row_num_for_every_claim as (
  select
      patient_data_source_id
    , claim_id
    , min(row_num_b) as min_closing_row
  from join_every_row_to_later_closes
  group by
      patient_data_source_id
    , claim_id
)

, add_min_closing_row_to_every_claim as (
  select
      aa.patient_data_source_id
    , aa.claim_id
    , aa.start_date
    , aa.end_date
    , aa.discharge_disposition_code
    , aa.facility_id
    , aa.row_num
    , aa.close_flag
    , bb.min_closing_row
  from close_flags as aa
  left outer join find_min_closing_row_num_for_every_claim as bb
    on aa.patient_data_source_id = bb.patient_data_source_id
    and aa.claim_id = bb.claim_id
)

, add_encounter_id as (
  select
      aa.patient_data_source_id
    , aa.claim_id
    , aa.start_date
    , aa.end_date
    , aa.discharge_disposition_code
    , aa.facility_id
    , aa.row_num
    , aa.close_flag
    , aa.min_closing_row
    , bb.claim_id as encounter_id
  from add_min_closing_row_to_every_claim as aa
  left outer join add_min_closing_row_to_every_claim as bb
    on aa.patient_data_source_id = bb.patient_data_source_id
    and aa.min_closing_row = bb.row_num
)

select
    patient_data_source_id
  , claim_id
  , start_date
  , end_date
  , discharge_disposition_code
  , facility_id
  , row_number() over (partition by encounter_id
order by start_date, end_date, claim_id) as encounter_claim_number
  , row_number() over (partition by encounter_id
order by start_date desc, end_date desc, claim_id desc) as encounter_claim_number_desc
  , close_flag
  , min_closing_row
  , dense_rank() over (
order by encounter_id) as encounter_id
from add_encounter_id
