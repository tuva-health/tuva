{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with add_row_num as (
select
  patient_id,
  claim_id,
  start_date,
  end_date,
  discharge_disposition_code,
  facility_npi,
  row_number() over (partition by patient_id order by end_date, start_date, claim_id) as row_num
from {{ ref('emergency_department__int_institutional_claims') }}
),

check_for_merges_with_larger_row_num as (
select
  aa.patient_id,
  aa.claim_id as claim_id_a,
  bb.claim_id as claim_id_b,
  aa.row_num as row_num_a,
  bb.row_num as row_num_b,
  case
    -- Claims with same end_date and same facility_npi should be merged:
    when (aa.end_date = bb.end_date
          and aa.facility_npi = bb.facility_npi) then 1
    -- Claims with different end_date 
    -- should be merged if they overlap:
    when ( (aa.end_date <> bb.end_date) and 
           (aa.end_date >= bb.start_date) and --overlap requirement
           (aa.facility_npi = bb.facility_npi)
	 )then 1
    else 0
  end as merge_flag
from add_row_num aa
     inner join add_row_num bb
     on aa.patient_id = bb.patient_id
     and aa.row_num < bb.row_num
),

merges_with_larger_row_num as (
select
  patient_id,
  claim_id_a,
  claim_id_b,
  row_num_a,
  row_num_b,
  merge_flag
from check_for_merges_with_larger_row_num
where merge_flag = 1
),


claim_ids_that_merge_with_larger_row_num as (
select distinct claim_id_a as claim_id
from merges_with_larger_row_num
),


claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num as (
select distinct aa.claim_id as claim_id
from add_row_num aa
     inner join
     merges_with_larger_row_num bb
     on aa.patient_id = bb.patient_id
     and bb.row_num_a < aa.row_num
     and bb.row_num_b > aa.row_num
),


close_flags as (
select
  aa.patient_id,
  aa.claim_id,
  aa.start_date,
  aa.end_date,
  aa.discharge_disposition_code,
  aa.facility_npi,
  aa.row_num,
  case when (bb.claim_id is null and cc.claim_id is null) then 1
       else 0
  end as close_flag

from add_row_num aa

left join claim_ids_that_merge_with_larger_row_num bb
on aa.claim_id = bb.claim_id

left join claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num cc
on aa.claim_id = cc.claim_id
),


join_every_row_to_later_closes as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.row_num as row_num,
  bb.row_num as row_num_b
from close_flags aa inner join close_flags bb
     on aa.patient_id = bb.patient_id
     and aa.row_num <= bb.row_num
where bb.close_flag = 1
),


find_min_closing_row_num_for_every_claim as (
select
  patient_id,
  claim_id,
  min(row_num_b) as min_closing_row
from join_every_row_to_later_closes
group by patient_id, claim_id
),


add_min_closing_row_to_every_claim as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.start_date as start_date,
  aa.end_date as end_date,
  aa.discharge_disposition_code as discharge_disposition_code,
  aa.facility_npi as facility_npi,
  aa.row_num as row_num,
  aa.close_flag as close_flag,
  bb.min_closing_row as min_closing_row
from close_flags aa
     left join find_min_closing_row_num_for_every_claim bb
     on aa.patient_id = bb.patient_id
     and aa.claim_id = bb.claim_id
),


add_encounter_id as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.start_date as start_date,
  aa.end_date as end_date,
  aa.discharge_disposition_code as discharge_disposition_code,
  aa.facility_npi as facility_npi,
  aa.row_num as row_num,
  aa.close_flag as close_flag,
  aa.min_closing_row as min_closing_row,
  bb.claim_id as encounter_id
from add_min_closing_row_to_every_claim aa
     left join add_min_closing_row_to_every_claim bb
     on aa.patient_id = bb.patient_id
     and aa.min_closing_row = bb.row_num
)

select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from add_encounter_id
