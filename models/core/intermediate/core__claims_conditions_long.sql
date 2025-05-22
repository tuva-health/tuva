
{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '1' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_1 as source_code,
  diagnosis_poa_1 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_1 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '2' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_2 as source_code,
  diagnosis_poa_2 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_2 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '3' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_3 as source_code,
  diagnosis_poa_3 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_3 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '4' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_4 as source_code,
  diagnosis_poa_4 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_4 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '5' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_5 as source_code,
  diagnosis_poa_5 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_5 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '6' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_6 as source_code,
  diagnosis_poa_6 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_6 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '7' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_7 as source_code,
  diagnosis_poa_7 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_7 is not null

union all

select
claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '8' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_8 as source_code,
  diagnosis_poa_8 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_8 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '9' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_9 as source_code,
  diagnosis_poa_9 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_9 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '10' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_10 as source_code,
  diagnosis_poa_10 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_10 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '11' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_11 as source_code,
  diagnosis_poa_11 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_11 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '12' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_12 as source_code,
  diagnosis_poa_12 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_12 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '13' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_13 as source_code,
  diagnosis_poa_13 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_13 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '14' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_14 as source_code,
  diagnosis_poa_14 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_14 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '15' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_15 as source_code,
  diagnosis_poa_15 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_15 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '16' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_16 as source_code,
  diagnosis_poa_16 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_16 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '17' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_17 as source_code,
  diagnosis_poa_17 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_17 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '18' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_18 as source_code,
  diagnosis_poa_18 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_18 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '19' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_19 as source_code,
  diagnosis_poa_19 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_19 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '20' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_20 as source_code,
  diagnosis_poa_20 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_20 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '21' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_21 as source_code,
  diagnosis_poa_21 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_21 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '22' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_22 as source_code,
  diagnosis_poa_22 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_22 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '23' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_23 as source_code,
  diagnosis_poa_23 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_23 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '24' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_24 as source_code,
  diagnosis_poa_24 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_24 is not null

union all

select
  claim_id,
  data_source,
  recorded_date,
  person_id,
  member_id,
  '25' as condition_rank,
  diagnosis_code_type,
  diagnosis_code_25 as source_code,
  diagnosis_poa_25 as present_on_admit_code
from {{ ref('core__recorded_date_and_header_fields') }}
where diagnosis_code_25 is not null
