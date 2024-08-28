{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select claim_id
,claim_line_number
,old_encounter_id
,'office visit radiology' as encounter_type
,0 as priority_number
from {{ ref('office_visits__int_office_visits_radiology') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'office visit surgery' as encounter_type
,1 as priority_number
from {{ ref('office_visits__int_office_visits_surgery') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'office visit injections' as encounter_type
,2 as priority_number
from {{ ref('office_visits__int_office_visits_injections') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'office visit pt/ot/st' as encounter_type
,3 as priority_number
from {{ ref('office_visits__int_office_visits_ptotst') }}

union

select claim_id
,claim_line_number
,old_encounter_id
,'office visit' as encounter_type
,9999 as priority_number
from {{ ref('office_visits__int_office_visits') }}
