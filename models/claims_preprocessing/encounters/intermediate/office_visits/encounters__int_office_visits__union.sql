select medical_claim_sk
    , encounter_id
    , 'office visit radiology' as encounter_type
    , 0 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where radiology_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'office visit surgery' as encounter_type
    , 1 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where surgery_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'office visit injections' as encounter_type
    , 2 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where injections_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'office visit pt/ot/st' as encounter_type
    , 3 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where ptotst_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'office visit' as encounter_type
    , 4 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where em_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'telehealth' as encounter_type
    , 5 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
where telehealth_flag = 1

union

select medical_claim_sk
    , encounter_id
    , 'office visit - other' as encounter_type
    , 9999 as priority_number
from {{ ref('encounters__int_office_visits__generate_encounter_id') }}
