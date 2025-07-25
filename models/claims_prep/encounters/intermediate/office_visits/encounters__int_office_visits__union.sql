select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit radiology' as encounter_type
    , 0 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where radiology_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit surgery' as encounter_type
    , 1 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where surgery_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit injections' as encounter_type
    , 2 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where injections_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit pt/ot/st' as encounter_type
    , 3 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where ptotst_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit' as encounter_type
    , 4 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where em_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'telehealth' as encounter_type
    , 5 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
where telehealth_flag = 1

union

select medical_claim_sk
    , encounter_id
    , start_date as encounter_start_date
    , end_date as encounter_end_date
    , 'office visit - other' as encounter_type
    , 9999 as priority_number
from {{ ref('encounters__int_office_visits__categorized') }}
