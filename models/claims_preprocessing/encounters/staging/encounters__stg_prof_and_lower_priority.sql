/*
This model unions professional claims and institutional claims that are "lower priority" (dme/lab/ambulance).
These should be part of a higher priority encounter where one exists.
*/
select
    medical_claim_sk
    , data_source
    , claim_id
    , claim_line_number
    , patient_sk
    , start_date
from {{ ref('encounters__stg_medical_claim') }}
where claim_type = 'professional'
union
select
    medical_claim_sk
    , data_source
    , claim_id
    , claim_line_number
    , patient_sk
    , start_date
from {{ ref('encounters__stg_medical_claim') }}
where service_category_2 in ('lab', 'durable medical equipment', 'ambulance')