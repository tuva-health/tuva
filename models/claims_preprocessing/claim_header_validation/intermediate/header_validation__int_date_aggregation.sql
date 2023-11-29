select
    claim_id
    , data_source
    , min(claim_start_date) as minimum_claim_start_date
    , max(claim_end_date) as maximum_claim_end_date
    , min(admission_date) as minimum_admission_date
    , max(claim_end_date) as maximum_discharge_date
from {{ ref('medical_claim') }}
where claim_type = 'institutional'
group by
    claim_id
    , data_source

union all

select
    claim_id
    , data_source
    , min(claim_start_date) as minimum_claim_start_date
    , max(claim_end_date) as maximum_claim_end_date
    , null as minimum_admission_date
    , null as maximum_discharge_date
from {{ ref('medical_claim') }}
where claim_type = 'professional'
group by
    claim_id
    , data_source