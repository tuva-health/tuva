select
    'medical claim total rows' as data_quality_check
    , count(*) as result_count
from {{ ref('medical_claim') }}

union all

select
    'medical claim institutional total rows' as data_quality_check
    , count(*) as result_count
from {{ ref('medical_claim') }}
where claim_type = 'institutional'

union all

select
    'medical claim professional total rows' as data_quality_check
    , count(*) as result_count
from {{ ref('medical_claim') }}
where claim_type = 'professional'
union all

select
    'medical claim distinct claim count' as data_quality_check
    , count(distinct claim_id) as result_count
from {{ ref('medical_claim') }}

union all

select
    'medical claim distinct patient count' as data_quality_check
    , count(distinct patient_id) as result_count
from {{ ref('medical_claim') }}

