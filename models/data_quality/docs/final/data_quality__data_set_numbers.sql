select
    'medical claim total rows'
    , count(*)
from {{ ref('medical_claim') }}

union all

select
    'medical claim institutional total rows'
    , count(*)
from {{ ref('medical_claim') }}
where claim_type = 'institutional'

union all

select
    'medical claim professional total rows'
    , count(*)
from {{ ref('medical_claim') }}
where claim_type = 'professional'

union all

select
    'medical claim distinct claim count'
    , count(distinct claim_id)
from {{ ref('medical_claim') }}

union all

select
    'medical claim distinct patient count'
    , count(distinct patient_id)
from {{ ref('medical_claim') }}