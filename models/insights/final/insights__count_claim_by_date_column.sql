select
    'claim_start_date' as date_field
    , cast(year(claim_start_date) as varchar) || right('0'||cast(month(claim_start_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__medical_claim') }}
group by 
    year(claim_start_date)
    , month(claim_start_date)

union all

select
    'claim_end_date' as date_field
    , cast(year(claim_end_date) as varchar) || right('0'||cast(month(claim_end_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__medical_claim') }}
group by 
    year(claim_end_date)
    , month(claim_end_date)

union all

select
    'admission_date' as date_field
    , cast(year(admission_date) as varchar) || right('0'||cast(month(admission_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__medical_claim') }}
group by 
    year(admission_date)
    , month(admission_date)

union all

select
    'discharge_date' as date_field
    , cast(year(discharge_date) as varchar) || right('0'||cast(month(discharge_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__medical_claim') }}
group by 
    year(discharge_date)
    , month(discharge_date)

union all

select
    'medical paid_date' as date_field
    , cast(year(paid_date) as varchar) || right('0'||cast(month(paid_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__medical_claim') }}
group by 
    year(paid_date)
    , month(paid_date)

union all

select
    'dispensing_date' as date_field
    , cast(year(dispensing_date) as varchar) || right('0'||cast(month(dispensing_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__pharmacy_claim') }}
group by 
    year(dispensing_date)
    , month(dispensing_date)

union all

select
    'pharmacy paid_date' as date_field
    , cast(year(paid_date) as varchar) || right('0'||cast(month(paid_date) as varchar),2) as year_month
    , count(distinct claim_id)
from {{ ref('core__pharmacy_claim') }}
group by 
    year(paid_date)
    , month(paid_date)




