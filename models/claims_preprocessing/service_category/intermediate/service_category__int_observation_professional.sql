with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'observation' as service_category_2
    , 'observation' as service_category_3
from service_category__stg_medical_claim
where claim_type = 'professional'
    and hcpcs_code in ('G0378', 'G0379')
