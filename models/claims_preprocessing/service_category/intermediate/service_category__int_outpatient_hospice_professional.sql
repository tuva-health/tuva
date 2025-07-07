with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    med.medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient hospice' as service_category_2
    , 'outpatient hospice' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5009')
