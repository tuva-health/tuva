with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
),
service_category__home_health_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_home_health_institutional') }}
)
select
    med.medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient hospice' as service_category_2
    , 'outpatient hospice' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_outpatient_institutional as outpatient
    on med.medical_claim_sk = outpatient.medical_claim_sk
    left join service_category__home_health_institutional as home
    on med.medical_claim_sk = home.medical_claim_sk
where
    med.bill_type_code in ('81')
    or (
        med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5009')
        and home.medical_claim_sk is not null -- is not home health
    )
    or med.revenue_center_code in ('0650', '0651', '0652', '0657', '0659')
