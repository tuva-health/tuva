with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('service_category__stg_outpatient_institutional') }}
)
select
    med.medical_claim_sk
    , 'ancillary' as service_category_1
    , 'ambulance' as service_category_2
    , 'ambulance' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_outpatient_institutional as inst
    on med.medical_claim_sk = inst.medical_claim_sk
where
    (med.hcpcs_code between 'A0425' and 'A0436'
    or med.revenue_center_code = '0540')
