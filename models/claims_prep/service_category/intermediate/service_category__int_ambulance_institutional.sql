with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'ancillary' as service_category_1
    , 'ambulance' as service_category_2
    , 'ambulance' as service_category_3
from service_category__stg_outpatient_institutional
where
    (hcpcs_code between 'A0425' and 'A0436'
    or revenue_center_code = '0540')
