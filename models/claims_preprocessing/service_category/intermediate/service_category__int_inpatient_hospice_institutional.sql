with service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient hospice' as service_category_2
    , 'inpatient hospice' as service_category_3
from service_category__stg_inpatient_institutional
where bill_type_code in ('82')
  or revenue_center_code in ('0655', '0656', '0658', '0115', '0125', '0135', '0145', '0155', '0235')
