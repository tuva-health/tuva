with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    med.medical_claim_sk
  , 'inpatient' as service_category_1
  , 'inpatient hospice' as service_category_2
  , 'inpatient hospice' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_inpatient_institutional as inst
    on med.medical_claim_sk = inst.medical_claim_sk
where med.bill_type_code in ('82')
  or med.revenue_center_code in ('0655', '0656', '0658', '0115', '0125', '0135', '0145', '0155', '0235')
