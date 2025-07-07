with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    med.medical_claim_sk
    , 'outpatient' as service_category_1
    , 'pharmacy' as service_category_2
    , 'pharmacy' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_outpatient_institutional as outpatient
    on med.medical_claim_sk = outpatient.medical_claim_sk
where
    (substring(med.revenue_center_code, 1, 3) in ('025', '026', '063', '089') -- pharmacy and iv therapy
    or med.revenue_center_code = '0547'
    or med.ccs_category = '240') -- medications
union
select
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'pharmacy' as service_category_2
    , 'pharmacy' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_inpatient_institutional as inpatient
    on med.medical_claim_sk = inpatient.medical_claim_sk
where
    (substring(med.revenue_center_code, 1, 3) in ('025', '026', '063', '089') -- pharmacy and iv therapy
    or med.revenue_center_code = '0547')
