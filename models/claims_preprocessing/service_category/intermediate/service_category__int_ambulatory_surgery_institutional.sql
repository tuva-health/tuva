with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    med.medical_claim_sk
    , 'outpatient' as service_category_1
    , 'ambulatory surgery center' as service_category_2
    , 'ambulatory surgery center' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_outpatient_institutional as inst
    on med.medical_claim_sk = inst.medical_claim_sk
where
    med.revenue_center_code in ('0490', '0499')
    or med.primary_taxonomy_code = '261QA1903X'
