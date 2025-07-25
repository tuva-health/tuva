with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'ambulatory surgery center' as service_category_2
    , 'ambulatory surgery center' as service_category_3
from service_category__stg_outpatient_institutional
where
    revenue_center_code in ('0490', '0499')
    or primary_taxonomy_code = '261QA1903X'
