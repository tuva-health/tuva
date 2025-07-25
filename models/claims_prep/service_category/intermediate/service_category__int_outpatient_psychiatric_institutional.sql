with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient psychiatric' as service_category_2
    , 'outpatient psychiatric' as service_category_3
from service_category__stg_outpatient_institutional
where revenue_center_code in ('0513', '0905')
    or primary_taxonomy_code in ('283Q00000X', '273R00000X')
