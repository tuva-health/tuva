with service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient psychiatric' as service_category_2
    , 'inpatient psychiatric' as service_category_3
from service_category__stg_inpatient_institutional
where primary_taxonomy_code in ('283Q00000X', '273R00000X')
