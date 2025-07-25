with service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient long term acute care' as service_category_2
    , 'inpatient long term acute care' as service_category_3
from service_category__stg_inpatient_institutional
where primary_taxonomy_code in ('282E00000X')
