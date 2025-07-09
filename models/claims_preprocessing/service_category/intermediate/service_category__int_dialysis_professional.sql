with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select distinct
    medical_claim_sk
  , 'outpatient' as service_category_1
  , 'dialysis' as service_category_2
  , 'dialysis' as service_category_3
from service_category__stg_professional
where
    place_of_service_code in ('65')
    or ccs_category in ('91', '58', '57')
