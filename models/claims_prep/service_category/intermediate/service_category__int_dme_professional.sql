with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
  , 'ancillary' as service_category_1
  , 'durable medical equipment' as service_category_2
  , 'durable medical equipment' as service_category_3
from service_category__stg_professional
where ccs_category = '243'
