with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
  , 'outpatient' as service_category_1
  , 'outpatient surgery' as service_category_2
  , 'outpatient surgery' as service_category_3
from service_category__stg_outpatient_institutional
where ccs_category between '1' and '176'
    or ccs_category in ('229', '230', '231', '232', '244')
