with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient surgery' as service_category_2
    , 'outpatient surgery' as service_category_3
from service_category__stg_professional
where
    (ccs_category between '1' and '176'
    or ccs_category in ('229', '230', '231', '232', '244'))
    and place_of_service_code in ('15', '17', '19', '22', '49', '50', '60', '71', '72')
