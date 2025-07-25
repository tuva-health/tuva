with service_category__stg_office_based as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_office_based') }}
)
select
    medical_claim_sk
    , 'office-based' as service_category_1
    , case
        when place_of_service_code = '11' then 'office-based visit'
        when place_of_service_code in ('02', '10') then 'telehealth visit'
    end as service_category_2
    , case
        when place_of_service_code = '11' then 'office-based visit'
        when place_of_service_code in ('02', '10') then 'telehealth visit'
    end as service_category_3
from service_category__stg_office_based
where
    (place_of_service_code = '11' and ccs_category = '227') -- consultation eval and preventative care
    or place_of_service_code in ('02', '10') -- telehealth
