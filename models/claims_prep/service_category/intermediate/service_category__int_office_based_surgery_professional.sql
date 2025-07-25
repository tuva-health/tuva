with service_category__stg_office_based as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_office_based') }}
)
select
    medical_claim_sk
    , 'office-based' as service_category_1
    , 'office-based surgery' as service_category_2
    , 'office-based surgery' as service_category_3
from service_category__stg_office_based
where
    (hcpcs_code between '10021' and '69999')
