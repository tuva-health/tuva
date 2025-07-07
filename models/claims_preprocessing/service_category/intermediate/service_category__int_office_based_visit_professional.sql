with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_office_based as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_office_based') }}
)
select
    med.medical_claim_sk
    , 'office-based' as service_category_1
    , case
        when med.place_of_service_code = '11' then 'office-based visit'
        when med.place_of_service_code in ('02', '10') then 'telehealth visit'
    end as service_category_2
    , case
        when med.place_of_service_code = '11' then 'office-based visit'
        when med.place_of_service_code in ('02', '10') then 'telehealth visit'
    end as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_office_based as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where
    (med.place_of_service_code = '11' and med.ccs_category = '227') -- consultation eval and preventative care
    or med.place_of_service_code in ('02', '10') -- telehealth
