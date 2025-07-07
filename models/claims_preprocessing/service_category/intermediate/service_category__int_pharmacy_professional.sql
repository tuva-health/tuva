with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    med.medical_claim_sk
    , case when med.place_of_service_code = '11'
        then 'office-based'
        else 'outpatient'
    end as service_category_1
    , 'pharmacy' as service_category_2
    , 'pharmacy' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where med.ccs_category = '240' --medications
