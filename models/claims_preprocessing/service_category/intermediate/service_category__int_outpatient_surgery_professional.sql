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
    , 'outpatient' as service_category_1
    , 'outpatient surgery' as service_category_2
    , 'outpatient surgery' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where
    (med.ccs_category between '1' and '176'
    or med.ccs_category in ('229', '230', '231', '232', '244'))
    and med.place_of_service_code in ('15', '17', '19', '22', '49', '50', '60', '71', '72')
