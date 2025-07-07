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
  , 'emergency department' as service_category_2
  , 'emergency department' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where med.place_of_service_code = '23'
    or med.hcpcs_code in ('99281', '99282', '99283', '99284', '99285', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384')
