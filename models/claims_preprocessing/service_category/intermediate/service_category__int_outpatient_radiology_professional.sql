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
    , 'outpatient radiology' as service_category_2
    , case when med.modality = 'Nuclear medicine' then 'pet'
          when med.modality = 'Magnetic resonance' then 'mri'
          when med.modality = 'Computerized tomography' then 'ct'
          when med.modality in ('Invasive', 'Ultrasound', 'Computer-aided detection', 'Three-dimensional reconstruction', 'Radiography') then 'general'
          else 'general'
      end as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where med.modality is not null
    and place_of_service_code <> '11'
