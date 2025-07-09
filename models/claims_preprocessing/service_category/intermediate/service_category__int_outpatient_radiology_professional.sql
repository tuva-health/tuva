with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient radiology' as service_category_2
    , case when modality = 'Nuclear medicine' then 'pet'
          when modality = 'Magnetic resonance' then 'mri'
          when modality = 'Computerized tomography' then 'ct'
          when modality in ('Invasive', 'Ultrasound', 'Computer-aided detection', 'Three-dimensional reconstruction', 'Radiography') then 'general'
          else 'general'
      end as service_category_3
from service_category__stg_professional
where modality is not null
    and place_of_service_code <> '11'
