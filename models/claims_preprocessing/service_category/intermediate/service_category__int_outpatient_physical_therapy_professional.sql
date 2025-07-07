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
  , 'outpatient pt/ot/st' as service_category_2
  , 'outpatient pt/ot/st' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_professional as prof
    on med.medical_claim_sk = prof.medical_claim_sk
where (
    ccs_category in ('213', '212', '215')
    or med.facility_primary_specialty_description in (
        'Occupational Health'
        , 'Occupational Medicine'
        , 'Occupational Therapist in Private Practice'
        , 'Occupational Therapy Assistant'
        , 'Physical Therapist'
        , 'Physical Therapist in Private Practice'
        , 'Physical Therapy Assistant'
        , 'Speech Language Pathologist'
        , 'Speech-Language Assistant'
    ))
    and place_of_service_code <> '11'
