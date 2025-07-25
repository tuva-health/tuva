with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
  , 'outpatient' as service_category_1
  , 'outpatient pt/ot/st' as service_category_2
  , 'outpatient pt/ot/st' as service_category_3
from service_category__stg_professional
where (
    ccs_category in ('213', '212', '215')
    or facility_primary_specialty_description in (
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
