with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    med.medical_claim_sk
  , 'outpatient' as service_category_1
  , 'outpatient substance use' as service_category_2
  , 'outpatient substance use' as service_category_3
from service_category__stg_medical_claim as med
inner join service_category__stg_outpatient_institutional as outpatient
  on med.medical_claim_sk = outpatient.medical_claim_sk
where
    med.default_ccsr_category_description_op in (
        'MBD026'
        , 'SYM008'
        , 'MBD025'
        , 'SYM009'
        , 'MBD034'
    )
    or med.primary_taxonomy_code in (
        '324500000X'
        , '261QR0405X'
        , '101YA0400X'
    )
