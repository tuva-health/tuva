with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient substance use' as service_category_2
    , 'inpatient substance use' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_inpatient_institutional as a
    on med.medical_claim_sk = a.medical_claim_sk
where
    med.primary_taxonomy_code in (
        '324500000X'
        , '261QR0405X'
        , '101YA0400X'
    )
    or med.default_ccsr_category_description_ip in (
        'MBD026'
        , 'SYM008'
        , 'MBD025'
        , 'SYM009'
        , 'MBD034'
    )
