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
    , 'outpatient rehabilitation' as service_category_2
    , 'outpatient rehabilitation' as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_outpatient_institutional as outpatient
    on med.medical_claim_sk = outpatient.medical_claim_sk
where med.primary_taxonomy_code in (
    '283X00000X'
    , '273Y00000X'
    , '261QR0400X'
    , '315D00000X'
    , '261QR0401X'
    , '208100000X'
    , '225400000X'
    , '324500000X'
    , '2278P1005X'
    , '261QR0405X'
    , '2081S0010X'
    , '261QR0404X'
)