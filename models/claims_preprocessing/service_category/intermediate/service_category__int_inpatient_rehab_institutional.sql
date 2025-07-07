with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'inpatient rehabilitation' as service_category_2
    , 'inpatient rehabilitation' as service_category_3
from {{ ref('service_category__stg_medical_claim') }} as med
    inner join {{ ref('service_category__stg_inpatient_institutional') }} as i
    on med.medical_claim_sk = i.medical_claim_sk
where med.primary_taxonomy_code in ('283X00000X', '273Y00000X')
