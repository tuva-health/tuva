with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'emergency department' as service_category_2
    , 'emergency department' as service_category_3
from service_category__stg_outpatient_institutional
where revenue_center_code in ('450', '451', '452', '459', '981')
    or hcpcs_code in ('99281', '99282', '99283', '99284', '99285', 'G0380', 'G0381', 'G0382', 'G0383', 'G0384')
union
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'emergency department' as service_category_2
    , 'emergency department' as service_category_3
from service_category__stg_inpatient_institutional
where revenue_center_code in ('0450', '0451', '0452', '0459', '0981')