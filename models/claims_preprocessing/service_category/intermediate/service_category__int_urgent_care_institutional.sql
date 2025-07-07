with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'urgent care' as service_category_2
    , 'urgent care' as service_category_3
from service_category__stg_medical_claim
where claim_type = 'institutional'
    and (
            (revenue_center_code = '0456'
            and bill_type_code in ('13', '71', '73')
            )
            or hcpcs_code in ('S9088', '99051', 'S9083')
        )