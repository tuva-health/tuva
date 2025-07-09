with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'skilled nursing' as service_category_2
    , case bill_type_code
        when '21' then 'inpatient part A'
        when '22' then 'inpatient part B'
        end as service_category_3
from service_category__stg_medical_claim
where claim_type = 'institutional'
    and bill_type_code in ('21', '22')