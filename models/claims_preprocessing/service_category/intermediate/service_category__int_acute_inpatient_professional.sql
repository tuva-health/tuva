/*
 * Determines claim line service categories for acute inpatient.
 */
with service_category__stg_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_professional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'acute inpatient' as service_category_2
    , case
        when hcpcs_code in ('59400', '59409', '59410', '59610', '59612', '59614') then 'l/d - vaginal delivery'
        when hcpcs_code in ('59510', '59514', '59515', '59618', '59620', '59622') then 'l/d - cesarean delivery'
        else 'acute inpatient - other'
    end as service_category_3
from service_category__stg_professional
where place_of_service_code = '21'
