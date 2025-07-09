with service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    medical_claim_sk
    , 'inpatient' as service_category_1
    , 'acute inpatient' as service_category_2
    , case
        when drg_code in ('768', '796', '797', '798', '805', '806', '807') then 'l/d - vaginal delivery'
        when drg_code in ('783', '784', '785', '786', '787', '788') then 'l/d - cesarean delivery'
        when drg_code = '795' then 'l/d - newborn'
        when drg_code in ('789', '790', '791', '792', '793', '794') then 'l/d - newborn nicu'
        when revenue_center_code in ('0173', '0174') then 'l/d - newborn nicu'
        else 'l/d - other'
    end as service_category_3
from service_category__stg_inpatient_institutional
where mdc_code in ('14', '15')
