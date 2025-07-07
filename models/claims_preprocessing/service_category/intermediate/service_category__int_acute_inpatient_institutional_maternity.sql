with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
),
ms_drg as (
    select *
    from {{ ref('tuva_data_assets', 'ms_drg') }}
)
select
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'acute inpatient' as service_category_2
    , case
        when med.drg_code in ('768', '796', '797', '798', '805', '806', '807') then 'l/d - vaginal delivery'
        when med.drg_code in ('783', '784', '785', '786', '787', '788') then 'l/d - cesarean delivery'
        when med.drg_code = '795' then 'l/d - newborn'
        when med.drg_code in ('789', '790', '791', '792', '793', '794') then 'l/d - newborn nicu'
        when med.revenue_center_code in ('0173', '0174') then 'l/d - newborn nicu'
        else 'l/d - other'
    end as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_inpatient_institutional as a
    on med.medical_claim_sk = a.medical_claim_sk
    inner join ms_drg
    on med.drg_code_type = 'ms-drg'
    and med.drg_code = ms_drg.ms_drg_code
where ms_drg.mdc_code in ('14', '15')
