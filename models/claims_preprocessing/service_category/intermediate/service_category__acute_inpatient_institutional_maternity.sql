{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    a.claim_id
  , 'inpatient' as service_category_1
  , 'acute inpatient' as service_category_2
  , case 
      when s.ms_drg_code in ('768', '796', '797', '798', '805', '806', '807') then 'L/D - Vaginal Delivery'
      when s.ms_drg_code in ('783', '784', '785', '786', '787', '788') then 'L/D - Cesarean Delivery'
      when s.ms_drg_code in ('795') then 'L/D - Newborn'
      when s.ms_drg_code in ('789', '790', '791', '792', '793', '794') then 'L/D - Newborn NICU'
      when s.revenue_center_code in ('0173', '0174') then 'L/D - Newborn NICU'
      else 'L/D - Other'
    end as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as s
inner join {{ ref('service_category__stg_inpatient_institutional') }} as a
  on s.claim_id = a.claim_id
inner join {{ ref('terminology__ms_drg') }} as m
  on s.ms_drg_code = m.ms_drg_code
where 
  s.mdc_code in ('MDC 14', 'MDC 15')
