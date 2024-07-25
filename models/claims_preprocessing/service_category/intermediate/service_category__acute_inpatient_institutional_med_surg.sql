{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}



select distinct 
  a.claim_id
, case when m.medical_surgical = 'M' then 'Medical'
       when m.medical_surgical = 'P' then 'Surgical'
       else 'Acute Inpatient' end    as service_category_2
, case when m.medical_surgical = 'M' then 'Medical'
       when m.medical_surgical = 'P' then 'Surgical'
       else 'Acute Inpatient' end    as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} s
inner join {{ ref('service_category__stg_inpatient_institutional') }} a on s.claim_id = a.claim_id
inner join {{ref('terminology__ms_drg')}} m on s.ms_drg_code = m.ms_drg_code