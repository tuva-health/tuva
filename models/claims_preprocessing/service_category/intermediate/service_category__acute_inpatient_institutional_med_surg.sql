{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    a.claim_id
  , 'inpatient' as service_category_1
  , 'acute inpatient' as service_category_2
  , case
      when coalesce(ms.medical_surgical, apr.medical_surgical) = 'M' then 'medical'
      when coalesce(ms.medical_surgical, apr.medical_surgical) = 'P' then 'surgical'
      when coalesce(ms.medical_surgical, apr.medical_surgical) = 'surgical' then 'surgical'
      else 'acute inpatient - other'
    end as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as s
inner join {{ ref('service_category__stg_inpatient_institutional') }} as a
  on s.claim_id = a.claim_id
left outer join {{ ref('terminology__ms_drg') }} as ms
  on s.drg_code = ms.ms_drg_code
  and s.drg_code_type = 'ms-drg'
left outer join {{ ref('terminology__apr_drg') }} as apr
  on s.drg_code = apr.apr_drg_code
  and s.drg_code_type = 'apr-drg'
where
  ms.ms_drg_code is not null
  or apr.apr_drg_code is not null
