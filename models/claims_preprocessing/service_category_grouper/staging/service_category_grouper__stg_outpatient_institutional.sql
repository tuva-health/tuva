{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    a.claim_id
  , a.data_source
  , 'outpatient' as service_type
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category_grouper__stg_medical_claim') }} as a
left outer join {{ ref('service_category_grouper__stg_inpatient_institutional') }} as i
  on a.claim_id = i.claim_id
  and a.data_source = i.data_source
where i.claim_id is null
  and a.claim_type = 'institutional'
