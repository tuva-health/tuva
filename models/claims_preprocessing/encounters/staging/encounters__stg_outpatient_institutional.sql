{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select distinct 
  a.claim_id
, 'Outpatient' as service_type
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('encounters__stg_medical_claim') }} a
left join {{ ref('encounters__stg_inpatient_institutional') }} i on a.claim_id = i.claim_id
where i.claim_id is null
and
a.claim_type = 'institutional'

