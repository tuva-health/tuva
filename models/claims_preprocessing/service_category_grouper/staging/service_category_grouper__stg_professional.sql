{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
    a.claim_id
  , a.claim_line_number
  , a.data_source
  , a.claim_line_id as claim_line_id
  , 'professional' as service_type
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
where a.claim_type = 'professional'
