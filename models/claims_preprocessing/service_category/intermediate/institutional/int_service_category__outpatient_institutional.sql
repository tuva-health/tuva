{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    a.claim_id
  , a.data_source
  , 'outpatient' as service_type
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('stg_service_category__medical_claim') }} as a
left outer join {{ ref('int_service_category__inpatient_institutional') }} as i
  on a.claim_id = i.claim_id
  and a.data_source = i.data_source
where i.claim_id is null
  and a.claim_type = 'institutional'
