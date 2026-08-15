{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct
    med.claim_id
  , med.claim_line_number
  , med.data_source
  , med.claim_line_id
  , 'ancillary' as service_category_1
  , 'durable medical equipment' as service_category_2
  , 'durable medical equipment' as service_category_3
  , '{{ this.name }}' as source_model_name
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('stg_service_category__medical_claim') }} as med
inner join {{ ref('int_service_category__professional') }} as prof
  on med.claim_id = prof.claim_id
  and med.claim_line_number = prof.claim_line_number
  and med.data_source = prof.data_source
where med.ccs_category = '243'
