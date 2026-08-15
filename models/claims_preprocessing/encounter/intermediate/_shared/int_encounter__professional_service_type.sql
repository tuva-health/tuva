{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select
  claim_id
, claim_line_number
, claim_line_id
, service_type
, data_source
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('int_service_category__professional') }} as a
