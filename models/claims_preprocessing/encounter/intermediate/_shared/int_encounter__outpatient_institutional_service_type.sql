{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}


select
  claim_id
, service_type
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('int_service_category__outpatient_institutional') }} as a
