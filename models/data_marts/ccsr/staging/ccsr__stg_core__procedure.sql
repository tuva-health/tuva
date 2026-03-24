{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False)) | as_bool
   )
}}

select
      encounter_id
    , claim_id
    , person_id
    , normalized_code
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__procedure') }}
