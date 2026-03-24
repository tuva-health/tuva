{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}
-- NOTE: Need distinct since condition_rank is not included
select distinct
      claim_id
    , person_id
    , payer
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__condition') }}
