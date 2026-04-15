{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
      claim_id
    , person_id
    , procedure_date
    , normalized_code_type
    , normalized_code
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__procedure') }}
where claim_id is not null
