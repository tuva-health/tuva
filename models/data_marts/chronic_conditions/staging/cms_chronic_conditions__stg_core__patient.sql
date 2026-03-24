{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
    person_id
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__patient') }}
