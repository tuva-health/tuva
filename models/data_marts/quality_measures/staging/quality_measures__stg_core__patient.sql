{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False)) | as_bool
   )
}}
select
      person_id
    , sex
    , birth_date
    , death_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__patient') }}
