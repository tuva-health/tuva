{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False))
 | as_bool
   )
}}

select *    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('quality_measures__measures') }} as p
