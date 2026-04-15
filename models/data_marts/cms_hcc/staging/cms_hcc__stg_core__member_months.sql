{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}
-- Need distinct to deduplicate and remove the plan column
select distinct
      person_id
    , payer
    , year_month
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__member_months') }}
