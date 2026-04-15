{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
      claim_id
    , person_id
    , paid_date
    , ndc_code
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__pharmacy_claim') }}
