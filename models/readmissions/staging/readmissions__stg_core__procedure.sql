{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
  encounter_id
, payer
, {{ quote_column('plan') }}
, normalized_code
, normalized_code_type
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__procedure') }}
