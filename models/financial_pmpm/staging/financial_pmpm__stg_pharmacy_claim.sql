{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select
    person_id
    , member_id
    , dispensing_date
    , paid_date
    , paid_amount
    , allowed_amount
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__pharmacy_claim') }}
