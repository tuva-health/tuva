{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


select
    person_id
    , member_id
    , claim_id
    , claim_line_number
    , claim_start_date
    , claim_end_date
    , service_category_1
    , service_category_2
    , paid_amount
    , allowed_amount
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__medical_claim') }}
