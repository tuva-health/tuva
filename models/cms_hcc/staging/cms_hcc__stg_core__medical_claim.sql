{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
select
      claim_id
    , claim_line_number
    , claim_type
    , person_id
    , payer
    , claim_start_date
    , claim_end_date
    , bill_type_code
    , hcpcs_code
    , rendering_id
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__medical_claim') }}
