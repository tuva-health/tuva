{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
-- Need distinct to deduplicate and remove the plan column
select distinct
      person_id
    , payer
    , enrollment_start_date
    , enrollment_end_date
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , institutional_snp_flag
    , data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__eligibility') }}
where lower(payer_type) = 'medicare'
   or payer_type is null
