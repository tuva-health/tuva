{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_core_columns -%}
      eligibility_id
    , person_id
    , member_id
    , subscriber_id
    , birth_date
    , death_date
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , hospice_flag
    , institutional_snp_flag
    , long_term_institutional_flag
    , subscriber_relation
    , group_id
    , group_name
    , normalized_state_name
    , fips_state_code
    , fips_state_abbreviation
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__eligibility')) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , file_date
    , ingest_datetime
    , file_name
    , tuva_last_run
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('core__stg_claims_eligibility') }}
