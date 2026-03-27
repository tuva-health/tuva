
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the eligibility table in core.
-- *************************************************

{%- set tuva_core_columns -%}
       {{ concat_custom([
            "person_id",
            "'-'",
            "member_id",
            "'-'",
            "enrollment_start_date",
            "'-'",
            "enrollment_end_date",
            "'-'",
            "payer",
            "'-'",
            quote_column('plan'),
            "'-'",
            "data_source"
        ]) }} as eligibility_id
       , cast(person_id as {{ dbt.type_string() }}) as person_id
       , cast(member_id as {{ dbt.type_string() }}) as member_id
       , cast(subscriber_id as {{ dbt.type_string() }}) as subscriber_id
       , cast(birth_date as date) as birth_date
       , cast(death_date as date) as death_date
       , cast(enrollment_start_date as date) as enrollment_start_date
       , cast(enrollment_end_date as date) as enrollment_end_date
       , cast(payer as {{ dbt.type_string() }}) as payer
       , cast(payer_type as {{ dbt.type_string() }}) as payer_type
       , {{ quote_column('plan') }}
       , cast(original_reason_entitlement_code as {{ dbt.type_string() }}) as original_reason_entitlement_code
       , cast(dual_status_code as {{ dbt.type_string() }}) as dual_status_code
       , cast(medicare_status_code as {{ dbt.type_string() }}) as medicare_status_code
       , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
       , cast(hospice_flag as {{ dbt.type_int() }}) as hospice_flag
       , cast(institutional_snp_flag as {{ dbt.type_int() }}) as institutional_snp_flag
       , cast(long_term_institutional_flag as {{ dbt.type_int() }}) as long_term_institutional_flag
       , cast(subscriber_relation as {{ dbt.type_string() }}) as subscriber_relation
       , cast(group_id as {{ dbt.type_string() }}) as group_id
       , cast(group_name as {{ dbt.type_string() }}) as group_name
       , cast(normalized_state_name as {{ dbt.type_string() }}) as normalized_state_name
       , cast(fips_state_code as {{ dbt.type_string() }}) as fips_state_code
       , cast(fips_state_abbreviation as {{ dbt.type_string() }}) as fips_state_abbreviation
{%- endset -%}

{%- set tuva_metadata_columns -%}
       , cast(data_source as {{ dbt.type_string() }}) as data_source
       , cast(file_date as {{ dbt.type_timestamp() }}) as file_date
       , cast(file_name as {{ dbt.type_string() }}) as file_name
       , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
       , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__eligibility'), strip_prefix=false) }}
{%- endset %}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized_input__eligibility') }}
