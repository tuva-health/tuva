{{ config(
     enabled = var('claims_enabled', False)
 | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , member_id
    , subscriber_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ the_tuva_project.quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , hospice_flag
    , cast(case when upper(coalesce(snp_type, '')) = 'I-SNP' then 1 else 0 end as {{ dbt.type_int() }}) as institutional_snp_flag
    , medicaid_indicator
    , long_term_institutional_flag
    , part_d_raf_type
    , low_income_subsidy_indicator
    , metal_level
    , csr_indicator
    , enrollment_duration_months
    , esrd_status
    , transplant_duration_months
    , group_id
    , group_name
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , social_security_number
    , subscriber_relation
    , address
    , city
    , state
    , zip_code
    , phone
    , email
    , ethnicity
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    , {{ dbt.concat([
        "'claims_'",
        "cast(person_id as " ~ dbt.type_string() ~ ")"
    ]) }} as x_temp_record_origin
    {# , first_name as x_temp_first_name #}
    {# , payer_type as zzz_temp_payer_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_date
    , file_name
    , ingest_datetime
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('raw_data__eligibility') }}
