{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
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
    {# , person_id as x_temp_person_id #}
    {# , first_name as x_temp_first_name #}
    {# , payer_type as zzz_temp_payer_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_date
    , file_name
    , ingest_datetime
{%- endset -%}

{%- if var('use_synthetic_data') == true -%}
  {%- set eligibility_relation = ref('eligibility_seed') -%}
{%- else -%}
  {%- set eligibility_relation = source('source_input', 'eligibility') -%}
{%- endif -%}

{%- if execute -%}
  {%- set eligibility_columns = adapter.get_columns_in_relation(eligibility_relation) -%}
  {%- set eligibility_column_names = eligibility_columns | map(attribute='name') | map('lower') | list -%}
{%- else -%}
  {%- set eligibility_column_names = [] -%}
{%- endif -%}

{# New nullable eligibility fields may be absent on older source_input schemas. #}
{%- set snp_type_expr -%}
  {%- if 'snp_type' in eligibility_column_names -%}
    snp_type
  {%- elif 'institutional_snp_flag' in eligibility_column_names -%}
    case when institutional_snp_flag = 1 then 'I-SNP' else null end
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set medicaid_indicator_expr -%}
  {%- if 'medicaid_indicator' in eligibility_column_names -%}
    medicaid_indicator
  {%- else -%}
    cast(null as {{ dbt.type_int() }})
  {%- endif -%}
{%- endset -%}

{%- set long_term_institutional_flag_expr -%}
  {%- if 'long_term_institutional_flag' in eligibility_column_names -%}
    long_term_institutional_flag
  {%- else -%}
    cast(null as {{ dbt.type_int() }})
  {%- endif -%}
{%- endset -%}

{%- set part_d_raf_type_expr -%}
  {%- if 'part_d_raf_type' in eligibility_column_names -%}
    part_d_raf_type
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set low_income_subsidy_indicator_expr -%}
  {%- if 'low_income_subsidy_indicator' in eligibility_column_names -%}
    low_income_subsidy_indicator
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set metal_level_expr -%}
  {%- if 'metal_level' in eligibility_column_names -%}
    metal_level
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set csr_indicator_expr -%}
  {%- if 'csr_indicator' in eligibility_column_names -%}
    csr_indicator
  {%- else -%}
    cast(null as {{ dbt.type_int() }})
  {%- endif -%}
{%- endset -%}

{%- set enrollment_duration_months_expr -%}
  {%- if 'enrollment_duration_months' in eligibility_column_names -%}
    enrollment_duration_months
  {%- else -%}
    cast(null as {{ dbt.type_int() }})
  {%- endif -%}
{%- endset -%}

{%- set esrd_status_expr -%}
  {%- if 'esrd_status' in eligibility_column_names -%}
    esrd_status
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set transplant_duration_months_expr -%}
  {%- if 'transplant_duration_months' in eligibility_column_names -%}
    transplant_duration_months
  {%- else -%}
    cast(null as {{ dbt.type_int() }})
  {%- endif -%}
{%- endset -%}

select
    {{ tuva_columns }}
    , {{ snp_type_expr }} as snp_type
    , {{ medicaid_indicator_expr }} as medicaid_indicator
    , {{ long_term_institutional_flag_expr }} as long_term_institutional_flag
    , {{ part_d_raf_type_expr }} as part_d_raf_type
    , {{ low_income_subsidy_indicator_expr }} as low_income_subsidy_indicator
    , {{ metal_level_expr }} as metal_level
    , {{ csr_indicator_expr }} as csr_indicator
    , {{ enrollment_duration_months_expr }} as enrollment_duration_months
    , {{ esrd_status_expr }} as esrd_status
    , {{ transplant_duration_months_expr }} as transplant_duration_months
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ eligibility_relation }}
