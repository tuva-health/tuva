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
    , institutional_snp_flag
    , long_term_institutional_flag
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

{% if var('use_synthetic_data') == true -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('eligibility_seed') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'eligibility') }}

{%- endif %}
