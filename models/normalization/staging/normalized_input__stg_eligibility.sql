{{ config(
     enabled = var('claims_enabled', False)
 | as_bool
   )
}}


select
      person_id
    , {{ concat_custom([
        "person_id",
        "coalesce(cast(member_id as " ~ dbt.type_string() ~ "),'')",
        "coalesce(data_source,'')",
        "coalesce(payer,'')",
        "coalesce(" ~ quote_column('plan') ~ ",'')",
        "coalesce(cast(enrollment_start_date as " ~ dbt.type_string() ~ "),'')",
        "coalesce(cast(enrollment_end_date as " ~ dbt.type_string() ~ "),'')"
    ]) }} as person_id_key
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
    , {{ quote_column('plan') }}
    , subscriber_relation
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , hospice_flag
    , snp_type
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
    , address
    , city
    , state
    , zip_code
    , phone
    , email
    , ethnicity
    , data_source
    , file_name
    , file_date
    , ingest_datetime
    {{ select_extension_columns(ref('input_layer__eligibility'), strip_prefix=false) }}
from {{ ref('input_layer__eligibility') }}
