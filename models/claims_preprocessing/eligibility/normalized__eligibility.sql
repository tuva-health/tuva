{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select
      {{ concat_custom([
          "elig.person_id",
          "'-'",
          "elig.member_id",
          "'-'",
          "date_norm.normalized_enrollment_start_date",
          "'-'",
          "date_norm.normalized_enrollment_end_date",
          "'-'",
          "elig.payer",
          "'-'",
          "elig." ~ quote_column('plan'),
          "'-'",
          "elig.data_source"
      ]) }} as eligibility_id
    , elig.person_id
    , elig.member_id
    , elig.subscriber_id
    , elig.subscriber_relation
    , date_norm.normalized_enrollment_start_date as enrollment_start_date
    , date_norm.normalized_enrollment_end_date as enrollment_end_date
    , elig.payer
    , elig.payer_type
    , elig.{{ quote_column('plan') }}
    , elig.first_name
    , elig.middle_name
    , elig.last_name
    , elig.name_suffix
    , elig.social_security_number
    , elig.address
    , elig.city
    , elig.state
    , elig.zip_code
    , elig.phone
    , elig.email
    , elig.ethnicity
    , elig.gender
    , elig.race
    , date_norm.normalized_birth_date as birth_date
    , date_norm.normalized_death_date as death_date
    , elig.death_flag
    , elig.original_reason_entitlement_code
    , elig.dual_status_code
    , elig.medicare_status_code
    , elig.enrollment_status
    , elig.hospice_flag
    , elig.institutional_snp_flag
    , elig.medicaid_indicator
    , elig.long_term_institutional_flag
    , elig.part_d_raf_type
    , elig.low_income_subsidy_indicator
    , elig.metal_level
    , elig.csr_indicator
    , elig.enrollment_duration_months
    , elig.esrd_status
    , elig.transplant_duration_months
    , elig.group_id
    , elig.group_name
    , state_norm.fips_state_code
    , state_norm.normalized_state_name
    , state_norm.fips_state_abbreviation
    {{ select_extension_columns(ref('int_eligibility_casting'), alias='elig', strip_prefix=false) }}
    , elig.file_date
    , elig.file_name
    , elig.ingest_datetime
    , elig.tuva_last_run
    , elig.data_source
from {{ ref('int_eligibility_casting') }} as elig
left outer join {{ ref('int_eligibility_dates_normalized') }} as date_norm
  on elig.person_id_key = date_norm.person_id_key
left outer join {{ ref('int_eligibility_state_normalized') }} as state_norm
  on elig.person_id_key = state_norm.person_id_key
