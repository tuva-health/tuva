{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select distinct
    elig.person_id
    , {{ concat_custom([
        "elig.person_id",
        "coalesce(cast(elig.member_id as " ~ dbt.type_string() ~ "),'')",
        "coalesce(elig.data_source,'')",
        "coalesce(elig.payer,'')",
        "coalesce(elig." ~ quote_column('plan') ~ ",'')",
        "coalesce(cast(elig.enrollment_start_date as " ~ dbt.type_string() ~ "),'')",
        "coalesce(cast(elig.enrollment_end_date as " ~ dbt.type_string() ~ "),'')"
    ]) }} as person_id_key
    , ansi.ansi_fips_state_name as normalized_state_name
    , ansi.ansi_fips_state_code as fips_state_code
    , ansi.ansi_fips_state_abbreviation as fips_state_abbreviation
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('normalized_input__stg_eligibility') }} as elig
left outer join {{ ref('reference_data__ansi_fips_state') }} as ansi
  on (
       trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_abbreviation))
    or trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_code))
    or trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_name))
  )
