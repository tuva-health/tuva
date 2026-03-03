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
  , cal_dob.full_date as normalized_birth_date
  , cal_death.full_date as normalized_death_date
  , cal_enroll_start.full_date as normalized_enrollment_start_date
  , cal_enroll_end.full_date as normalized_enrollment_end_date
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('normalized_input__stg_eligibility') }} as elig
left outer join {{ ref('reference_data__calendar') }} as cal_dob
    on elig.birth_date = cal_dob.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_death
    on elig.death_date = cal_death.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_enroll_start
    on elig.enrollment_start_date = cal_enroll_start.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_enroll_end
    on elig.enrollment_end_date = cal_enroll_end.full_date
