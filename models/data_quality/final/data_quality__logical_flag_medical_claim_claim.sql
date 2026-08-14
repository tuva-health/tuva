{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('claims_enabled', false) | as_bool)
   )
}}

{% set string_type = dbt.type_string() %}
{% set source_sentinel = dq_source_key_sentinel() %}
{% set institutional_claim_where_sql = "lower(cast(source_rows.claim_type as " ~ string_type ~ ")) = 'institutional'" %}
{% set inpatient_facility_claim_where_sql = dq_medical_claim_inpatient_facility_where_sql('source_rows') %}
{% set acute_inpatient_claim_where_sql = dq_medical_claim_acute_inpatient_where_sql('source_rows') %}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__medical_claim') }}
),

aggregated_claims as (
    select
          source_rows.claim_id
        , source_rows.data_source
        , count(distinct source_rows.claim_type) as claim_type_distinct_count
        , count(distinct case when source_rows.person_id is not null then source_rows.person_id end) as person_id_distinct_count
        , count(distinct case when source_rows.facility_npi is not null then cast(source_rows.facility_npi as {{ string_type }}) end) as facility_npi_distinct_count
        , cast(sum(case when {{ inpatient_facility_claim_where_sql }} then 1 else 0 end) as {{ dbt.type_int() }}) as inpatient_claim_line_count
        , count(distinct case when {{ inpatient_facility_claim_where_sql }} and source_rows.admission_date is not null then source_rows.admission_date end) as inpatient_admission_date_distinct_count
        , count(distinct case when {{ inpatient_facility_claim_where_sql }} and source_rows.discharge_date is not null then source_rows.discharge_date end) as inpatient_discharge_date_distinct_count
        , cast(sum(case when {{ institutional_claim_where_sql }} then 1 else 0 end) as {{ dbt.type_int() }}) as institutional_claim_line_count
        , count(distinct case when {{ institutional_claim_where_sql }} then source_rows.bill_type_code end) as institutional_bill_type_distinct_count
        , cast(sum(case when {{ acute_inpatient_claim_where_sql }} then 1 else 0 end) as {{ dbt.type_int() }}) as acute_inpatient_claim_line_count
        , count(distinct case when {{ acute_inpatient_claim_where_sql }} then source_rows.drg_code end) as acute_inpatient_drg_distinct_count
    from source_rows
    group by
          source_rows.claim_id
        , source_rows.data_source
),

missing_eligibility_claims as (
    select distinct
          claim_rows.claim_id
        , claim_rows.data_source
    from source_rows as claim_rows
    where claim_rows.person_id is not null
      and (claim_rows.claim_start_date is not null or claim_rows.claim_end_date is not null)
      and not exists (
          select 1
          from {{ ref('stg_input_layer__eligibility') }} as eligibility_rows
          where coalesce(cast(eligibility_rows.data_source as {{ string_type }}), '{{ source_sentinel }}') = coalesce(cast(claim_rows.data_source as {{ string_type }}), '{{ source_sentinel }}')
            and eligibility_rows.person_id = claim_rows.person_id
            and (
                coalesce(claim_rows.claim_end_date, claim_rows.claim_start_date) >= eligibility_rows.enrollment_start_date
                and coalesce(claim_rows.claim_start_date, claim_rows.claim_end_date) <= eligibility_rows.enrollment_end_date
            )
      )
),

institutional_diagnosis_position_counts as (
    {% for i in range(1, 26) %}
    select
          source_rows.claim_id
        , source_rows.data_source
        , 'diagnosis_code_{{ i }}' as diagnosis_column
        , count(distinct case
            when source_rows.diagnosis_code_{{ i }} is not null
                then replace(cast(source_rows.diagnosis_code_{{ i }} as {{ string_type }}), '.', '')
            end) as diagnosis_code_distinct_count
    from source_rows
    where {{ institutional_claim_where_sql }}
    group by
          source_rows.claim_id
        , source_rows.data_source
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

institutional_diagnosis_claims as (
    select
          claim_id
        , data_source
        , max(diagnosis_code_distinct_count) as max_diagnosis_code_distinct_count
    from institutional_diagnosis_position_counts
    group by
          claim_id
        , data_source
),

final as (
    select
          aggregated_claims.claim_id
        , aggregated_claims.data_source
        , {{ dq_logical_int_flag_sql("aggregated_claims.claim_type_distinct_count <> 1") }} as claim_type_count_ne_one_per_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.person_id_distinct_count > 1") }} as multiple_person_ids_per_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.facility_npi_distinct_count > 1") }} as facility_npi_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.inpatient_claim_line_count > 0 and aggregated_claims.inpatient_admission_date_distinct_count > 1") }} as admission_date_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.inpatient_claim_line_count > 0 and aggregated_claims.inpatient_discharge_date_distinct_count > 1") }} as discharge_date_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.institutional_claim_line_count > 0 and aggregated_claims.institutional_bill_type_distinct_count <> 1") }} as bill_type_code_count_ne_one_for_institutional_claim
        , {{ dq_logical_int_flag_sql("aggregated_claims.acute_inpatient_claim_line_count > 0 and aggregated_claims.acute_inpatient_drg_distinct_count <> 1") }} as drg_code_count_ne_one_for_acute_inpatient_claim
        , {{ dq_logical_int_flag_sql("coalesce(institutional_diagnosis_claims.max_diagnosis_code_distinct_count, 0) > 1") }} as diagnosis_code_count_gt_one_per_position_for_institutional_claim
        , {{ dq_logical_int_flag_sql("missing_eligibility_claims.claim_id is not null") }} as no_matching_eligibility_span
    from aggregated_claims
    left join missing_eligibility_claims
        on aggregated_claims.claim_id = missing_eligibility_claims.claim_id
       and (
            aggregated_claims.data_source = missing_eligibility_claims.data_source
            or (
                aggregated_claims.data_source is null
                and missing_eligibility_claims.data_source is null
            )
       )
    left join institutional_diagnosis_claims
        on aggregated_claims.claim_id = institutional_diagnosis_claims.claim_id
       and (
            aggregated_claims.data_source = institutional_diagnosis_claims.data_source
            or (
                aggregated_claims.data_source is null
                and institutional_diagnosis_claims.data_source is null
            )
       )
)

select *
from final
