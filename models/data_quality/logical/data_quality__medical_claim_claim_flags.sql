{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('claims_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'medical_claim_claim_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set institutional_claim_where_sql = "cast(source_rows.claim_type as " ~ string_type ~ ") = 'institutional'" %}
{% set bill_type_prefix_expression = substring("cast(source_rows.bill_type_code as " ~ string_type ~ ")", 1, 2) %}
{% set inpatient_facility_claim_where_sql = institutional_claim_where_sql
    ~ " and source_rows.bill_type_code is not null"
    ~ " and " ~ bill_type_prefix_expression
    ~ " in ('11', '12', '15', '16', '17', '18', '21', '22', '25', '26', '27', '28', '31', '41', '42', '45', '46', '47', '48', '61', '62', '65', '66', '67', '68', '82')"
%}
{% set acute_inpatient_claim_where_sql = institutional_claim_where_sql
    ~ " and source_rows.bill_type_code is not null"
    ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12')"
%}

{% set diagnosis_code_columns = [] %}
{% set diagnosis_position_multiple_value_conditions = [] %}
{% set diagnosis_position_populated_conditions = [] %}
{% for index in range(1, 26) %}
    {% do diagnosis_code_columns.append('diagnosis_code_' ~ index) %}
    {% do diagnosis_position_multiple_value_conditions.append(
        'aggregated_claims.diagnosis_code_' ~ index ~ '_distinct_count > 1'
    ) %}
    {% do diagnosis_position_populated_conditions.append(
        'aggregated_claims.diagnosis_code_' ~ index ~ '_distinct_count > 0'
    ) %}
{% endfor %}

{% set procedure_code_columns = [] %}
{% for index in range(1, 26) %}
    {% do procedure_code_columns.append('procedure_code_' ~ index) %}
{% endfor %}

{% set diagnosis_code_populated_where_sql = dq_has_any_columns_populated_sql(diagnosis_code_columns, 'source_rows') %}
{% set procedure_code_populated_where_sql = dq_has_any_columns_populated_sql(procedure_code_columns, 'source_rows') %}

with source_rows as (
    select *
    from {{ ref('input_layer__medical_claim') }}
),

aggregated_claims as (
    select
          source_rows.claim_id
        , source_rows.data_source
        , cast(count(*) as {{ dbt.type_bigint() }}) as claim_line_count
        , cast(count(distinct source_rows.claim_type) as {{ dbt.type_bigint() }}) as claim_type_distinct_count
        , cast(count(source_rows.person_id) as {{ dbt.type_bigint() }}) as person_id_nonnull_count
        , cast(count(distinct case when source_rows.person_id is not null then source_rows.person_id end) as {{ dbt.type_bigint() }}) as person_id_distinct_count
        , cast(count(source_rows.member_id) as {{ dbt.type_bigint() }}) as member_id_nonnull_count
        , cast(count(distinct source_rows.member_id) as {{ dbt.type_bigint() }}) as member_id_distinct_count
        , cast(count(source_rows.payer) as {{ dbt.type_bigint() }}) as payer_nonnull_count
        , cast(count(distinct source_rows.payer) as {{ dbt.type_bigint() }}) as payer_distinct_count
        , cast(count(source_rows.{{ quote_column('plan') }}) as {{ dbt.type_bigint() }}) as plan_nonnull_count
        , cast(count(distinct source_rows.{{ quote_column('plan') }}) as {{ dbt.type_bigint() }}) as plan_distinct_count
        , cast(count(source_rows.claim_start_date) as {{ dbt.type_bigint() }}) as claim_start_date_nonnull_count
        , cast(count(distinct source_rows.claim_start_date) as {{ dbt.type_bigint() }}) as claim_start_date_distinct_count
        , cast(count(source_rows.claim_end_date) as {{ dbt.type_bigint() }}) as claim_end_date_nonnull_count
        , cast(count(distinct source_rows.claim_end_date) as {{ dbt.type_bigint() }}) as claim_end_date_distinct_count
        , cast(count(source_rows.billing_npi) as {{ dbt.type_bigint() }}) as billing_npi_nonnull_count
        , cast(count(distinct source_rows.billing_npi) as {{ dbt.type_bigint() }}) as billing_npi_distinct_count
        , cast(count(source_rows.facility_npi) as {{ dbt.type_bigint() }}) as facility_npi_nonnull_count
        , cast(count(distinct case when source_rows.facility_npi is not null then cast(source_rows.facility_npi as {{ string_type }}) end) as {{ dbt.type_bigint() }}) as facility_npi_distinct_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_claim_line_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} and source_rows.admission_date is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_admission_date_nonnull_count
        , cast(count(distinct case when {{ inpatient_facility_claim_where_sql }} and source_rows.admission_date is not null then source_rows.admission_date end) as {{ dbt.type_bigint() }}) as inpatient_admission_date_distinct_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} and source_rows.discharge_date is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_discharge_date_nonnull_count
        , cast(count(distinct case when {{ inpatient_facility_claim_where_sql }} and source_rows.discharge_date is not null then source_rows.discharge_date end) as {{ dbt.type_bigint() }}) as inpatient_discharge_date_distinct_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} and source_rows.admit_source_code is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_admit_source_code_nonnull_count
        , cast(count(distinct case when {{ inpatient_facility_claim_where_sql }} then source_rows.admit_source_code end) as {{ dbt.type_bigint() }}) as inpatient_admit_source_code_distinct_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} and source_rows.admit_type_code is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_admit_type_code_nonnull_count
        , cast(count(distinct case when {{ inpatient_facility_claim_where_sql }} then source_rows.admit_type_code end) as {{ dbt.type_bigint() }}) as inpatient_admit_type_code_distinct_count
        , cast(sum(cast(case when {{ inpatient_facility_claim_where_sql }} and source_rows.discharge_disposition_code is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as inpatient_discharge_disposition_code_nonnull_count
        , cast(count(distinct case when {{ inpatient_facility_claim_where_sql }} then source_rows.discharge_disposition_code end) as {{ dbt.type_bigint() }}) as inpatient_discharge_disposition_code_distinct_count
        , cast(sum(cast(case when {{ institutional_claim_where_sql }} then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as institutional_claim_line_count
        , cast(count(distinct case when {{ institutional_claim_where_sql }} then source_rows.bill_type_code end) as {{ dbt.type_bigint() }}) as institutional_bill_type_distinct_count
        , cast(sum(cast(case when {{ acute_inpatient_claim_where_sql }} then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as acute_inpatient_claim_line_count
        , cast(count(distinct case when {{ acute_inpatient_claim_where_sql }} then source_rows.drg_code end) as {{ dbt.type_bigint() }}) as acute_inpatient_drg_distinct_count
        , cast(sum(cast(case when {{ acute_inpatient_claim_where_sql }} and source_rows.drg_code_type is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as acute_inpatient_drg_code_type_nonnull_count
        , cast(count(distinct case when {{ acute_inpatient_claim_where_sql }} then cast(source_rows.drg_code_type as {{ string_type }}) end) as {{ dbt.type_bigint() }}) as acute_inpatient_drg_code_type_distinct_count
        , cast(sum(cast(case when {{ diagnosis_code_populated_where_sql }} then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as diagnosis_bearing_line_count
        , cast(sum(cast(case when {{ diagnosis_code_populated_where_sql }} and source_rows.diagnosis_code_type is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as diagnosis_code_type_nonnull_count
        , cast(count(distinct case when {{ diagnosis_code_populated_where_sql }} then cast(source_rows.diagnosis_code_type as {{ string_type }}) end) as {{ dbt.type_bigint() }}) as diagnosis_code_type_distinct_count
        {% for index in range(1, 26) %}
        , cast(count(distinct case
            when {{ institutional_claim_where_sql }}
             and source_rows.diagnosis_code_{{ index }} is not null
                then replace(cast(source_rows.diagnosis_code_{{ index }} as {{ string_type }}), '.', '')
            end) as {{ dbt.type_bigint() }}) as diagnosis_code_{{ index }}_distinct_count
        {% endfor %}
        , cast(sum(cast(case when {{ procedure_code_populated_where_sql }} then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as procedure_bearing_line_count
        , cast(sum(cast(case when {{ procedure_code_populated_where_sql }} and source_rows.procedure_code_type is not null then 1 else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as procedure_code_type_nonnull_count
        , cast(count(distinct case when {{ procedure_code_populated_where_sql }} then cast(source_rows.procedure_code_type as {{ string_type }}) end) as {{ dbt.type_bigint() }}) as procedure_code_type_distinct_count
    from source_rows
    group by
          source_rows.claim_id
        , source_rows.data_source
),

final as (
    select
          aggregated_claims.claim_id
        , aggregated_claims.data_source
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.claim_type_distinct_count <> 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.claim_type_distinct_count > 0"
          ) }} as claim_type_count_ne_one_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.person_id_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.person_id_nonnull_count > 0"
          ) }} as multiple_person_ids_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.member_id_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.member_id_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.member_id_nonnull_count > 0"
          ) }} as member_id_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.payer_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.payer_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.payer_nonnull_count > 0"
          ) }} as payer_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.plan_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.plan_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.plan_nonnull_count > 0"
          ) }} as plan_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.claim_start_date_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.claim_start_date_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.claim_start_date_nonnull_count > 0"
          ) }} as claim_start_date_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.claim_end_date_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.claim_end_date_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.claim_end_date_nonnull_count > 0"
          ) }} as claim_end_date_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.billing_npi_nonnull_count < aggregated_claims.claim_line_count or aggregated_claims.billing_npi_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.billing_npi_nonnull_count > 0"
          ) }} as billing_npi_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.facility_npi_distinct_count > 1",
            "aggregated_claims.claim_line_count > 1 and aggregated_claims.facility_npi_nonnull_count > 0"
          ) }} as facility_npi_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.inpatient_admission_date_distinct_count > 1",
            "aggregated_claims.inpatient_claim_line_count > 1 and aggregated_claims.inpatient_admission_date_nonnull_count > 0"
          ) }} as admission_date_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.inpatient_discharge_date_distinct_count > 1",
            "aggregated_claims.inpatient_claim_line_count > 1 and aggregated_claims.inpatient_discharge_date_nonnull_count > 0"
          ) }} as discharge_date_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.inpatient_admit_source_code_nonnull_count < aggregated_claims.inpatient_claim_line_count or aggregated_claims.inpatient_admit_source_code_distinct_count > 1",
            "aggregated_claims.inpatient_claim_line_count > 1 and aggregated_claims.inpatient_admit_source_code_nonnull_count > 0"
          ) }} as admit_source_code_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.inpatient_admit_type_code_nonnull_count < aggregated_claims.inpatient_claim_line_count or aggregated_claims.inpatient_admit_type_code_distinct_count > 1",
            "aggregated_claims.inpatient_claim_line_count > 1 and aggregated_claims.inpatient_admit_type_code_nonnull_count > 0"
          ) }} as admit_type_code_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.inpatient_discharge_disposition_code_nonnull_count < aggregated_claims.inpatient_claim_line_count or aggregated_claims.inpatient_discharge_disposition_code_distinct_count > 1",
            "aggregated_claims.inpatient_claim_line_count > 1 and aggregated_claims.inpatient_discharge_disposition_code_nonnull_count > 0"
          ) }} as discharge_disposition_code_has_multiple_values_per_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.institutional_bill_type_distinct_count <> 1",
            "aggregated_claims.institutional_claim_line_count > 1 and aggregated_claims.institutional_bill_type_distinct_count > 0"
          ) }} as bill_type_code_count_ne_one_for_institutional_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.acute_inpatient_drg_distinct_count <> 1",
            "aggregated_claims.acute_inpatient_claim_line_count > 1 and aggregated_claims.acute_inpatient_drg_distinct_count > 0"
          ) }} as drg_code_count_ne_one_for_acute_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.acute_inpatient_drg_code_type_nonnull_count < aggregated_claims.acute_inpatient_claim_line_count or aggregated_claims.acute_inpatient_drg_code_type_distinct_count > 1",
            "aggregated_claims.acute_inpatient_claim_line_count > 1 and aggregated_claims.acute_inpatient_drg_code_type_nonnull_count > 0"
          ) }} as drg_code_type_has_multiple_values_per_acute_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.diagnosis_code_type_nonnull_count < aggregated_claims.diagnosis_bearing_line_count or aggregated_claims.diagnosis_code_type_distinct_count > 1",
            "aggregated_claims.diagnosis_bearing_line_count > 1 and aggregated_claims.diagnosis_code_type_nonnull_count > 0"
          ) }} as diagnosis_code_type_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "aggregated_claims.procedure_code_type_nonnull_count < aggregated_claims.procedure_bearing_line_count or aggregated_claims.procedure_code_type_distinct_count > 1",
            "aggregated_claims.procedure_bearing_line_count > 1 and aggregated_claims.procedure_code_type_nonnull_count > 0"
          ) }} as procedure_code_type_has_multiple_values_per_claim
        , {{ dq_logical_int_flag_sql(
            "(" ~ (diagnosis_position_multiple_value_conditions | join(' or ')) ~ ")",
            "aggregated_claims.institutional_claim_line_count > 1 and (" ~ (diagnosis_position_populated_conditions | join(' or ')) ~ ")"
          ) }} as diagnosis_code_count_gt_one_per_position_for_institutional_claim
    from aggregated_claims
)

select *
from final
