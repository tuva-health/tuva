{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('claims_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'medical_claim_line_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_recent_claim_date_sql = dq_date_literal_sql('2000-01-01') %}

{% set diagnosis_code_columns = [] %}
{% for index in range(1, 26) %}
    {% do diagnosis_code_columns.append('diagnosis_code_' ~ index) %}
{% endfor %}

{% set secondary_diagnosis_code_columns = [] %}
{% for index in range(2, 26) %}
    {% do secondary_diagnosis_code_columns.append('diagnosis_code_' ~ index) %}
{% endfor %}

{% set procedure_code_columns = [] %}
{% for index in range(1, 26) %}
    {% do procedure_code_columns.append('procedure_code_' ~ index) %}
{% endfor %}

{% set institutional_claim_where_sql = "cast(source_rows.claim_type as " ~ string_type ~ ") = 'institutional'" %}
{% set professional_claim_where_sql = "cast(source_rows.claim_type as " ~ string_type ~ ") = 'professional'" %}
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
{% set medical_claim_institutional_indicator_where_sql = dq_has_any_columns_populated_sql(['bill_type_code', 'drg_code', 'admit_type_code', 'admit_source_code', 'discharge_disposition_code', 'revenue_center_code'], 'source_rows') %}
{% set diagnosis_code_populated_where_sql = dq_has_any_columns_populated_sql(diagnosis_code_columns, 'source_rows') %}
{% set secondary_diagnosis_code_populated_where_sql = dq_has_any_columns_populated_sql(secondary_diagnosis_code_columns, 'source_rows') %}
{% set procedure_code_populated_where_sql = dq_has_any_columns_populated_sql(procedure_code_columns, 'source_rows') %}
{% set diagnosis_code_type_valid_where = "cast(source_rows.diagnosis_code_type as " ~ string_type ~ ") in ('icd-10-cm', 'icd-9-cm')" %}
{% set diagnosis_code_type_invalid_where = "source_rows.diagnosis_code_type is not null and cast(source_rows.diagnosis_code_type as " ~ string_type ~ ") not in ('icd-10-cm', 'icd-9-cm')" %}
{% set procedure_code_type_valid_where = "cast(source_rows.procedure_code_type as " ~ string_type ~ ") in ('icd-10-pcs', 'icd-9-pcs')" %}
{% set procedure_code_type_invalid_where = "source_rows.procedure_code_type is not null and cast(source_rows.procedure_code_type as " ~ string_type ~ ") not in ('icd-10-pcs', 'icd-9-pcs')" %}
{% set drg_code_type_valid_where = "cast(source_rows.drg_code_type as " ~ string_type ~ ") in ('ms-drg', 'apr-drg')" %}
{% set drg_code_type_invalid_where = "source_rows.drg_code_type is not null and cast(source_rows.drg_code_type as " ~ string_type ~ ") not in ('ms-drg', 'apr-drg')" %}

{% set inferred_claim_date_sql = "coalesce(source_rows.claim_line_start_date, source_rows.claim_start_date, source_rows.admission_date)" %}
{% set eligibility_match_applicable_where_sql =
    "source_rows.person_id is not null"
    ~ " and source_rows.member_id is not null"
    ~ " and source_rows.payer is not null"
    ~ " and source_rows." ~ quote_column('plan') ~ " is not null"
    ~ " and source_rows.data_source is not null"
    ~ " and " ~ inferred_claim_date_sql ~ " is not null"
%}
{% set no_matching_eligibility_where_sql %}
not exists (
    select 1
    from {{ ref('input_layer__eligibility') }} as eligibility_rows
    where eligibility_rows.person_id = source_rows.person_id
      and eligibility_rows.member_id = source_rows.member_id
      and eligibility_rows.payer = source_rows.payer
      and eligibility_rows.{{ quote_column('plan') }} = source_rows.{{ quote_column('plan') }}
      and eligibility_rows.data_source = source_rows.data_source
      and (
          {{ date_part('year', inferred_claim_date_sql) }} * 100
          + {{ date_part('month', inferred_claim_date_sql) }}
      ) between (
          {{ date_part('year', 'eligibility_rows.enrollment_start_date') }} * 100
          + {{ date_part('month', 'eligibility_rows.enrollment_start_date') }}
      ) and (
          {{ date_part('year', 'eligibility_rows.enrollment_end_date') }} * 100
          + {{ date_part('month', 'eligibility_rows.enrollment_end_date') }}
      )
)
{% endset %}

{% set diagnosis_code_union_queries = [] %}
{% for column_name in diagnosis_code_columns %}
    {% set query %}
        select
              source_rows._dq_claim_id_key
            , source_rows._dq_claim_line_number_key
            , source_rows._dq_data_source_key
            , {{ loop.index }} as diagnosis_position
            , cast(source_rows.diagnosis_code_type as {{ string_type }}) as diagnosis_code_type
            , replace(cast(source_rows.{{ quote_column(column_name) }} as {{ string_type }}), '.', '') as diagnosis_code
        from source_rows
        where source_rows.{{ quote_column(column_name) }} is not null
          and cast(source_rows.diagnosis_code_type as {{ string_type }}) in ('icd-10-cm', 'icd-9-cm')
    {% endset %}
    {% do diagnosis_code_union_queries.append(query | trim) %}
{% endfor %}

{% set procedure_code_union_queries = [] %}
{% for column_name in procedure_code_columns %}
    {% set query %}
        select
              source_rows._dq_claim_id_key
            , source_rows._dq_claim_line_number_key
            , source_rows._dq_data_source_key
            , {{ loop.index }} as procedure_position
            , cast(source_rows.procedure_code_type as {{ string_type }}) as procedure_code_type
            , replace(cast(source_rows.{{ quote_column(column_name) }} as {{ string_type }}), '.', '') as procedure_code
        from source_rows
        where source_rows.{{ quote_column(column_name) }} is not null
          and cast(source_rows.procedure_code_type as {{ string_type }}) in ('icd-10-pcs', 'icd-9-pcs')
    {% endset %}
    {% do procedure_code_union_queries.append(query | trim) %}
{% endfor %}

with source_rows as (
    select
          medical_claim_rows.*
        , medical_claim_rows.claim_id as _dq_claim_id_key
        , medical_claim_rows.claim_line_number as _dq_claim_line_number_key
        , medical_claim_rows.data_source as _dq_data_source_key
    from {{ ref('input_layer__medical_claim') }} as medical_claim_rows
),

diagnosis_codes as (
    {{ diagnosis_code_union_queries | join('\nunion all\n') }}
),

diagnosis_code_flags as (
    select
          diagnosis_codes._dq_claim_id_key
        , diagnosis_codes._dq_claim_line_number_key
        , diagnosis_codes._dq_data_source_key
        , cast(max(
            case
                when diagnosis_codes.diagnosis_position = 1
                 and diagnosis_codes.diagnosis_code_type = 'icd-10-cm'
                 and icd_10_diagnosis_lookup.icd_10_cm is null
                    then 1
                when diagnosis_codes.diagnosis_position = 1
                 and diagnosis_codes.diagnosis_code_type = 'icd-9-cm'
                 and icd_9_diagnosis_lookup.icd_9_cm is null
                    then 1
                else 0
            end
          ) as {{ dbt.type_int() }}) as diagnosis_code_1_invalid
        , cast(max(
            case
                when diagnosis_codes.diagnosis_position > 1
                 and diagnosis_codes.diagnosis_code_type = 'icd-10-cm'
                 and icd_10_diagnosis_lookup.icd_10_cm is null
                    then 1
                when diagnosis_codes.diagnosis_position > 1
                 and diagnosis_codes.diagnosis_code_type = 'icd-9-cm'
                 and icd_9_diagnosis_lookup.icd_9_cm is null
                    then 1
                else 0
            end
          ) as {{ dbt.type_int() }}) as diagnosis_code_2_to_25_invalid
    from diagnosis_codes
    left join {{ ref('terminology__icd_10_cm') }} as icd_10_diagnosis_lookup
        on diagnosis_codes.diagnosis_code_type = 'icd-10-cm'
       and diagnosis_codes.diagnosis_code = replace(cast(icd_10_diagnosis_lookup.icd_10_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_9_cm') }} as icd_9_diagnosis_lookup
        on diagnosis_codes.diagnosis_code_type = 'icd-9-cm'
       and diagnosis_codes.diagnosis_code = replace(cast(icd_9_diagnosis_lookup.icd_9_cm as {{ string_type }}), '.', '')
    group by
          diagnosis_codes._dq_claim_id_key
        , diagnosis_codes._dq_claim_line_number_key
        , diagnosis_codes._dq_data_source_key
),

procedure_codes as (
    {{ procedure_code_union_queries | join('\nunion all\n') }}
),

procedure_code_flags as (
    select
          procedure_codes._dq_claim_id_key
        , procedure_codes._dq_claim_line_number_key
        , procedure_codes._dq_data_source_key
        , cast(max(
            case
                when procedure_codes.procedure_code_type = 'icd-10-pcs'
                 and icd_10_procedure_lookup.icd_10_pcs is null
                    then 1
                when procedure_codes.procedure_code_type = 'icd-9-pcs'
                 and icd_9_procedure_lookup.icd_9_pcs is null
                    then 1
                else 0
            end
          ) as {{ dbt.type_int() }}) as procedure_code_1_to_25_invalid
    from procedure_codes
    left join {{ ref('terminology__icd_10_pcs') }} as icd_10_procedure_lookup
        on procedure_codes.procedure_code_type = 'icd-10-pcs'
       and procedure_codes.procedure_code = replace(cast(icd_10_procedure_lookup.icd_10_pcs as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_9_pcs') }} as icd_9_procedure_lookup
        on procedure_codes.procedure_code_type = 'icd-9-pcs'
       and procedure_codes.procedure_code = replace(cast(icd_9_procedure_lookup.icd_9_pcs as {{ string_type }}), '.', '')
    group by
          procedure_codes._dq_claim_id_key
        , procedure_codes._dq_claim_line_number_key
        , procedure_codes._dq_data_source_key
),

final as (
    select
          source_rows.claim_id
        , source_rows.claim_line_number
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_line_number <= 0",
            "source_rows.claim_line_number is not null"
          ) }} as claim_line_number_not_positive
        , {{ dq_logical_int_flag_sql("source_rows.claim_type is null", "1 = 1") }} as claim_type_null
        , {{ dq_logical_int_flag_sql(
            "cast(source_rows.claim_type as " ~ string_type ~ ") not in ('professional', 'institutional', 'undetermined')",
            "source_rows.claim_type is not null"
          ) }} as claim_type_invalid
        , {{ dq_logical_int_flag_sql(
            medical_claim_institutional_indicator_where_sql,
            professional_claim_where_sql
          ) }} as institutional_indicators_present_for_professional_claim
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null", "1 = 1") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.claim_start_date is null", "1 = 1") }} as claim_start_date_null
        , {{ dq_logical_int_flag_sql("source_rows.claim_end_date is null", "1 = 1") }} as claim_end_date_null
        , {{ dq_logical_int_flag_sql("source_rows.claim_line_start_date is null", "1 = 1") }} as claim_line_start_date_null
        , {{ dq_logical_int_flag_sql("source_rows.claim_line_end_date is null", "1 = 1") }} as claim_line_end_date_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_start_date < " ~ min_recent_claim_date_sql ~ " or source_rows.claim_start_date > " ~ current_date_sql,
            "source_rows.claim_start_date is not null"
          ) }} as claim_start_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_end_date < " ~ min_recent_claim_date_sql ~ " or source_rows.claim_end_date > " ~ current_date_sql,
            "source_rows.claim_end_date is not null"
          ) }} as claim_end_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_line_start_date < " ~ min_recent_claim_date_sql ~ " or source_rows.claim_line_start_date > " ~ current_date_sql,
            "source_rows.claim_line_start_date is not null"
          ) }} as claim_line_start_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_line_end_date < " ~ min_recent_claim_date_sql ~ " or source_rows.claim_line_end_date > " ~ current_date_sql,
            "source_rows.claim_line_end_date is not null"
          ) }} as claim_line_end_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_start_date > source_rows.claim_end_date",
            "source_rows.claim_start_date is not null and source_rows.claim_end_date is not null"
          ) }} as claim_start_after_claim_end
        , {{ dq_logical_int_flag_sql(
            "source_rows.claim_line_start_date > source_rows.claim_line_end_date",
            "source_rows.claim_line_start_date is not null and source_rows.claim_line_end_date is not null"
          ) }} as claim_line_start_after_claim_line_end
        , {{ dq_logical_int_flag_sql(
            "source_rows.admission_date > source_rows.discharge_date",
            "source_rows.admission_date is not null and source_rows.discharge_date is not null"
          ) }} as admission_date_after_discharge_date
        , {{ dq_logical_int_flag_sql(
            "source_rows.admission_date < " ~ min_recent_claim_date_sql ~ " or source_rows.admission_date > " ~ current_date_sql,
            "source_rows.admission_date is not null"
          ) }} as admission_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.admission_date is null",
            inpatient_facility_claim_where_sql
          ) }} as admission_date_null_for_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.discharge_date is null",
            inpatient_facility_claim_where_sql
          ) }} as discharge_date_null_for_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.discharge_date < " ~ min_recent_claim_date_sql ~ " or source_rows.discharge_date > " ~ current_date_sql,
            "source_rows.discharge_date is not null"
          ) }} as discharge_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.paid_date is null", "1 = 1") }} as paid_date_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_date < " ~ min_recent_claim_date_sql ~ " or source_rows.paid_date > " ~ current_date_sql,
            "source_rows.paid_date is not null"
          ) }} as paid_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_date < source_rows.claim_end_date",
            "source_rows.paid_date is not null and source_rows.claim_end_date is not null"
          ) }} as paid_date_before_claim_end_date
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.paid_amount is null", "1 = 1") }} as paid_amount_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_amount < 0",
            "source_rows.paid_amount is not null"
          ) }} as paid_amount_lt_zero
        , {{ dq_logical_int_flag_sql("source_rows.allowed_amount is null", "1 = 1") }} as allowed_amount_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.allowed_amount < 0",
            "source_rows.allowed_amount is not null"
          ) }} as allowed_amount_lt_zero
        , {{ dq_logical_int_flag_sql(
            "source_rows.paid_amount > source_rows.allowed_amount",
            "source_rows.paid_amount is not null and source_rows.allowed_amount is not null"
          ) }} as paid_amount_gt_allowed_amount
        , {{ dq_logical_int_flag_sql(
            "admit_source_lookup.admit_source_code is null",
            institutional_claim_where_sql ~ " and source_rows.admit_source_code is not null"
          ) }} as admit_source_code_invalid
        , {{ dq_logical_int_flag_sql(
            "admit_type_lookup.admit_type_code is null",
            institutional_claim_where_sql ~ " and source_rows.admit_type_code is not null"
          ) }} as admit_type_code_invalid
        , {{ dq_logical_int_flag_sql(
            "discharge_disposition_lookup.discharge_disposition_code is null",
            institutional_claim_where_sql ~ " and source_rows.discharge_disposition_code is not null"
          ) }} as discharge_disposition_code_invalid
        , {{ dq_logical_int_flag_sql(
            "place_of_service_lookup.place_of_service_code is null",
            professional_claim_where_sql ~ " and source_rows.place_of_service_code is not null"
          ) }} as place_of_service_code_invalid
        , {{ dq_logical_int_flag_sql(
            "bill_type_lookup.bill_type_code is null",
            institutional_claim_where_sql ~ " and source_rows.bill_type_code is not null"
          ) }} as bill_type_code_invalid
        , {{ dq_logical_int_flag_sql(
            "revenue_center_lookup.revenue_center_code is null",
            institutional_claim_where_sql ~ " and source_rows.revenue_center_code is not null"
          ) }} as revenue_center_code_invalid
        , {{ dq_logical_int_flag_sql(
            "source_rows.place_of_service_code is null",
            professional_claim_where_sql
          ) }} as place_of_service_code_null_for_professional_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.place_of_service_code is not null",
            institutional_claim_where_sql
          ) }} as place_of_service_code_present_for_institutional_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.bill_type_code is null",
            institutional_claim_where_sql
          ) }} as bill_type_code_null_for_institutional_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.revenue_center_code is null",
            institutional_claim_where_sql
          ) }} as revenue_center_code_null_for_institutional_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.hcpcs_code is null",
            professional_claim_where_sql
          ) }} as hcpcs_code_null_for_professional_claim
        , {{ dq_logical_int_flag_sql(
            "rendering_provider_lookup.npi is null",
            "source_rows.rendering_npi is not null"
          ) }} as rendering_npi_invalid
        , {{ dq_logical_int_flag_sql(
            "billing_provider_lookup.npi is null",
            "source_rows.billing_npi is not null"
          ) }} as billing_npi_invalid
        , {{ dq_logical_int_flag_sql(
            "facility_provider_lookup.npi is null",
            "source_rows.facility_npi is not null"
          ) }} as facility_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.rendering_npi is null", "1 = 1") }} as rendering_npi_null
        , {{ dq_logical_int_flag_sql("source_rows.billing_npi is null", "1 = 1") }} as billing_npi_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.facility_npi is null",
            inpatient_facility_claim_where_sql
          ) }} as facility_npi_null_for_inpatient_claim
        , {{ dq_logical_int_flag_sql(
            "source_rows.drg_code_type is null",
            "source_rows.drg_code is not null"
          ) }} as drg_code_type_null_when_drg_code_present
        , {{ dq_logical_int_flag_sql(
            drg_code_type_invalid_where,
            "source_rows.drg_code_type is not null"
          ) }} as drg_code_type_invalid
        , {{ dq_logical_int_flag_sql(
            "ms_drg_lookup.ms_drg_code is null and apr_drg_lookup.apr_drg_code is null",
            institutional_claim_where_sql ~ " and source_rows.drg_code is not null and " ~ drg_code_type_valid_where
          ) }} as drg_code_invalid
        , {{ dq_logical_int_flag_sql(
            "source_rows.drg_code is null",
            acute_inpatient_claim_where_sql
          ) }} as drg_code_null_for_acute_inpatient_claim
        , {{ dq_logical_int_flag_sql("source_rows.diagnosis_code_1 is null", "1 = 1") }} as diagnosis_code_1_null
        , {{ dq_logical_int_flag_sql(
            "source_rows.diagnosis_code_type is null",
            diagnosis_code_populated_where_sql
          ) }} as diagnosis_code_type_null_when_diagnosis_code_present
        , {{ dq_logical_int_flag_sql(
            diagnosis_code_type_invalid_where,
            "source_rows.diagnosis_code_type is not null"
          ) }} as diagnosis_code_type_invalid
        , {{ dq_logical_int_flag_sql(
            "coalesce(diagnosis_code_flags.diagnosis_code_1_invalid, 0) = 1",
            "source_rows.diagnosis_code_1 is not null and " ~ diagnosis_code_type_valid_where
          ) }} as diagnosis_code_1_invalid
        , {{ dq_logical_int_flag_sql(
            "coalesce(diagnosis_code_flags.diagnosis_code_2_to_25_invalid, 0) = 1",
            secondary_diagnosis_code_populated_where_sql ~ " and " ~ diagnosis_code_type_valid_where
          ) }} as diagnosis_code_2_to_25_invalid
        , {{ dq_logical_int_flag_sql(
            "source_rows.procedure_code_type is null",
            procedure_code_populated_where_sql
          ) }} as procedure_code_type_null_when_procedure_code_present
        , {{ dq_logical_int_flag_sql(
            procedure_code_type_invalid_where,
            "source_rows.procedure_code_type is not null"
          ) }} as procedure_code_type_invalid
        , {{ dq_logical_int_flag_sql(
            "coalesce(procedure_code_flags.procedure_code_1_to_25_invalid, 0) = 1",
            procedure_code_populated_where_sql ~ " and " ~ procedure_code_type_valid_where
          ) }} as procedure_code_1_to_25_invalid
        , {{ dq_logical_int_flag_sql(
            no_matching_eligibility_where_sql | trim,
            eligibility_match_applicable_where_sql
          ) }} as no_matching_eligibility_span
    from source_rows
    left join diagnosis_code_flags
        on (
            source_rows._dq_claim_id_key = diagnosis_code_flags._dq_claim_id_key
            or (source_rows._dq_claim_id_key is null and diagnosis_code_flags._dq_claim_id_key is null)
        )
       and (
            source_rows._dq_claim_line_number_key = diagnosis_code_flags._dq_claim_line_number_key
            or (source_rows._dq_claim_line_number_key is null and diagnosis_code_flags._dq_claim_line_number_key is null)
       )
       and (
            source_rows._dq_data_source_key = diagnosis_code_flags._dq_data_source_key
            or (source_rows._dq_data_source_key is null and diagnosis_code_flags._dq_data_source_key is null)
       )
    left join procedure_code_flags
        on (
            source_rows._dq_claim_id_key = procedure_code_flags._dq_claim_id_key
            or (source_rows._dq_claim_id_key is null and procedure_code_flags._dq_claim_id_key is null)
        )
       and (
            source_rows._dq_claim_line_number_key = procedure_code_flags._dq_claim_line_number_key
            or (source_rows._dq_claim_line_number_key is null and procedure_code_flags._dq_claim_line_number_key is null)
       )
       and (
            source_rows._dq_data_source_key = procedure_code_flags._dq_data_source_key
            or (source_rows._dq_data_source_key is null and procedure_code_flags._dq_data_source_key is null)
       )
    left join {{ ref('terminology__admit_source') }} as admit_source_lookup
        on cast(source_rows.admit_source_code as {{ string_type }}) = cast(admit_source_lookup.admit_source_code as {{ string_type }})
    left join {{ ref('terminology__admit_type') }} as admit_type_lookup
        on cast(source_rows.admit_type_code as {{ string_type }}) = cast(admit_type_lookup.admit_type_code as {{ string_type }})
    left join {{ ref('terminology__discharge_disposition') }} as discharge_disposition_lookup
        on cast(source_rows.discharge_disposition_code as {{ string_type }}) = cast(discharge_disposition_lookup.discharge_disposition_code as {{ string_type }})
    left join {{ ref('terminology__place_of_service') }} as place_of_service_lookup
        on cast(source_rows.place_of_service_code as {{ string_type }}) = cast(place_of_service_lookup.place_of_service_code as {{ string_type }})
    left join {{ ref('terminology__bill_type') }} as bill_type_lookup
        on cast(source_rows.bill_type_code as {{ string_type }}) = cast(bill_type_lookup.bill_type_code as {{ string_type }})
    left join {{ ref('terminology__revenue_center') }} as revenue_center_lookup
        on cast(source_rows.revenue_center_code as {{ string_type }}) = cast(revenue_center_lookup.revenue_center_code as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as rendering_provider_lookup
        on cast(source_rows.rendering_npi as {{ string_type }}) = cast(rendering_provider_lookup.npi as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as billing_provider_lookup
        on cast(source_rows.billing_npi as {{ string_type }}) = cast(billing_provider_lookup.npi as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as facility_provider_lookup
        on cast(source_rows.facility_npi as {{ string_type }}) = cast(facility_provider_lookup.npi as {{ string_type }})
    left join {{ ref('terminology__ms_drg') }} as ms_drg_lookup
        on cast(source_rows.drg_code_type as {{ string_type }}) = 'ms-drg'
       and cast(source_rows.drg_code as {{ string_type }}) = cast(ms_drg_lookup.ms_drg_code as {{ string_type }})
    left join {{ ref('terminology__apr_drg') }} as apr_drg_lookup
        on cast(source_rows.drg_code_type as {{ string_type }}) = 'apr-drg'
       and cast(source_rows.drg_code as {{ string_type }}) = cast(apr_drg_lookup.apr_drg_code as {{ string_type }})
)

select *
from final
