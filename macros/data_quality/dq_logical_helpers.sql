{% macro dq_current_date_sql() %}
    {{ return("cast(" ~ dbt.current_timestamp() ~ " as " ~ api.Column.translate_type('date') ~ ")") }}
{% endmacro %}

{% macro dq_date_literal_sql(date_string) %}
    {{ return(dbt.cast("'" ~ date_string ~ "'", api.Column.translate_type('date'))) }}
{% endmacro %}

{% macro dq_logical_display_name(table_name, test_name) %}
    {% set display_names = {
        'eligibility__birth_date_after_death_date': 'birth_date after death_date',
        'eligibility__multiple_birth_dates_per_person': 'birth_date has multiple values per person_id',
        'eligibility__birth_date_null': 'birth_date null',
        'eligibility__birth_date_out_of_reasonable_range': 'birth_date out of reasonable range',
        'eligibility__death_flag_invalid': 'death flag invalid',
        'eligibility__death_flag_without_death_date': 'death_flag indicates death without death_date',
        'eligibility__death_date_out_of_reasonable_range': 'death_date out of reasonable range',
        'eligibility__enrollment_start_after_end': 'enrollment_start_date after enrollment_end_date',
        'eligibility__gender_invalid': 'gender invalid',
        'eligibility__multiple_genders_per_person': 'gender has multiple values per person_id',
        'eligibility__gender_null': 'gender null',
        'eligibility__payer_type_invalid': 'payer_type invalid',
        'eligibility__payer_type_null': 'payer_type null',
        'eligibility__race_invalid': 'race invalid',
        'eligibility__multiple_races_per_person': 'race has multiple values per person_id',
        'eligibility__race_null': 'race null',
        'medical_claim__admission_date_after_discharge_date': 'admission_date after discharge_date',
        'medical_claim__admission_date_has_multiple_values_per_inpatient_claim': 'admission_date has multiple values per inpatient claim',
        'medical_claim__admission_date_out_of_reasonable_range': 'admission_date out of reasonable range',
        'medical_claim__admit_source_code_invalid': 'admit_source_code invalid',
        'medical_claim__admit_type_code_invalid': 'admit_type_code invalid',
        'medical_claim__allowed_amount_null': 'allowed_amount null',
        'medical_claim__allowed_amount_lt_zero': 'allowed_amount less than zero',
        'medical_claim__bill_type_code_count_ne_one_for_institutional_claim': 'bill_type_code has multiple values per institutional claim_id',
        'medical_claim__bill_type_code_invalid': 'bill_type_code invalid',
        'medical_claim__bill_type_code_null_for_institutional_claim': 'bill_type_code null for institutional claim',
        'medical_claim__billing_npi_invalid': 'billing_npi invalid',
        'medical_claim__billing_npi_null': 'billing_npi null',
        'medical_claim__claim_end_date_null': 'claim_end_date null',
        'medical_claim__claim_line_end_date_null': 'claim_line_end_date null',
        'medical_claim__claim_line_start_date_null': 'claim_line_start_date null',
        'medical_claim__claim_line_start_after_claim_line_end': 'claim_line_start_date after claim_line_end_date',
        'medical_claim__claim_start_after_claim_end': 'claim_start_date after claim_end_date',
        'medical_claim__claim_start_date_null': 'claim_start_date null',
        'medical_claim__claim_type_count_ne_one_per_claim': 'claim_type has multiple values per claim_id',
        'medical_claim__claim_type_invalid': 'claim_type invalid',
        'medical_claim__claim_type_null': 'claim_type null',
        'medical_claim__institutional_indicators_present_for_professional_claim': 'institutional indicators present for professional claim',
        'medical_claim__diagnosis_code_1_invalid': 'diagnosis_code_1 invalid',
        'medical_claim__diagnosis_code_1_null': 'diagnosis_code_1 null',
        'medical_claim__diagnosis_code_2_to_25_invalid': 'diagnosis_code_2 to diagnosis_code_25 invalid',
        'medical_claim__diagnosis_code_type_invalid': 'diagnosis_code_type invalid',
        'medical_claim__diagnosis_code_type_null_when_diagnosis_code_present': 'diagnosis_code_type null when diagnosis_code present',
        'medical_claim__discharge_disposition_code_invalid': 'discharge_disposition_code invalid',
        'medical_claim__discharge_date_has_multiple_values_per_inpatient_claim': 'discharge_date has multiple values per inpatient claim',
        'medical_claim__discharge_date_out_of_reasonable_range': 'discharge_date out of reasonable range',
        'medical_claim__drg_code_count_ne_one_for_acute_inpatient_claim': 'drg_code has multiple values per acute inpatient claim_id',
        'medical_claim__drg_code_invalid': 'drg_code invalid',
        'medical_claim__drg_code_null_for_acute_inpatient_claim': 'drg_code null for acute inpatient claim',
        'medical_claim__drg_code_type_invalid': 'drg_code_type invalid',
        'medical_claim__drg_code_type_null_when_drg_code_present': 'drg_code_type null when drg_code present',
        'medical_claim__admission_date_null_for_inpatient_claim': 'admission_date null for inpatient claim',
        'medical_claim__discharge_date_null_for_inpatient_claim': 'discharge_date null for inpatient claim',
        'medical_claim__facility_npi_invalid': 'facility_npi invalid',
        'medical_claim__facility_npi_null_for_inpatient_claim': 'facility_npi null for inpatient claim',
        'medical_claim__hcpcs_code_null_for_professional_claim': 'hcpcs_code null for professional claim',
        'medical_claim__no_matching_eligibility_span': 'no matching eligibility span',
        'medical_claim__paid_amount_null': 'paid_amount null',
        'medical_claim__paid_amount_gt_allowed_amount': 'paid_amount greater than allowed_amount',
        'medical_claim__paid_amount_lt_zero': 'paid_amount less than zero',
        'medical_claim__multiple_person_ids_per_claim': 'person_id has multiple values per claim',
        'medical_claim__person_id_null': 'person_id null',
        'medical_claim__place_of_service_code_invalid': 'place_of_service_code invalid',
        'medical_claim__place_of_service_code_present_for_institutional_claim': 'place_of_service_code present for institutional claim',
        'medical_claim__place_of_service_code_null_for_professional_claim': 'place_of_service_code null for professional claim',
        'medical_claim__procedure_code_1_to_25_invalid': 'procedure_code_1 to procedure_code_25 invalid',
        'medical_claim__procedure_code_type_invalid': 'procedure_code_type invalid',
        'medical_claim__procedure_code_type_null_when_procedure_code_present': 'procedure_code_type null when procedure_code present',
        'medical_claim__rendering_npi_invalid': 'rendering_npi invalid',
        'medical_claim__rendering_npi_null': 'rendering_npi null',
        'medical_claim__revenue_center_code_invalid': 'revenue_center_code invalid',
        'medical_claim__revenue_center_code_null_for_institutional_claim': 'revenue_center_code null for institutional claim',
        'pharmacy_claim__allowed_amount_null': 'allowed_amount null',
        'pharmacy_claim__allowed_amount_lt_zero': 'allowed_amount less than zero',
        'pharmacy_claim__dispensing_date_null': 'dispensing_date null',
        'pharmacy_claim__dispensing_provider_npi_invalid': 'dispensing_provider_npi invalid',
        'pharmacy_claim__dispensing_provider_npi_null': 'dispensing_provider_npi null',
        'pharmacy_claim__ndc_code_invalid': 'ndc_code invalid',
        'pharmacy_claim__ndc_code_null': 'ndc_code null',
        'pharmacy_claim__no_matching_eligibility_span': 'no matching eligibility span',
        'pharmacy_claim__paid_amount_null': 'paid_amount null',
        'pharmacy_claim__paid_amount_gt_allowed_amount': 'paid_amount greater than allowed_amount',
        'pharmacy_claim__paid_amount_lt_zero': 'paid_amount less than zero',
        'pharmacy_claim__paid_date_null': 'paid_date null',
        'pharmacy_claim__multiple_person_ids_per_claim': 'person_id has multiple values per claim',
        'pharmacy_claim__person_id_null': 'person_id null',
        'pharmacy_claim__prescribing_provider_npi_invalid': 'prescribing_provider_npi invalid',
        'pharmacy_claim__prescribing_provider_npi_null': 'prescribing_provider_npi null'
    } %}

    {{ return(display_names.get(test_name, test_name)) }}
{% endmacro %}

{% macro dq_logical_source_key_expression_sql(relation, relation_alias='source_rows') %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        {{ return("coalesce(cast(" ~ relation_alias ~ ".data_source as " ~ dbt.type_string() ~ "), '" ~ dq_source_key_sentinel() ~ "')") }}
    {% else %}
        {{ return("'" ~ dq_source_key_sentinel() ~ "'") }}
    {% endif %}
{% endmacro %}

{% macro dq_has_any_columns_populated_sql(column_names, relation_alias='source_rows') %}
    {% set clauses = [] %}

    {% for column_name in column_names %}
        {% do clauses.append(relation_alias ~ "." ~ quote_column(column_name) ~ " is not null") %}
    {% endfor %}

    {{ return("(" ~ clauses | join(" or ") ~ ")") }}
{% endmacro %}

{% macro dq_medical_claim_inpatient_facility_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12', '15', '16', '17', '18', '21', '22', '25', '26', '27', '28', '31', '41', '42', '45', '46', '47', '48', '61', '62', '65', '66', '67', '68', '82')"
    ) }}
{% endmacro %}

{% macro dq_medical_claim_acute_inpatient_where_sql(relation_alias='source_rows') %}
    {% set bill_type_prefix_expression = substring("cast(" ~ relation_alias ~ ".bill_type_code as " ~ dbt.type_string() ~ ")", 1, 2) %}

    {{ return(
        "lower(cast(" ~ relation_alias ~ ".claim_type as " ~ dbt.type_string() ~ ")) = 'institutional'"
        ~ " and " ~ relation_alias ~ ".bill_type_code is not null"
        ~ " and " ~ bill_type_prefix_expression ~ " in ('11', '12')"
    ) }}
{% endmacro %}

{% macro dq_logical_count_where_sql(relation, table_name, test_name, where_sql, distinct_expression=None) %}
    {% set source_key_expression = dq_logical_source_key_expression_sql(relation, 'source_rows') %}
    {% set count_expression = "count(*)" if distinct_expression is none else "count(distinct " ~ distinct_expression ~ ")" %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}

    select
          sources.data_source
        , '{{ table_name }}' as {{ adapter.quote('table') }}
        , '{{ display_name }}' as test_name
        , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
    from (
        {{ dq_source_dimension_sql(relation) }}
    ) as sources
    left join (
        select
              {{ source_key_expression }} as data_source_key
            , cast({{ count_expression }} as {{ dbt.type_int() }}) as test_result
        from {{ relation }} as source_rows
        where {{ where_sql }}
        group by 1
    ) as violations
        on sources.data_source_key = violations.data_source_key
{% endmacro %}

{% macro dq_logical_group_having_sql(relation, table_name, test_name, group_expression, having_sql, where_sql=None) %}
    {% set source_key_expression = dq_logical_source_key_expression_sql(relation, 'source_rows') %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}

    select
          sources.data_source
        , '{{ table_name }}' as {{ adapter.quote('table') }}
        , '{{ display_name }}' as test_name
        , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
    from (
        {{ dq_source_dimension_sql(relation) }}
    ) as sources
    left join (
        select
              grouped_rows.data_source_key
            , cast(count(*) as {{ dbt.type_int() }}) as test_result
        from (
            select
                  {{ source_key_expression }} as data_source_key
                , {{ group_expression }} as group_key
            from {{ relation }} as source_rows
            {% if where_sql is not none %}
            where {{ where_sql }}
            {% endif %}
            group by 1, 2
            having {{ having_sql }}
        ) as grouped_rows
        group by 1
    ) as violations
        on sources.data_source_key = violations.data_source_key
{% endmacro %}

{% macro dq_logical_lookup_count_sql(
    relation,
    table_name,
    test_name,
    source_expression,
    lookup_relation,
    lookup_expression,
    lookup_null_expression,
    distinct_expression=None,
    where_sql=None,
    extra_join_sql=None
) %}
    {% set source_key_expression = dq_logical_source_key_expression_sql(relation, 'source_rows') %}
    {% set count_expression = "count(*)" if distinct_expression is none else "count(distinct " ~ distinct_expression ~ ")" %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}

    select
          sources.data_source
        , '{{ table_name }}' as {{ adapter.quote('table') }}
        , '{{ display_name }}' as test_name
        , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
    from (
        {{ dq_source_dimension_sql(relation) }}
    ) as sources
    left join (
        select
              {{ source_key_expression }} as data_source_key
            , cast({{ count_expression }} as {{ dbt.type_int() }}) as test_result
        from {{ relation }} as source_rows
        left join {{ lookup_relation }} as lookup_rows
            on {{ source_expression }} = {{ lookup_expression }}
            {% if extra_join_sql is not none %}
            and {{ extra_join_sql }}
            {% endif %}
        where {{ source_expression }} is not null
          and {{ lookup_null_expression }} is null
          {% if where_sql is not none %}
          and {{ where_sql }}
          {% endif %}
        group by 1
    ) as violations
        on sources.data_source_key = violations.data_source_key
{% endmacro %}

{% macro dq_logical_claim_span_match_sql(
    claim_relation,
    table_name,
    test_name,
    eligibility_relation,
    claim_where_sql,
    match_sql
) %}
    {% set claim_source_key_expression = dq_logical_source_key_expression_sql(claim_relation, 'claim_rows') %}
    {% set eligibility_source_key_expression = dq_logical_source_key_expression_sql(eligibility_relation, 'eligibility_rows') %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}

    select
          sources.data_source
        , '{{ table_name }}' as {{ adapter.quote('table') }}
        , '{{ display_name }}' as test_name
        , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
    from (
        {{ dq_source_dimension_sql(claim_relation) }}
    ) as sources
    left join (
        select
              missing_claims.data_source_key
            , cast(count(distinct missing_claims.claim_id) as {{ dbt.type_int() }}) as test_result
        from (
            select distinct
                  {{ claim_source_key_expression }} as data_source_key
                , claim_rows.claim_id
            from {{ claim_relation }} as claim_rows
            where {{ claim_where_sql }}
              and not exists (
                  select 1
                  from {{ eligibility_relation }} as eligibility_rows
                  where {{ eligibility_source_key_expression }} = {{ claim_source_key_expression }}
                    and eligibility_rows.person_id = claim_rows.person_id
                    and {{ match_sql }}
              )
        ) as missing_claims
        group by 1
    ) as violations
        on sources.data_source_key = violations.data_source_key
{% endmacro %}

{% macro dq_logical_multi_column_code_lookup_sql(
    relation,
    table_name,
    test_name,
    code_columns,
    type_column,
    code_type_to_lookup_map,
    distinct_expression='claim_codes.claim_id',
    base_where_sql=None
) %}
    {% set source_key_expression = dq_logical_source_key_expression_sql(relation, 'source_rows') %}
    {% set union_queries = [] %}
    {% set display_name = dq_logical_display_name(table_name, test_name) %}

    {% for code_column in code_columns %}
        {% set query %}
            select
                  {{ source_key_expression }} as data_source_key
                , source_rows.claim_id as claim_id
                , lower(cast(source_rows.{{ quote_column(type_column) }} as {{ dbt.type_string() }})) as code_type
                , replace(cast(source_rows.{{ quote_column(code_column) }} as {{ dbt.type_string() }}), '.', '') as code_value
            from {{ relation }} as source_rows
            where source_rows.{{ quote_column(code_column) }} is not null
            {% if base_where_sql is not none %}
              and {{ base_where_sql }}
            {% endif %}
        {% endset %}
        {% do union_queries.append(query) %}
    {% endfor %}

    select
          sources.data_source
        , '{{ table_name }}' as {{ adapter.quote('table') }}
        , '{{ display_name }}' as test_name
        , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
    from (
        {{ dq_source_dimension_sql(relation) }}
    ) as sources
    left join (
        select
              claim_codes.data_source_key
            , cast(count(distinct {{ distinct_expression }}) as {{ dbt.type_int() }}) as test_result
        from (
            {{ union_queries | join('\nunion all\n') }}
        ) as claim_codes
        {% for code_map in code_type_to_lookup_map %}
        left join {{ ref(code_map['lookup_model']) }} as lookup_{{ loop.index }}
            on claim_codes.code_type = '{{ code_map['code_type'] }}'
           and claim_codes.code_value = replace(cast(lookup_{{ loop.index }}.{{ quote_column(code_map['lookup_column']) }} as {{ dbt.type_string() }}), '.', '')
        {% endfor %}
        where (
            {% for code_map in code_type_to_lookup_map %}
            (claim_codes.code_type = '{{ code_map['code_type'] }}'
             and lookup_{{ loop.index }}.{{ quote_column(code_map['lookup_column']) }} is null)
            {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
        group by 1
    ) as violations
        on sources.data_source_key = violations.data_source_key
{% endmacro %}
