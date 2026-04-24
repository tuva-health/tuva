{% macro dq_expected_input_layer_models() %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set models = [] %}
    {% for graph_node in graph['nodes'].values() | sort(attribute='name') %}
        {% if graph_node.resource_type == 'model'
              and graph_node.package_name == 'the_tuva_project'
              and graph_node.original_file_path.startswith('models/input_layer/')
              and graph_node.name.startswith('input_layer__') %}
            {% do models.append(graph_node) %}
        {% endif %}
    {% endfor %}
    {{ return(models) }}
{% endmacro %}

{% macro dq_claims_structural_model_names() %}
    {{ return([
        'input_layer__eligibility',
        'input_layer__medical_claim',
        'input_layer__pharmacy_claim'
    ]) }}
{% endmacro %}

{% macro dq_expected_final_marts() %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set models = [] %}
    {% for graph_node in graph['nodes'].values() | sort(attribute='name') %}
        {% if graph_node.resource_type == 'model'
              and graph_node.package_name == 'the_tuva_project'
              and graph_node.original_file_path.startswith('models/data_marts/')
              and '/final/' in graph_node.original_file_path %}
            {% do models.append(graph_node) %}
        {% endif %}
    {% endfor %}
    {{ return(models) }}
{% endmacro %}

{% macro dq_find_model_node(model_name) %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% for graph_node in graph['nodes'].values() %}
        {% if graph_node.resource_type == 'model'
              and graph_node.package_name == 'the_tuva_project'
              and graph_node.name == model_name %}
            {{ return(graph_node) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}

{% macro dq_actual_relation(node) %}
    {% if node is none %}
        {{ return(none) }}
    {% endif %}

    {{ return(
        adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias
        )
    ) }}
{% endmacro %}

{% macro dq_actual_columns(relation) %}
    {% if relation is none %}
        {{ return([]) }}
    {% endif %}

    {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}

{% macro dq_has_column(columns, column_name) %}
    {% set requested_name = column_name | lower %}
    {% for column in columns %}
        {% if column.name | lower == requested_name %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}
    {{ return(false) }}
{% endmacro %}

{% macro dq_expected_columns(node) %}
    {% set expected_columns = [] %}

    {% for column in node.columns.values() | sort(attribute='name') %}
        {% set meta = column.config.meta if column.config is not none and column.config.meta is not none else {} %}
        {% do expected_columns.append(
            {
                'name': column.name | lower,
                'data_type': meta.get('data_type'),
                'is_primary_key': meta.get('is_primary_key', false)
            }
        ) %}
    {% endfor %}

    {{ return(expected_columns) }}
{% endmacro %}

{% macro dq_expected_pk_columns(node) %}
    {% set pk_columns = [] %}

    {% for column in dq_expected_columns(node) %}
        {% if column['is_primary_key'] %}
            {% do pk_columns.append(column['name']) %}
        {% endif %}
    {% endfor %}

    {{ return(pk_columns) }}
{% endmacro %}

{% macro dq_source_key_sentinel() %}
    {{ return('__dq_null__') }}
{% endmacro %}

{% macro dq_empty_row_sql() %}
    select 1 as _dq_empty_row
{% endmacro %}

{% macro dq_empty_result_guard_sql() %}
    from (
        {{ dq_empty_row_sql() }}
    ) as dq_empty_row
    where 1 = 0
{% endmacro %}

{% macro dq_grouped_rowcount_sql(relation, group_cols=[]) %}
    select
        {% for column_name in group_cols %}
            cast({{ quote_column(column_name) }} as {{ dbt.type_string() }}) as {{ column_name }},
        {% endfor %}
        cast(count(*) as {{ dbt.type_numeric() }}) as row_count
    from {{ relation }}
    {% if group_cols | length > 0 %}
    group by
        {% for _ in group_cols %}
            {{ loop.index }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro dq_source_row_count_sql(relation) %}
    {% set actual_columns = dq_actual_columns(relation) %}

    select
          {% if dq_has_column(actual_columns, 'data_source') %}
          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
          {% else %}
          '{{ dq_source_key_sentinel() }}'
          {% endif %} as data_source_key
        , cast(count(*) as {{ dbt.type_numeric() }}) as row_count
    from {{ relation }}
    {% if dq_has_column(actual_columns, 'data_source') %}
    group by 1
    {% endif %}
{% endmacro %}

{% macro dq_source_dimension_sql(relation) %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        select distinct
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , cast(data_source as {{ dbt.type_string() }}) as data_source
        from {{ relation }}

        union all

        select
              '{{ dq_source_key_sentinel() }}' as data_source_key
            , cast(null as {{ dbt.type_string() }}) as data_source
        from (
            {{ dq_empty_row_sql() }}
        ) as dq_empty_source
        where not exists (
            select 1
            from {{ relation }}
        )
    {% else %}
        select
              '{{ dq_source_key_sentinel() }}' as data_source_key
            , cast(null as {{ dbt.type_string() }}) as data_source
    {% endif %}
{% endmacro %}

{% macro dq_missing_source_dimension_sql() %}
    select
          '{{ dq_source_key_sentinel() }}' as data_source_key
        , cast(null as {{ dbt.type_string() }}) as data_source
{% endmacro %}

{% macro dq_source_key_expression_sql(relation, relation_alias='source_rows') %}
    {% set actual_columns = dq_actual_columns(relation) %}

    {% if dq_has_column(actual_columns, 'data_source') %}
        {{ return("coalesce(cast(" ~ relation_alias ~ ".data_source as " ~ dbt.type_string() ~ "), '" ~ dq_source_key_sentinel() ~ "')") }}
    {% else %}
        {{ return("'" ~ dq_source_key_sentinel() ~ "'") }}
    {% endif %}
{% endmacro %}

{% macro dq_duplicate_pk_count_sql(relation, pk_cols) %}
    {% set actual_columns = dq_actual_columns(relation) %}
    {% set has_data_source = dq_has_column(actual_columns, 'data_source') %}

    select
          duplicate_groups.data_source_key
        , cast(sum(duplicate_groups.duplicate_row_count) as {{ dbt.type_numeric() }}) as duplicate_pk_count
    from (
        select
              {% if has_data_source %}
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
              {% else %}
              '{{ dq_source_key_sentinel() }}'
              {% endif %} as data_source_key
            , cast(count(*) - 1 as {{ dbt.type_numeric() }}) as duplicate_row_count
        from {{ relation }}
        where
            {% for pk_col in pk_cols %}
                {{ quote_column(pk_col) }} is not null{% if not loop.last %} and {% endif %}
            {% endfor %}
        group by
            {% if has_data_source %}
                1,
            {% endif %}
            {% for pk_col in pk_cols %}
                {{ quote_column(pk_col) }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        having count(*) > 1
    ) as duplicate_groups
    group by duplicate_groups.data_source_key
{% endmacro %}

{% macro dq_pk_null_count_sql(relation, pk_cols) %}
    {% set actual_columns = dq_actual_columns(relation) %}
    {% set has_data_source = dq_has_column(actual_columns, 'data_source') %}

    select
          {% if has_data_source %}
          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
          {% else %}
          '{{ dq_source_key_sentinel() }}'
          {% endif %} as data_source_key
        , cast(sum(
            case
                when
                    {% for pk_col in pk_cols %}
                        {{ quote_column(pk_col) }} is null{% if not loop.last %} or {% endif %}
                    {% endfor %}
                then 1
                else 0
            end
          ) as {{ dbt.type_numeric() }}) as null_pk_count
    from {{ relation }}
    {% if has_data_source %}
    group by 1
    {% endif %}
{% endmacro %}

{% macro dq_base_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% set compact = normalized | replace(' ', '') %}
    {% set base = normalized.split('(')[0] | trim %}

    {% if base in ['varchar', 'nvarchar', 'string', 'text', 'char', 'character', 'character varying'] %}
        {{ return('string') }}
    {% elif base in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'int64'] %}
        {{ return('integer') }}
    {% elif base in ['boolean', 'bool', 'bit'] %}
        {{ return('boolean') }}
    {% elif base == 'date' %}
        {{ return('date') }}
    {% elif 'timestamp' in base or base in ['datetime', 'datetime2', 'smalldatetime'] %}
        {{ return('timestamp') }}
    {% elif base in ['number', 'numeric', 'decimal', 'float', 'float4', 'float8', 'double', 'double precision', 'real', 'float64', 'bignumeric'] %}
        {% if '(' in compact and ')' in compact and ',' in compact %}
            {% set scale = compact.split('(', 1)[1].split(')', 1)[0].split(',')[-1] | int %}
            {% if scale == 0 %}
                {{ return('integer') }}
            {% endif %}
        {% endif %}
        {{ return('numeric') }}
    {% else %}
        {{ return(base) }}
    {% endif %}
{% endmacro %}

{% macro dq_type_family(type_string) %}
    {{ return(adapter.dispatch('dq_type_family', 'the_tuva_project')(type_string)) }}
{% endmacro %}

{% macro dq_type_families_match(expected_type, actual_type) %}
    {% set expected_family = dq_type_family(expected_type) %}
    {% set actual_family = dq_type_family(actual_type) %}

    {% if expected_family == actual_family %}
        {{ return(true) }}
    {% elif expected_family == 'boolean' and actual_family == 'integer' %}
        {{ return(true) }}
    {% elif expected_family == 'numeric' and actual_family == 'integer' %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}

{% macro default__dq_type_family(type_string) %}
    {{ return(dq_base_type_family(type_string)) }}
{% endmacro %}

{% macro bigquery__dq_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% if normalized == 'bool' %}
        {{ return('boolean') }}
    {% elif normalized == 'bytes' %}
        {{ return('string') }}
    {% else %}
        {{ return(dq_base_type_family(normalized)) }}
    {% endif %}
{% endmacro %}

{% macro fabric__dq_type_family(type_string) %}
    {% if type_string is none %}
        {{ return('unknown') }}
    {% endif %}

    {% set normalized = type_string | lower | trim %}
    {% if normalized in ['nvarchar', 'varchar', 'char', 'nchar'] %}
        {{ return('string') }}
    {% else %}
        {{ return(dq_base_type_family(normalized)) }}
    {% endif %}
{% endmacro %}

{% macro dq_representative_data_marts() %}
    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set representatives = [
        {'data_mart_name': 'ahrq_measures', 'model_name': 'ahrq_measures__pqi_summary'},
        {'data_mart_name': 'ccsr', 'model_name': 'ccsr__procedure_summary'},
        {'data_mart_name': 'chronic_conditions', 'model_name': 'chronic_conditions__cms_chronic_conditions_wide'},
        {'data_mart_name': 'cms_hcc', 'model_name': 'cms_hcc__patient_risk_scores'},
        {'data_mart_name': 'ed_classification', 'model_name': 'ed_classification__summary'},
        {'data_mart_name': 'financial_pmpm', 'model_name': 'financial_pmpm__pmpm_payer'},
        {'data_mart_name': 'hcc_recapture', 'model_name': 'hcc_recapture__recapture_rates'},
        {'data_mart_name': 'hcc_suspecting', 'model_name': 'hcc_suspecting__summary'},
        {'data_mart_name': 'pharmacy', 'model_name': 'pharmacy__brand_generic_opportunity'},
        {'data_mart_name': 'provider_attribution', 'model_name': 'provider_attribution__provider_ranking'},
        {'data_mart_name': 'quality_measures', 'model_name': 'quality_measures__summary_wide'},
        {'data_mart_name': 'readmissions', 'model_name': 'readmissions__readmission_summary'},
        {'data_mart_name': 'semantic_layer', 'model_name': 'semantic_layer__fact_member_months'}
    ] %}

    {% set marts = [] %}
    {% for representative in representatives %}
        {% set node = dq_find_model_node(representative['model_name']) %}
        {% if node is not none %}
            {% do marts.append({
                'data_mart_name': representative['data_mart_name'],
                'model_name': representative['model_name'],
                'node': node
            }) %}
        {% endif %}
    {% endfor %}

    {{ return(marts) }}
{% endmacro %}

{% macro dq_logical_rules() %}
    {{ return([
        {
            'table_name': 'medical_claim',
            'test_name': 'medical_claim__claim_start_after_claim_end',
            'model_name': 'input_layer__medical_claim',
            'rule_type': 'date_order',
            'left_column': 'claim_start_date',
            'right_column': 'claim_end_date'
        },
        {
            'table_name': 'medical_claim',
            'test_name': 'medical_claim__admit_after_discharge',
            'model_name': 'input_layer__medical_claim',
            'rule_type': 'date_order',
            'left_column': 'admission_date',
            'right_column': 'discharge_date'
        },
        {
            'table_name': 'medical_claim',
            'test_name': 'medical_claim__paid_amount_gt_allowed_amount',
            'model_name': 'input_layer__medical_claim',
            'rule_type': 'amount_lte',
            'left_column': 'paid_amount',
            'right_column': 'allowed_amount'
        },
        {
            'table_name': 'pharmacy_claim',
            'test_name': 'pharmacy_claim__paid_amount_gt_allowed_amount',
            'model_name': 'input_layer__pharmacy_claim',
            'rule_type': 'amount_lte',
            'left_column': 'paid_amount',
            'right_column': 'allowed_amount'
        },
        {
            'table_name': 'eligibility',
            'test_name': 'eligibility__enrollment_start_after_end',
            'model_name': 'input_layer__eligibility',
            'rule_type': 'date_order',
            'left_column': 'enrollment_start_date',
            'right_column': 'enrollment_end_date'
        },
        {
            'table_name': 'eligibility',
            'test_name': 'eligibility__death_before_birth',
            'model_name': 'input_layer__eligibility',
            'rule_type': 'date_order',
            'left_column': 'death_date',
            'right_column': 'birth_date',
            'operator': '<'
        }
    ]) }}
{% endmacro %}

{% macro dq_analytical_metric_is_count_sql(metric_expression) %}
    (
        lower({{ metric_expression }}) like 'count %'
        or lower({{ metric_expression }}) like 'members w/%'
        or lower({{ metric_expression }}) in (
            'total member months',
            'max member months',
            'index admissions',
            '30-day readmissions'
        )
    )
{% endmacro %}

{% macro dq_logical_rule_required_columns(rule) %}
    {% set required_columns = [] %}
    {% if rule['rule_type'] in ['date_order', 'amount_lte'] %}
        {% do required_columns.append(rule['left_column']) %}
        {% do required_columns.append(rule['right_column']) %}
    {% elif rule['rule_type'] == 'invalid_year_month' %}
        {% do required_columns.append(rule['column']) %}
    {% endif %}
    {{ return(required_columns) }}
{% endmacro %}

{% macro dq_render_logical_rule_sql(rule, relation) %}
    {% set actual_columns = dq_actual_columns(relation) %}
    {% set has_data_source = dq_has_column(actual_columns, 'data_source') %}
    {% set required_columns = dq_logical_rule_required_columns(rule) %}
    {% set logical_table_name = rule.get('table_name', rule['model_name'] | replace('input_layer__', '')) %}

    {% for column_name in required_columns %}
        {% if not dq_has_column(actual_columns, column_name) %}
            {{ return(none) }}
        {% endif %}
    {% endfor %}

    {% set source_key_expression %}
        {% if has_data_source %}
            coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
        {% else %}
            '{{ dq_source_key_sentinel() }}'
        {% endif %}
    {% endset %}

    {% set predicate %}
        {% if rule['rule_type'] == 'date_order' %}
            {% set operator = rule.get('operator', '>') %}
            {{ quote_column(rule['left_column']) }} is not null
            and {{ quote_column(rule['right_column']) }} is not null
            and {{ quote_column(rule['left_column']) }} {{ operator }} {{ quote_column(rule['right_column']) }}
        {% elif rule['rule_type'] == 'amount_lte' %}
            {{ quote_column(rule['left_column']) }} is not null
            and {{ quote_column(rule['right_column']) }} is not null
            and {{ quote_column(rule['left_column']) }} > {{ quote_column(rule['right_column']) }}
        {% endif %}
    {% endset %}

    {% if rule['rule_type'] == 'invalid_year_month' %}
        {% set query %}
            select
                  sources.data_source
                , '{{ logical_table_name }}' as {{ adapter.quote('table') }}
                , '{{ rule['test_name'] }}' as test_name
                , cast(coalesce(invalid_counts.test_result, 0) as {{ dbt.type_int() }}) as test_result
            from (
                {{ dq_source_dimension_sql(relation) }}
            ) as sources
            left join (
                select
                      {{ source_key_expression }} as data_source_key
                    , cast(count(*) as {{ dbt.type_int() }}) as test_result
                from {{ relation }}
                where {{ quote_column(rule['column']) }} is not null
                  and cast({{ quote_column(rule['column']) }} as {{ dbt.type_string() }}) not in (
                      select distinct
                          cast(replace(year_month, '-', '') as {{ dbt.type_string() }})
                      from {{ ref('reference_data__calendar') }}
                  )
                group by 1
            ) as invalid_counts
                on sources.data_source_key = invalid_counts.data_source_key
        {% endset %}
    {% else %}
        {% set query %}
            select
                  sources.data_source
                , '{{ logical_table_name }}' as {{ adapter.quote('table') }}
                , '{{ rule['test_name'] }}' as test_name
                , cast(coalesce(violations.test_result, 0) as {{ dbt.type_int() }}) as test_result
            from (
                {{ dq_source_dimension_sql(relation) }}
            ) as sources
            left join (
                select
                      {{ source_key_expression }} as data_source_key
                    , cast(count(*) as {{ dbt.type_int() }}) as test_result
                from {{ relation }}
                where {{ predicate }}
                group by 1
            ) as violations
                on sources.data_source_key = violations.data_source_key
        {% endset %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}
