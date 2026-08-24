{% macro dq_string_literal_sql(value) %}
    {% if value is none %}
        {{ return("cast(null as " ~ dbt.type_string() ~ ")") }}
    {% endif %}

    {% set escaped = (value | string)
        | replace('\\', '\\\\')
        | replace('\n', ' ')
        | replace('\r', ' ')
        | replace("'", "''")
    %}
    {{ return("'" ~ escaped ~ "'") }}
{% endmacro %}

{% macro dq_input_layer_table_type(table_name) %}
    {% set claims_tables = ['eligibility', 'medical_claim', 'pharmacy_claim'] %}
    {% set other_tables = ['provider_attribution'] %}

    {% if table_name in claims_tables %}
        {{ return('Claims') }}
    {% elif table_name in other_tables %}
        {{ return('Other') }}
    {% else %}
        {{ return('Clinical') }}
    {% endif %}
{% endmacro %}

{% macro dq_domain_catalog_rows() %}
    {% set rows = [
        {'domain_group_key': 'claims_preprocessing', 'domain_group_name': 'Claims Preprocessing', 'component_key': 'claims_enrollment_flags', 'component_name': 'Claims Enrollment Flags', 'display_order': 101},
        {'domain_group_key': 'claims_preprocessing', 'domain_group_name': 'Claims Preprocessing', 'component_key': 'service_categories', 'component_name': 'Service Categories', 'display_order': 102},
        {'domain_group_key': 'claims_preprocessing', 'domain_group_name': 'Claims Preprocessing', 'component_key': 'encounters', 'component_name': 'Encounters', 'display_order': 103},
        {'domain_group_key': 'claims_preprocessing', 'domain_group_name': 'Claims Preprocessing', 'component_key': 'member_month', 'component_name': 'Member Month', 'display_order': 104},
        {'domain_group_key': 'claims_preprocessing', 'domain_group_name': 'Claims Preprocessing', 'component_key': 'provider_attribution', 'component_name': 'Provider Attribution', 'display_order': 105},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'appointment', 'component_name': 'Appointment', 'display_order': 201},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'condition', 'component_name': 'Condition', 'display_order': 202},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'cost', 'component_name': 'Cost', 'display_order': 203},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'eligibility', 'component_name': 'Eligibility', 'display_order': 204},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'encounter', 'component_name': 'Encounter', 'display_order': 205},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'immunization', 'component_name': 'Immunization', 'display_order': 206},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'lab_result', 'component_name': 'Lab Result', 'display_order': 207},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'location', 'component_name': 'Location', 'display_order': 208},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'medical_claim', 'component_name': 'Medical Claim', 'display_order': 209},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'medication', 'component_name': 'Medication', 'display_order': 210},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'member_month', 'component_name': 'Member Month', 'display_order': 211},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'observation', 'component_name': 'Observation', 'display_order': 212},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'patient', 'component_name': 'Patient', 'display_order': 213},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'person_id_crosswalk', 'component_name': 'Person ID Crosswalk', 'display_order': 214},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'pharmacy_claim', 'component_name': 'Pharmacy Claim', 'display_order': 215},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'practitioner', 'component_name': 'Practitioner', 'display_order': 216},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'procedure', 'component_name': 'Procedure', 'display_order': 217},
        {'domain_group_key': 'core_data_model', 'domain_group_name': 'Core Data Model', 'component_key': 'utilization', 'component_name': 'Utilization', 'display_order': 218},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'ahrq_ccsrs', 'component_name': 'AHRQ CCSRs', 'display_order': 301},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'ahrq_quality_indicators', 'component_name': 'AHRQ Quality Indicators', 'display_order': 302},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'cms_chronic_conditions', 'component_name': 'CMS Chronic Conditions', 'display_order': 303},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'cms_hccs', 'component_name': 'CMS HCCs', 'display_order': 304},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'nyu_ed_classification', 'component_name': 'NYU ED Classification', 'display_order': 305},
        {'domain_group_key': 'data_marts', 'domain_group_name': 'Data Marts', 'component_key': 'quality_measures', 'component_name': 'Quality Measures', 'display_order': 306},
        {'domain_group_key': 'enterprise_applications', 'domain_group_name': 'Enterprise Applications', 'component_key': 'empi', 'component_name': 'EMPI', 'display_order': 401},
        {'domain_group_key': 'enterprise_applications', 'domain_group_name': 'Enterprise Applications', 'component_key': 'hedis_and_stars', 'component_name': 'HEDIS and Stars', 'display_order': 402},
        {'domain_group_key': 'enterprise_applications', 'domain_group_name': 'Enterprise Applications', 'component_key': 'medical_economics', 'component_name': 'Medical Economics', 'display_order': 403},
        {'domain_group_key': 'enterprise_applications', 'domain_group_name': 'Enterprise Applications', 'component_key': 'risk_adjustment', 'component_name': 'Risk Adjustment', 'display_order': 404}
    ] %}

    {{ return(rows) }}
{% endmacro %}

{% macro dq_add_table_requirements(requirements, domain_group_key, component_key, input_table_names, source='tuva_core_builtin') %}
    {% for input_table_name in input_table_names %}
        {% do requirements.append({
            'domain_group_key': domain_group_key,
            'component_key': component_key,
            'input_table_name': input_table_name,
            'input_column_name': none,
            'requirement_level': 'required',
            'source': source
        }) %}
    {% endfor %}
{% endmacro %}

{% macro dq_builtin_domain_input_requirements() %}
    {% set requirements = [] %}

    {% do dq_add_table_requirements(requirements, 'claims_preprocessing', 'claims_enrollment_flags', ['eligibility', 'medical_claim', 'pharmacy_claim']) %}
    {% do dq_add_table_requirements(requirements, 'claims_preprocessing', 'service_categories', ['medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'claims_preprocessing', 'encounters', ['eligibility', 'medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'claims_preprocessing', 'member_month', ['eligibility']) %}
    {% do dq_add_table_requirements(requirements, 'claims_preprocessing', 'provider_attribution', ['eligibility', 'medical_claim', 'provider_attribution']) %}

    {% do dq_add_table_requirements(requirements, 'core_data_model', 'appointment', ['appointment']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'condition', ['condition', 'medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'cost', ['eligibility', 'medical_claim', 'pharmacy_claim', 'provider_attribution']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'eligibility', ['eligibility']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'encounter', ['encounter', 'eligibility', 'medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'immunization', ['immunization']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'lab_result', ['lab_result']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'location', ['location', 'medical_claim', 'pharmacy_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'medical_claim', ['eligibility', 'medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'medication', ['medication', 'pharmacy_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'member_month', ['eligibility', 'medical_claim', 'provider_attribution']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'observation', ['observation']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'patient', ['eligibility', 'patient']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'person_id_crosswalk', ['eligibility', 'patient']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'pharmacy_claim', ['eligibility', 'pharmacy_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'practitioner', ['practitioner', 'medical_claim', 'pharmacy_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'procedure', ['procedure', 'medical_claim']) %}
    {% do dq_add_table_requirements(requirements, 'core_data_model', 'utilization', ['eligibility', 'medical_claim', 'provider_attribution']) %}

    {% do dq_add_table_requirements(requirements, 'data_marts', 'ahrq_ccsrs', ['condition', 'eligibility', 'medical_claim', 'procedure']) %}
    {% do dq_add_table_requirements(requirements, 'data_marts', 'ahrq_quality_indicators', ['condition', 'eligibility', 'encounter', 'medical_claim', 'patient', 'procedure', 'provider_attribution']) %}
    {% do dq_add_table_requirements(requirements, 'data_marts', 'cms_chronic_conditions', ['condition', 'eligibility', 'encounter', 'medical_claim', 'patient', 'pharmacy_claim', 'procedure']) %}
    {% do dq_add_table_requirements(requirements, 'data_marts', 'cms_hccs', ['condition', 'eligibility', 'lab_result', 'medical_claim', 'medication', 'observation', 'patient', 'pharmacy_claim', 'provider_attribution']) %}
    {% do dq_add_table_requirements(requirements, 'data_marts', 'nyu_ed_classification', ['eligibility', 'encounter', 'medical_claim', 'patient']) %}
    {% do dq_add_table_requirements(requirements, 'data_marts', 'quality_measures', ['condition', 'eligibility', 'encounter', 'lab_result', 'medical_claim', 'medication', 'observation', 'patient', 'pharmacy_claim', 'procedure']) %}

    {% do dq_add_table_requirements(requirements, 'enterprise_applications', 'empi', ['eligibility', 'patient']) %}
    {% do dq_add_table_requirements(requirements, 'enterprise_applications', 'hedis_and_stars', ['condition', 'eligibility', 'encounter', 'lab_result', 'medical_claim', 'medication', 'observation', 'patient', 'pharmacy_claim', 'procedure']) %}
    {% do dq_add_table_requirements(requirements, 'enterprise_applications', 'medical_economics', ['eligibility', 'medical_claim', 'pharmacy_claim', 'provider_attribution']) %}
    {% do dq_add_table_requirements(requirements, 'enterprise_applications', 'risk_adjustment', ['condition', 'eligibility', 'encounter', 'lab_result', 'medical_claim', 'medication', 'observation', 'patient', 'pharmacy_claim', 'procedure', 'provider_attribution']) %}

    {{ return(requirements) }}
{% endmacro %}

{% macro dq_external_domain_input_requirements() %}
    {% set requirements = [] %}
    {% set var_requirements = var('data_quality_domain_input_requirements', []) %}

    {% for requirement in var_requirements %}
        {% do requirements.append({
            'domain_group_key': requirement.get('domain_group_key'),
            'component_key': requirement.get('component_key'),
            'input_table_name': requirement.get('input_table_name'),
            'input_column_name': requirement.get('input_column_name'),
            'requirement_level': requirement.get('requirement_level', 'required'),
            'source': requirement.get('source', 'root_project_var')
        }) %}
    {% endfor %}

    {% if execute %}
        {% for graph_node in graph['nodes'].values() %}
            {% if graph_node.resource_type == 'model' %}
                {% set meta = graph_node.config.meta if graph_node.config is not none and graph_node.config.meta is not none else {} %}
                {% set dq_meta = meta.get('tuva_data_quality') %}
                {% if dq_meta is mapping %}
                    {% set node_requirements = dq_meta.get('input_requirements', []) %}
                    {% for requirement in node_requirements %}
                        {% do requirements.append({
                            'domain_group_key': requirement.get('domain_group_key', dq_meta.get('domain_group_key')),
                            'component_key': requirement.get('component_key', dq_meta.get('component_key', graph_node.name)),
                            'input_table_name': requirement.get('input_table_name'),
                            'input_column_name': requirement.get('input_column_name'),
                            'requirement_level': requirement.get('requirement_level', 'required'),
                            'source': graph_node.package_name ~ '.' ~ graph_node.name
                        }) %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {{ return(requirements) }}
{% endmacro %}

{% macro dq_domain_input_requirement_rows() %}
    {% set requirements = dq_builtin_domain_input_requirements() %}
    {% for requirement in dq_external_domain_input_requirements() %}
        {% do requirements.append(requirement) %}
    {% endfor %}
    {{ return(requirements) }}
{% endmacro %}

{% macro dq_safe_ratio_sql(numerator_expression, denominator_expression) %}
    {{ return(
        "case when " ~ denominator_expression ~ " = 0 then cast(null as "
        ~ dbt.type_numeric()
        ~ ") else cast(" ~ numerator_expression ~ " as "
        ~ dbt.type_numeric()
        ~ ") / cast(" ~ denominator_expression ~ " as "
        ~ dbt.type_numeric()
        ~ ") end"
    ) }}
{% endmacro %}
