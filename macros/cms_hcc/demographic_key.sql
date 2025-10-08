{% macro cms_hcc_demographic_key(model_version, enrollment_status, gender, age_group, medicaid_status, dual_status, orec, institutional_status) -%}
    {{ concat_custom([
        model_version,
        "'|'",
        enrollment_status,
        "'|'",
        gender,
        "'|'",
        age_group,
        "'|'",
        medicaid_status,
        "'|'",
        dual_status,
        "'|'",
        orec,
        "'|'",
        institutional_status
    ]) }}
{%- endmacro %}

