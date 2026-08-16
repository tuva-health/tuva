{% macro encounter_definitions() %}
{#
    One place where every encounter type's spelling lives.

    Models refer to a type by a safe key -- outpatient_psychiatric -- and this
    macro maps it to the literal the crosswalk actually stores. Those literals
    are irregular: 'outpatient psych' is abbreviated where 'outpatient substance
    use' is not, and 'inpatient long term acute care' spells out what the model
    name shortens. A typo used to mean an empty table and a green run; now the
    spellings sit side by side where a mismatch is visible, and an unknown key
    fails the compile instead.

    Only the types built from a shared macro are registered here. The rest have
    their own model bodies and keep their literal inline.

        encounter_type: the value stored in int_encounter__combined_claim_line_crosswalk
        source_model:   the upstream model supplying encounter dates
        snf_part_b:     optional, adds the column only skilled nursing reports
#}
{{ return({
    'inpatient_hospice'             : {'encounter_type': 'inpatient hospice'               , 'source_model': 'int_encounter__inpatient_hospice_start_end_date'},
    'inpatient_long_term'           : {'encounter_type': 'inpatient long term acute care'  , 'source_model': 'int_encounter__inpatient_long_term_start_end_date'},
    'inpatient_psychiatric'         : {'encounter_type': 'inpatient psych'                 , 'source_model': 'int_encounter__inpatient_psychiatric_start_end_date'},
    'inpatient_rehab'               : {'encounter_type': 'inpatient rehabilitation'        , 'source_model': 'int_encounter__inpatient_rehab_start_end_date'},
    'inpatient_skilled_nursing'     : {'encounter_type': 'inpatient skilled nursing'       , 'source_model': 'int_encounter__inpatient_skilled_nursing_start_end_date', 'snf_part_b': true},
    'inpatient_substance_use'       : {'encounter_type': 'inpatient substance use'         , 'source_model': 'int_encounter__inpatient_substance_use_start_end_date'},
    'outpatient_hospice'            : {'encounter_type': 'outpatient hospice'              , 'source_model': 'int_encounter__outpatient_hospice_generate_id'},
    'outpatient_hospital_or_clinic' : {'encounter_type': 'outpatient hospital or clinic'   , 'source_model': 'int_encounter__outpatient_hospital_or_clinic_generate_id'},
    'outpatient_psychiatric'        : {'encounter_type': 'outpatient psych'                , 'source_model': 'int_encounter__outpatient_psychiatric_generate_id'},
    'outpatient_rehab'              : {'encounter_type': 'outpatient rehabilitation'       , 'source_model': 'int_encounter__outpatient_rehab_generate_id'},
    'outpatient_substance_use'      : {'encounter_type': 'outpatient substance use'        , 'source_model': 'int_encounter__outpatient_substance_use_generate_id'},
    'outpatient_surgery'            : {'encounter_type': 'outpatient surgery'              , 'source_model': 'int_encounter__outpatient_surgery_generate_id'},
    'outpatient_therapy'            : {'encounter_type': 'outpatient pt/ot/st'             , 'source_model': 'int_encounter__outpatient_therapy_generate_id'},
    'urgent_care'                   : {'encounter_type': 'urgent care'                     , 'source_model': 'int_encounter__urgent_care_generate_id'},
}) }}
{% endmacro %}


{% macro encounter_definition(key) %}
{#
    Looks up one entry and fails loudly on an unknown key, listing the valid
    ones. Silent failure here would give an empty table, not an error.
#}
{%- set defs = encounter_definitions() -%}
{%- if key not in defs -%}
    {%- do exceptions.raise_compiler_error(
        "Unknown encounter key '" ~ key ~ "'. Known keys: "
        ~ (defs.keys() | list | sort | join(', '))) -%}
{%- endif -%}
{{ return(defs[key]) }}
{% endmacro %}
