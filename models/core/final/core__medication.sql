{{ config(
     enabled = (var('claims_enabled', False) | as_bool)
            or (var('clinical_enabled', False) | as_bool)
   )
}}

{%- set tuva_extension_columns_from_all_medications -%}
{% if var('clinical_enabled', False) | as_bool %}
    {{ select_extension_columns(ref('normalized__medication'), alias='meds', strip_prefix=false) }}
{% endif %}
{%- endset -%}

{%- set tuva_extension_columns_from_source_mapping -%}
{% if var('clinical_enabled', False) | as_bool %}
    {{ select_extension_columns(ref('normalized__medication'), alias='sm', strip_prefix=false) }}
{% endif %}
{%- endset -%}

{%- set tuva_metadata_columns -%}
   , tuva_last_run
   , data_source
{%- endset -%}

with all_medications as (
{% if var('clinical_enabled', False) == true
    and var('claims_enabled', False) == true -%}

    {{ smart_union([ref('int_core__medication_from_claim'), ref('normalized__medication')]) }}

{% elif var('clinical_enabled', False) == true -%}

    select *
    from {{ ref('normalized__medication') }}

{% elif var('claims_enabled', False) == true -%}

    select *
    from {{ ref('int_core__medication_from_claim') }}

{%- endif %}
),

source_mapping as (
    select
     meds.medication_id
   , meds.source_type
   , meds.person_id
   , meds.member_id
   , meds.patient_id
   , meds.encounter_id
   , meds.claim_id
   , meds.claim_line_number
   , meds.payer
   , meds.{{ quote_column('plan') }}
   , meds.dispensing_date
   , meds.prescribing_date
   , meds.source_code_type
   , meds.source_code
   , meds.source_description
   , coalesce(
       meds.ndc_code
       , ndc.ndc
       ) as ndc_code
   , coalesce(
       meds.ndc_description
       , ndc.fda_description
       , ndc.rxnorm_description
       ) as ndc_description
   , coalesce(
        meds.rxnorm_code
        , rxatc.rxcui
        ) as rxnorm_code
   , rxatc.rxnorm_description as rxnorm_description
   , coalesce(
        meds.atc_code
        , rxatc.atc_3_code
        ) as atc_code
   , rxatc.atc_4_name as atc_description
   , meds.route
   , meds.strength
   , meds.quantity
   , meds.quantity_unit
   , meds.days_supply
   , meds.practitioner_id
   , meds.data_source
   , meds.tuva_last_run
   {{ tuva_extension_columns_from_all_medications }}
from all_medications as meds
    left outer join {{ ref('terminology__ndc') }} as ndc
        on meds.source_code_type = 'ndc'
        and meds.source_code = ndc.ndc
    left outer join {{ ref('terminology__rxnorm_to_atc') }} as rxatc
        on meds.source_code_type = 'rxnorm'
        and meds.source_code = rxatc.rxcui
   )


-- add auto rxnorm + atc
select
     sm.medication_id
   , sm.source_type
   , sm.person_id
   , sm.member_id
   , sm.patient_id
   , sm.encounter_id
   , sm.claim_id
   , sm.claim_line_number
   , sm.payer
   , sm.{{ quote_column('plan') }}
   , sm.dispensing_date
   , sm.prescribing_date
   , sm.source_code_type
   , sm.source_code
   , sm.source_description
   , sm.ndc_code
   , coalesce(
        sm.ndc_description
        , ndc.fda_description
        , ndc.rxnorm_description
        ) as ndc_description
   , coalesce(
        sm.rxnorm_code
        , ndc.rxcui
        ) as rxnorm_code
   , coalesce(
        sm.rxnorm_description
        , ndc.rxnorm_description
        , rxatc.rxnorm_description
        ) as rxnorm_description
   , coalesce(
        sm.atc_code
        , rxatc.atc_3_code
        ) as atc_code
   , coalesce(
        sm.atc_description
        , rxatc.atc_3_name
        ) as atc_description
   , sm.route
   , sm.strength
   , sm.quantity
   , sm.quantity_unit
   , sm.days_supply
   , sm.practitioner_id
   {{ tuva_extension_columns_from_source_mapping }}
   {{ tuva_metadata_columns }}
from source_mapping as sm
    left outer join {{ ref('terminology__ndc') }} as ndc
        on sm.ndc_code = ndc.ndc
    left outer join {{ ref('terminology__rxnorm_to_atc') }} as rxatc
        on coalesce(sm.rxnorm_code, ndc.rxcui) = rxatc.rxcui
