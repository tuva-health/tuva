{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the procedure table in core.
-- *************************************************
{% set procedure_cols = range(1, 26) %}

{%- set tuva_core_columns -%}
{{ dbt.safe_cast(
    concat_custom([
        "unpivot_cte.data_source",
        "'_'",
        "unpivot_cte.claim_id",
        "'_'",
        "unpivot_cte.procedure_sequence_id",
        "'_'",
        "unpivot_cte.source_code",
        "case when unpivot_cte.modifier_1 is not null then CONCAT('_', unpivot_cte.modifier_1) else '' end",
        "case when unpivot_cte.modifier_2 is not null then CONCAT('_', unpivot_cte.modifier_2) else '' end",
        "case when unpivot_cte.modifier_3 is not null then CONCAT('_', unpivot_cte.modifier_3) else '' end",
        "case when unpivot_cte.modifier_4 is not null then CONCAT('_', unpivot_cte.modifier_4) else '' end",
        "case when unpivot_cte.modifier_5 is not null then CONCAT('_', unpivot_cte.modifier_5) else '' end",
        "case when unpivot_cte.practitioner_npi is not null then CONCAT('_', unpivot_cte.practitioner_npi) else '' end"
    ]), api.Column.translate_type("string"))
 }} as procedure_id
    , cast(unpivot_cte.person_id as {{ dbt.type_string() }}) as person_id
    , cast(unpivot_cte.member_id as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as encounter_id --one claim can be on multiple encounters, so nulling out for now
    , cast(unpivot_cte.claim_id as {{ dbt.type_string() }}) as claim_id
    , {{ try_to_cast_date('unpivot_cte.procedure_date', 'YYYY-MM-DD') }} as procedure_date
    , cast(unpivot_cte.source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(unpivot_cte.source_code as {{ dbt.type_string() }}) as source_code
    , cast(null as {{ dbt.type_string() }}) as source_description
    , cast(
        case
        when icd.icd_10_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        end
      as {{ dbt.type_string() }}) as normalized_code_type
    , cast(
        coalesce(
            icd.icd_10_pcs
          , hcpcs.hcpcs
        )
      as {{ dbt.type_string() }}) as normalized_code
    , cast(
        coalesce(
            icd.description
          , hcpcs.short_description
        )
      as {{ dbt.type_string() }}) as normalized_description
    , cast(unpivot_cte.modifier_1 as {{ dbt.type_string() }}) as modifier_1
    , cast(unpivot_cte.modifier_2 as {{ dbt.type_string() }}) as modifier_2
    , cast(unpivot_cte.modifier_3 as {{ dbt.type_string() }}) as modifier_3
    , cast(unpivot_cte.modifier_4 as {{ dbt.type_string() }}) as modifier_4
    , cast(unpivot_cte.modifier_5 as {{ dbt.type_string() }}) as modifier_5
    , cast(unpivot_cte.practitioner_npi as {{ dbt.type_string() }}) as practitioner_id
{%- endset -%}

{%- set tuva_metadata_columns -%}
      , cast(unpivot_cte.data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset %}

with unpivot_cte as (

select
    claim_id as claim_id
  , claim_line_number as procedure_sequence_id
  , person_id
  , member_id
  , coalesce(claim_line_start_date
           , claim_start_date
           , admission_date
           , discharge_date
           , claim_end_date
    ) as procedure_date
  , 'hcpcs' as source_code_type
  , hcpcs_code as source_code
  , rendering_id as practitioner_npi
  , hcpcs_modifier_1 as modifier_1
  , hcpcs_modifier_2 as modifier_2
  , hcpcs_modifier_3 as modifier_3
  , hcpcs_modifier_4 as modifier_4
  , hcpcs_modifier_5 as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }}
where hcpcs_code is not null

{% for i in procedure_cols %}
{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}


select
    claim_id as claim_id
  , {{ i }} as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_{{ i }} as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_{{ i }} as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }}
where procedure_code_{{ i }} is not null
{% endfor %}

)

select distinct
    {{ tuva_core_columns }}
    {{ tuva_metadata_columns }}
from unpivot_cte
left outer join {{ ref('terminology__icd_10_pcs') }} as icd
    on unpivot_cte.source_code = icd.icd_10_pcs
left outer join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
    on unpivot_cte.source_code = hcpcs.hcpcs
