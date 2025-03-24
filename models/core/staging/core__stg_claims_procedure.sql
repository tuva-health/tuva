{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the procedure table in core.
-- *************************************************

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

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 1 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_1 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_1 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_1 is not null
    
{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 2 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_2 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_2 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_2 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 3 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_3 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_3 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_3 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 4 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_4 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_4 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_4 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 5 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_5 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_5 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_5 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 6 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_6 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_6 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_6 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 7 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_7 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_7 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_7 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 8 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_8 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_8 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_8 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 9 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_9 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_9 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_9 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 10 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_10 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_10 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_10 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 11 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_11 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_11 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_11 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 12 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_12 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_12 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_12 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 13 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_13 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_13 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_13 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 14 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_14 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_14 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_14 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 15 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_15 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_15 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_15 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 16 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_16 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_16 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_16 is not null
    
{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 17 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_17 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_17 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_17 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 18 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_18 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_18 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_18 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 19 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_19 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_19 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_19 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 20 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_20 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_20 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_20 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 21 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_21 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_21 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_21 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 22 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_22 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_22 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_22 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 23 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_23 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_23 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_23 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 24 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_24 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_24 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_24 is not null

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select
    claim_id as claim_id
  , 25 as procedure_sequence_id
  , person_id
  , member_id
  , procedure_date_25 as procedure_date
  , procedure_code_type as source_code_type
  , procedure_code_25 as source_code
  , rendering_id as practitioner_npi
  , null as modifier_1
  , null as modifier_2
  , null as modifier_3
  , null as modifier_4
  , null as modifier_5
  , data_source as data_source
from {{ ref('normalized_input__medical_claim') }} 
where procedure_code_25 is not null

)

select distinct
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
    , cast(unpivot_cte.person_id as {{ dbt.type_string() }} ) as person_id
    , cast(unpivot_cte.member_id as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id --one claim can be on multiple encounters, so nulling out for now
    , cast(unpivot_cte.claim_id as {{ dbt.type_string() }} ) as claim_id
    , {{ try_to_cast_date('unpivot_cte.procedure_date', 'YYYY-MM-DD') }} as procedure_date
    , cast(unpivot_cte.source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(unpivot_cte.source_code as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(
        case
        when icd.icd_10_pcs is not null then 'icd-10-pcs'
        when hcpcs.hcpcs is not null then 'hcpcs'
        end
      as {{ dbt.type_string() }} ) as normalized_code_type
    , cast (
        coalesce (
            icd.icd_10_pcs
          , hcpcs.hcpcs
        )
      as {{ dbt.type_string() }} ) as normalized_code
    , cast (
        coalesce (
            icd.description
          , hcpcs.short_description
        )
      as {{ dbt.type_string() }} ) as normalized_description
    , cast(unpivot_cte.modifier_1 as {{ dbt.type_string() }} ) as modifier_1
    , cast(unpivot_cte.modifier_2 as {{ dbt.type_string() }} ) as modifier_2
    , cast(unpivot_cte.modifier_3 as {{ dbt.type_string() }} ) as modifier_3
    , cast(unpivot_cte.modifier_4 as {{ dbt.type_string() }} ) as modifier_4
    , cast(unpivot_cte.modifier_5 as {{ dbt.type_string() }} ) as modifier_5
    , cast(unpivot_cte.practitioner_npi as {{ dbt.type_string() }} ) as practitioner_id
    , cast(unpivot_cte.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from unpivot_cte
--inner join {{ ref('encounters__combined_claim_line_crosswalk') }} x on unpivot_cte.claim_id = x.claim_id
--and
--unpivot_cte.claim_line_number = x.claim_line_number
--and
--x.claim_line_attribution_number = 1
left join {{ ref('terminology__icd_10_pcs') }} as icd
    on unpivot_cte.source_code = icd.icd_10_pcs
left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
    on unpivot_cte.source_code = hcpcs.hcpcs