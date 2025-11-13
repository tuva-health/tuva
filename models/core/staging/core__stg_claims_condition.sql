{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the condition table in core.
-- *************************************************

with unpivot_cte as (

  {% for i in range(1, 26) %}
  select
        claim_id
        , claim_line_number
        , payer
        , person_id
        , member_id
        , coalesce(admission_date
                 , claim_start_date
                 , discharge_date
                 , claim_end_date
          ) as recorded_date
        , 'discharge_diagnosis' as condition_type
        , diagnosis_code_type as source_code_type
        , diagnosis_code_{{ i }} as source_code
        , {{ i }} as diagnosis_rank
        , diagnosis_poa_{{ i }} as present_on_admit_code
        , data_source
  from {{ ref('normalized_input__medical_claim') }}
  where diagnosis_code_{{ i }} is not null
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}

)

select distinct
{{ dbt.safe_cast(
    concat_custom([
        "CAST(unpivot_cte.payer AS " ~ dbt.type_string() ~ ")",
        "'_'",
        "CAST(unpivot_cte.data_source AS " ~ dbt.type_string() ~ ")",
        "'_'",
        "CAST(unpivot_cte.claim_id AS " ~ dbt.type_string() ~ ")",
        "'_'",
        "CAST(unpivot_cte.diagnosis_rank AS " ~ dbt.type_string() ~ ")",
        "'_'",
        "CAST(unpivot_cte.source_code AS " ~ dbt.type_string() ~ ")",
    ]), api.Column.translate_type("string"))
 }} as condition_id
    , cast(unpivot_cte.person_id as {{ dbt.type_string() }}) as person_id
    , cast(unpivot_cte.member_id as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as encounter_id --one claim can be on multiple encounters, so nulling out for now
    , cast(unpivot_cte.claim_id as {{ dbt.type_string() }}) as claim_id
    , {{ try_to_cast_date('unpivot_cte.recorded_date', 'YYYY-MM-DD') }} as recorded_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as onset_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as resolved_date
    , cast('active' as {{ dbt.type_string() }}) as status
    , cast(unpivot_cte.condition_type as {{ dbt.type_string() }}) as condition_type
    , cast(unpivot_cte.source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(unpivot_cte.source_code as {{ dbt.type_string() }}) as source_code
    , cast(null as {{ dbt.type_string() }}) as source_description
    , cast(
        case
        when icd.icd_10_cm is not null then 'icd-10-cm'
        end as {{ dbt.type_string() }}
      ) as normalized_code_type
    , cast(icd.icd_10_cm as {{ dbt.type_string() }}) as normalized_code
    , cast(icd.long_description as {{ dbt.type_string() }}) as normalized_description
    , cast(unpivot_cte.diagnosis_rank as {{ dbt.type_int() }}) as condition_rank
    , cast(unpivot_cte.present_on_admit_code as {{ dbt.type_string() }}) as present_on_admit_code
    , cast(poa.present_on_admit_description as {{ dbt.type_string() }}) as present_on_admit_description
    , cast(unpivot_cte.data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(unpivot_cte.payer as {{ dbt.type_string() }}) as payer
from unpivot_cte
--inner join {{ ref('encounters__combined_claim_line_crosswalk') }} x on unpivot_cte.claim_id = x.claim_id
--and
--unpivot_cte.claim_line_number = x.claim_line_number
--and
--x.claim_line_attribution_number = 1
left outer join {{ ref('terminology__icd_10_cm') }} as icd
    on unpivot_cte.source_code = icd.icd_10_cm
left outer join {{ ref('terminology__present_on_admission') }} as poa
    on unpivot_cte.present_on_admit_code = poa.present_on_admit_code
