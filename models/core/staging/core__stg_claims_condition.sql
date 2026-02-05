{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the condition table in core.
-- *************************************************

-- The codes need to be brought in like this since a single diagnosis column can have multiple different diagnosis
-- TODO: Add test to ensure all diagnosis in the medical claim are being brought through and none are being lost
with combine_diag_poa as (
 select
      diag.claim_id
    , diag.data_source
    , diag.diagnosis_code_type as source_code_type
    , diag.diagnosis_code as source_code
    , cast('discharge_diagnosis' as {{ dbt.type_string() }}) as condition_type
    {% if target.type == 'fabric' %}
    , reverse(left(reverse(diag.column_name), charindex('_', reverse(diag.column_name)) - 1)) as diagnosis_rank
    {% else %}
    , {{ dbt.split_part(string_text='diag.column_name', delimiter_text="'_'", part_number=-1) }} as diagnosis_rank
    {% endif %}
    , poa.normalized_code as present_on_admit_code
 from {{ ref('normalized_input__int_diagnosis_code_intermediate') }} as diag
  -- noqa: disable=ambiguous.join
 left join {{ ref('normalized_input__int_present_on_admit_voting') }} as poa
    on diag.claim_id = poa.claim_id
    and diag.data_source = poa.data_source
    {% if target.type == 'fabric' %}
    and reverse(left(reverse(diag.column_name), charindex('_', reverse(diag.column_name)) - 1)) = reverse(left(reverse(poa.column_name), charindex('_', reverse(poa.column_name)) - 1))
    {% else %}
    and {{ dbt.split_part(string_text='diag.column_name', delimiter_text="'_'", part_number=-1) }} = {{ dbt.split_part(string_text='poa.column_name', delimiter_text="'_'", part_number=-1) }}
    {% endif %}
  -- noqa: enable=ambiguous.join
)

, unpivot_cte as (
select
      code.claim_id
    , med.claim_line_number
    , med.payer
    , med.{{ quote_column('plan') }}
    , med.person_id
    , med.member_id
    , coalesce(med.admission_date
             , med.claim_start_date
             , med.discharge_date
             , med.claim_end_date
      ) as recorded_date
    , code.data_source
    , code.source_code_type
    , code.source_code
    , code.condition_type
    , code.diagnosis_rank
    , code.present_on_admit_code
-- Using this CTE since normalized_input__medical_claim is missing diagnosis due to the max
-- in normalized_input__int_diagnosis_code_final. Some diagnosis columns have more than 1 diagnosis
-- and we want all possible diagnosis.
from combine_diag_poa as code
inner join {{ ref('normalized_input__medical_claim') }} as med
    on code.claim_id = med.claim_id
    and code.data_source = med.data_source
where code.source_code is not null
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
    , cast(unpivot_cte.{{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
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
