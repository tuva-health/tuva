{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    e.encounter_id
  , e.person_id
  , {{ concat_strings(["e.person_id", "'|'", the_tuva_project.yyyymm("e.encounter_start_date")]) }} as member_month_sk
  , coalesce({{ try_to_cast_int('s.ed_classification_order') }}, 99) as ed_classification_order
  , coalesce(s.ed_classification_description, 'Unclassified') as ed_classification_description
  , case when {{ try_to_cast_int('s.ed_classification_order') }} <= 3 then 1 else 0 end as avoidable
  , case when {{ try_to_cast_int('s.ed_classification_order') }} <= 3 then s.ed_classification_description else null end as avoidable_description
  , e.paid_amount
  , e.allowed_amount
  , e.charge_amount
  , e.claim_count
  , e.inst_claim_count
  , e.prof_claim_count
  , e.tuva_last_run
FROM {{ ref('semantic_layer__stg_core__encounter') }} as e
LEFT JOIN {{ ref('semantic_layer__stg_ed_classification__summary')}} as s
    ON e.encounter_id = s.encounter_id
WHERE e.encounter_type = 'emergency department'
