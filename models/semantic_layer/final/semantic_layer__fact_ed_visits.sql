{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    e.encounter_id
  , e.person_id
  , {{ dbt.concat(["e.person_id", "'|'", "TO_CHAR(e.encounter_start_date, 'YYYYMM')"]) }} as member_month_sk
  , coalesce(s.ed_classification_order, 99) as ed_classification_order
  , coalesce(s.ed_classification_description, 'Unclassified') as ed_classification_description
  , case when s.ed_classification_order <= 3 then 1 else 0 end as avoidable
  , case when s.encounter_id is null then 'Unclassified'
        when s.ed_classification_order <= 3 then s.ed_classification_description
        else 'Non-Avoidable' end as avoidable_category
  , e.paid_amount
  , e.allowed_amount
  , e.charge_amount
  , e.claim_count
  , e.inst_claim_count
  , e.prof_claim_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('core__encounter') }} e
LEFT JOIN {{ ref('ed_classification__summary')}} s
    ON e.encounter_id = s.encounter_id
WHERE e.encounter_type = 'emergency department'