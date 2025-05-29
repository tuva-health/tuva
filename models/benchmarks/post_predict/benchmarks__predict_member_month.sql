{{
    config(
        enabled = var('benchmarks_already_created', False) | as_bool
    )
}}

WITH expected_member_month AS (
    SELECT 
          p.benchmark_key
        , py.year_nbr
        , py.person_id
        , py.payer
        , py.{{ quote_column('plan') }}
        , py.data_source
        , p.paid_amount_pred/py.member_month_count AS paid_amount_pred
        , p.outpatient_paid_amount_pred/py.member_month_count AS outpatient_paid_amount_pred
        , p.other_paid_amount_pred/py.member_month_count AS other_paid_amount_pred
        , p.office_based_paid_amount_pred/py.member_month_count AS office_based_paid_amount_pred
        , p.inpatient_paid_amount_pred/py.member_month_count AS inpatient_paid_amount_pred
    FROM {{ var('predictions_person_year') }} p 
    INNER JOIN {{ ref('benchmarks__person_year') }} py ON p.benchmark_key = py.benchmark_key
)

, claim AS (
    SELECT
        mc.person_id
      , mc.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int AS year_month
      , SUM(mc.paid_amount) AS paid_amount
      , SUM(CASE WHEN mc.encounter_group = 'inpatient' THEN mc.paid_amount ELSE 0 END) AS inpatient_paid_amount_actual
      , SUM(CASE WHEN mc.encounter_group = 'outpatient' THEN mc.paid_amount ELSE 0 END) AS outpatient_paid_amount_actual
      , SUM(CASE WHEN mc.encounter_group = 'other' THEN mc.paid_amount ELSE 0 END) AS other_paid_amount_actual
      , SUM(CASE WHEN mc.encounter_group = 'office based' THEN mc.paid_amount ELSE 0 END) AS office_based_paid_amount_actual
      
    FROM {{ ref('core__medical_claim') }} AS mc
    INNER JOIN {{ ref('reference_data__calendar') }} AS cal 
      ON mc.claim_end_date = cal.full_date
    INNER JOIN {{ ref('core__member_months') }} AS mm 
      ON mc.person_id = mm.person_id
      AND mc.data_source = mm.data_source
      AND mc.payer = mm.payer
      AND mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      AND cal.year_month_int = mm.year_month
    GROUP BY
        mc.person_id
      , mc.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int 
)
{# 
, encounters AS (
    SELECT
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int AS year_month
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'acute inpatient' THEN e.encounter_id ELSE NULL END) AS inpatient_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'emergency department' THEN e.encounter_id ELSE NULL END) AS ed_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'office visit' THEN e.encounter_id ELSE NULL END) AS office_visit_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'home health' THEN e.encounter_id ELSE NULL END) AS home_health_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'inpatient skilled nursing' THEN e.encounter_id ELSE NULL END) AS snf_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'office visit radiology' THEN e.encounter_id ELSE NULL END) AS radiology_office_encounter_count
      , COUNT(DISTINCT CASE WHEN e.encounter_type = 'outpatient radiology' THEN e.encounter_id ELSE NULL END) AS radiology_outpatient_encounter_count
    FROM {{ ref('core__medical_claim') }} AS mc
    INNER JOIN {{ ref('core__encounter') }} AS e 
      ON e.encounter_id = mc.encounter_id
    INNER JOIN {{ ref('reference_data__calendar') }} AS cal 
      ON e.encounter_start_date = cal.full_date
    INNER JOIN {{ ref('core__member_months') }} AS mm 
      ON mc.person_id = mm.person_id
      AND mc.data_source = mm.data_source
      AND mc.payer = mm.payer
      AND mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      AND cal.year_month_int = mm.year_month
    GROUP BY
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int 
) #}

, member_month AS (
  SELECT person_id
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , year_month
  , LEFT(year_month,4) AS year_nbr
  , 1 as member_month_count
  FROM {{ ref('core__member_months') }} AS mm
  GROUP BY 
  person_id
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , year_month
)

, cal AS (
  SELECT DISTINCT year_month_int AS year_month
  , first_day_of_month
  FROM {{ ref('reference_data__calendar') }} 
)

SELECT
   mm.year_month,
   cal.first_day_of_month,
   mm.person_id,
   mm.payer,
   mm.{{ quote_column('plan') }},
   mm.data_source,
   emm.benchmark_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
   -- ==== ACTUAL PMPM paid amounts ====
   COALESCE(c.paid_amount,                  0) AS actual_paid_amount,
   COALESCE(c.inpatient_paid_amount_actual, 0) AS actual_inpatient_paid_amount,
   COALESCE(c.outpatient_paid_amount_actual,0) AS actual_outpatient_paid_amount,
   COALESCE(c.office_based_paid_amount_actual,0) AS actual_office_based_paid_amount,
   COALESCE(c.other_paid_amount_actual,     0) AS actual_other_paid_amount,

   -- ==== EXPECTED (PRED) PMPM paid amounts ====
   COALESCE(emm.paid_amount_pred,           0) AS expected_paid_amount,
   COALESCE(emm.inpatient_paid_amount_pred, 0) AS expected_inpatient_paid_amount,
   COALESCE(emm.outpatient_paid_amount_pred,0) AS expected_outpatient_paid_amount,
   COALESCE(emm.office_based_paid_amount_pred,0) AS expected_office_based_paid_amount,
   COALESCE(emm.other_paid_amount_pred,     0) AS expected_other_paid_amount

FROM member_month AS mm
INNER JOIN cal
  ON mm.year_month = cal.year_month

LEFT JOIN claim AS c 
  ON mm.person_id    = c.person_id
  AND mm.data_source = c.data_source
  AND mm.payer       = c.payer
  AND mm.{{ quote_column('plan') }}        = c.{{ quote_column('plan') }}
  AND mm.year_month  = c.year_month

INNER JOIN expected_member_month AS emm
  ON mm.year_nbr    = emm.year_nbr
  AND mm.person_id  = emm.person_id
  AND mm.payer      = emm.payer
  AND mm.{{ quote_column('plan') }}       = emm.{{ quote_column('plan') }}
  AND mm.data_source= emm.data_source