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

        , p.inpatient_count_pred/py.member_month_count*12000 as inpatient_encounter_count_pred
        , p.office_based_count_pred/py.member_month_count*12000 as office_based_encounter_count_pred
        , p.other_count_pred/py.member_month_count*12000 as other_encounter_count_pred
        , p.outpatient_count_pred/py.member_month_count*12000 as outpatient_encounter_count_pred

        , p.acute_inpatient_count_pred/py.member_month_count*12000 as acute_inpatient_encounter_count_pred
        , p.emergency_department_count_pred/py.member_month_count*12000 as ed_encounter_count_pred
        , p.inpatient_skilled_nursing_count_pred/py.member_month_count*12000 as inpatient_skilled_nursing_encounter_count_pred
        , p.office_visit_encounter_count/py.member_month_count*12000 as office_visit_encounter_count_pred
        , p.home_health_encounter_count/py.member_month_count*12000 as home_health_encounter_count_pred
        , p.snf_encounter_count/py.member_month_count*12000 as snf_encounter_count_pred

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
      , sum(mc.paid_amount) AS paid_amount
      , sum(CASE WHEN mc.encounter_group = 'inpatient' THEN mc.paid_amount ELSE 0 END) AS inpatient_paid_amount_actual
      , sum(CASE WHEN mc.encounter_group = 'outpatient' THEN mc.paid_amount ELSE 0 END) AS outpatient_paid_amount_actual
      , sum(CASE WHEN mc.encounter_group = 'other' THEN mc.paid_amount ELSE 0 END) AS other_paid_amount_actual
      , sum(CASE WHEN mc.encounter_group = 'office based' THEN mc.paid_amount ELSE 0 END) AS office_based_paid_amount_actual
      
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

, encounters AS (
    SELECT
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int AS year_month
      , count(DISTINCT CASE WHEN e.group = 'inpatient' THEN e.encounter_id ELSE NULL END) AS inpatient_encounter_count
      , count(DISTINCT CASE WHEN e.group = 'outpatient' THEN e.encounter_id ELSE NULL END) AS outpatient_encounter_count
      , count(DISTINCT CASE WHEN e.group = 'office based' THEN e.encounter_id ELSE NULL END) AS office_based_encounter_count
      , count(DISTINCT CASE WHEN e.group = 'other' THEN e.encounter_id ELSE NULL END) AS other_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'acute inpatient' THEN e.encounter_id ELSE NULL END) AS acute_inpatient_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'inpatient skilled nursing' THEN e.encounter_id ELSE NULL END) AS inpatient_skilled_nursing_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'emergency department' THEN e.encounter_id ELSE NULL END) AS ed_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'office visit' THEN e.encounter_id ELSE NULL END) AS office_visit_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'home health' THEN e.encounter_id ELSE NULL END) AS home_health_encounter_count
      , count(DISTINCT CASE WHEN e.encounter_type = 'inpatient skilled nursing' THEN e.encounter_id ELSE NULL END) AS snf_encounter_count -- Note: Same definition as inpatient_skilled_nursing_encounter_count as per original prompt

    FROM {{ ref('core__medical_claim') }} AS mc
    INNER JOIN {{ ref('core__encounter') }} AS e 
      ON e.encounter_id = mc.encounter_id
    INNER JOIN {{ ref('reference_data__calendar') }} AS cal 
      ON e.encounter_start_date = cal.full_date
    INNER JOIN {{ ref('core__member_months') }} AS mm 
      ON mc.person_id = mm.person_id -- Assuming mc.person_id is equivalent to e.person_id for the encounter context
      AND mc.data_source = mm.data_source -- Assuming mc.data_source is equivalent to e.data_source
      AND mc.payer = mm.payer
      AND mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      AND cal.year_month_int = mm.year_month
    GROUP BY
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year_month_int 
)

, member_month AS (
  SELECT 
    person_id
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , year_month
  , left(year_month,4) AS year_nbr
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
  SELECT DISTINCT 
    year_month_int AS year_month
  , first_day_of_month
  FROM {{ ref('reference_data__calendar') }} 
)

SELECT
   mm.year_month
 , cal.first_day_of_month
 , mm.person_id
 , mm.payer
 , mm.{{ quote_column('plan') }}
 , mm.data_source
 , emm.benchmark_key
 , '{{ var('tuva_last_run') }}' as tuva_last_run
   -- ==== ACTUAL PMPM paid amounts ====
 , coalesce(c.paid_amount,                  0) AS actual_paid_amount
 , coalesce(c.inpatient_paid_amount_actual, 0) AS actual_inpatient_paid_amount
 , coalesce(c.outpatient_paid_amount_actual,0) AS actual_outpatient_paid_amount
 , coalesce(c.office_based_paid_amount_actual,0) AS actual_office_based_paid_amount
 , coalesce(c.other_paid_amount_actual,     0) AS actual_other_paid_amount

   -- ==== EXPECTED (PRED) PMPM paid amounts ====
 , coalesce(emm.paid_amount_pred,           0) AS expected_paid_amount
 , coalesce(emm.inpatient_paid_amount_pred, 0) AS expected_inpatient_paid_amount
 , coalesce(emm.outpatient_paid_amount_pred,0) AS expected_outpatient_paid_amount
 , coalesce(emm.office_based_paid_amount_pred,0) AS expected_office_based_paid_amount
 , coalesce(emm.other_paid_amount_pred,     0) AS expected_other_paid_amount

   -- ==== ACTUAL Encounter counts (monthly per person) ====
 , coalesce(enc.inpatient_encounter_count, 0) as actual_inpatient_encounter_count
 , coalesce(enc.outpatient_encounter_count, 0) as actual_outpatient_encounter_count
 , coalesce(enc.office_based_encounter_count, 0) as actual_office_based_encounter_count
 , coalesce(enc.other_encounter_count, 0) as actual_other_encounter_count
 , coalesce(enc.acute_inpatient_encounter_count, 0) as actual_acute_inpatient_encounter_count
 , coalesce(enc.ed_encounter_count, 0) as actual_ed_encounter_count
 , coalesce(enc.inpatient_skilled_nursing_encounter_count, 0) as actual_inpatient_skilled_nursing_encounter_count
 , coalesce(enc.office_visit_encounter_count, 0) as actual_office_visit_encounter_count
 , coalesce(enc.home_health_encounter_count, 0) as actual_home_health_encounter_count
 , coalesce(enc.snf_encounter_count, 0) as actual_snf_encounter_count

   -- ==== EXPECTED (PRED) Encounter counts (rate per 12000 member months) ====
 , coalesce(emm.inpatient_encounter_count_pred, 0) as expected_inpatient_encounter_count
 , coalesce(emm.outpatient_encounter_count_pred, 0) as expected_outpatient_encounter_count
 , coalesce(emm.office_based_encounter_count_pred, 0) as expected_office_based_encounter_count
 , coalesce(emm.other_encounter_count_pred, 0) as expected_other_encounter_count
 , coalesce(emm.acute_inpatient_encounter_count_pred, 0) as expected_acute_inpatient_encounter_count
 , coalesce(emm.ed_encounter_count_pred, 0) as expected_ed_encounter_count
 , coalesce(emm.inpatient_skilled_nursing_encounter_count_pred, 0) as expected_inpatient_skilled_nursing_encounter_count
 , coalesce(emm.office_visit_encounter_count_pred, 0) as expected_office_visit_encounter_count
 , coalesce(emm.home_health_encounter_count_pred, 0) as expected_home_health_encounter_count
 , coalesce(emm.snf_encounter_count_pred, 0) as expected_snf_encounter_count

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

LEFT JOIN encounters AS enc
  ON mm.person_id    = enc.person_id
  AND mm.data_source = enc.data_source
  AND mm.payer       = enc.payer
  AND mm.{{ quote_column('plan') }}        = enc.{{ quote_column('plan') }}
  AND mm.year_month  = enc.year_month