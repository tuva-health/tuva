{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      person_id
    , payer
    , risk_model_code
    , enrollment_status
    , enrollment_status_default
    , orec_default
    , payment_year
    , collection_start_date
    , collection_end_date
    , sum(v24_risk_score) as v24_risk_score
    , sum(v28_risk_score) as v28_risk_score
    , sum(blended_risk_score) as blended_risk_score
    , sum(normalized_risk_score) as normalized_risk_score
    , sum(payment_risk_score) as payment_risk_score
    , sum(payment_risk_score_weighted_by_months) as payment_risk_score_weighted_by_months
    , max(member_months) as member_months
    , max(tuva_last_run) as tuva_last_run
from {{ ref('cms_hcc__patient_risk_scores_monthly_by_factor_type') }}
group by
      person_id
    , payer
    , risk_model_code
    , enrollment_status
    , enrollment_status_default
    , orec_default
    , payment_year
    , collection_start_date
    , collection_end_date
