{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      person_id
    , v24_risk_score
    , v28_risk_score
    , blended_risk_score
    , normalized_risk_score
    , payment_risk_score
    , payment_risk_score_weighted_by_months
    , member_months
    , payment_year
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('cms_hcc__patient_risk_scores_monthly') }}
where collection_end_date = (
        select max(collection_end_date)
        from {{ ref('cms_hcc__patient_risk_scores_monthly') }}
        where payment_year = {{ var('cms_hcc_payment_year') }}
    )
