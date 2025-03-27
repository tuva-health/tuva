{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select
      person_id
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , factor_type
    , risk_factor_description
    , coefficient
    , model_version
    , payment_year
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('cms_hcc__patient_risk_factors_monthly') }}
where collection_end_date = (
        select max(collection_end_date)
        from {{ ref('cms_hcc__patient_risk_factors_monthly') }}
        where payment_year = {{ var('cms_hcc_payment_year') }}
    )
