{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


with cte as (
    select 
        payment_year
        , person_id
        , model_version
        , patient_risk_sk
        , SUM(coefficient) as risk_score
    from {{ ref('mart_review__patient_risk_factors') }}
    group by payment_year
             , person_id
             , model_version
             , patient_risk_sk
)

select 
    case 
        when risk_score <= 0.5 then '.5'
        when risk_score between 0.5 and 1.0 then '1'
        when risk_score between 1.0 and 1.5 then '1.5'
        when risk_score between 1.5 and 2.0 then '2'
        when risk_score between 2.0 and 2.5 then '2.5'
        when risk_score between 2.5 and 3.0 then '3'
        when risk_score between 3.0 and 3.5 then '3.5'
        when risk_score between 3.5 and 4.0 then '4'
        when risk_score between 4.0 and 4.5 then '4.5'
        when risk_score between 4.5 and 5.0 then '5'
        when risk_score > 5.0 then '5+'
        else null 
        end as risk_score_bucket
        , payment_year
        , person_id
        , model_version
        , patient_risk_sk
        , risk_score
        , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte
