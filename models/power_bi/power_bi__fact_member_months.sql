with monthly_patient_risk as (
    select
       TO_CHAR(collection_end_date, 'YYYYMM') as year_month
     , person_id
     , normalized_risk_score
    from {{ ref('cms_hcc__patient_risk_scores_monthly') }}
),
monthly_population_risk as (
    select
       TO_CHAR(collection_end_date, 'YYYYMM') as year_month
     , avg(normalized_risk_score) monthly_avg_risk_score
    from {{ ref('cms_hcc__patient_risk_scores_monthly') }}
    group by
        TO_CHAR(collection_end_date, 'YYYYMM')
)
select
    mm.person_id
    , data_source
    , {{ dbt.concat(["mm.person_id", "'|'", "data_source"]) }} as patient_source_key
    ,mm.year_month
    ,payer
    ,{{ quote_column('plan') }}
    ,1 as member_months
    ,normalized_risk_score
    ,normalized_risk_score / monthly_avg_risk_score as population_normalized_risk_score
from {{ ref('core__member_months') }} mm
left join monthly_patient_risk mpr on mm.person_id = mpr.person_id
    and mm.year_month = mpr.year_month
left join monthly_population_risk pop_risk on mm.year_month = pop_risk.year_month