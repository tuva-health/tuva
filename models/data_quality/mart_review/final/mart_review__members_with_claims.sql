{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

WITH medical_claim AS (
    SELECT
        data_source,
        patient_id,
        year_month,
        SUM(paid_amount) AS paid_amount
    FROM {{ ref('mart_review__stg_medical_claim') }}
    GROUP BY data_source
    , patient_id
    , year_month
)

,pharmacy_claim AS (
    SELECT
        data_source,
        patient_id,
        year_month,
        SUM(paid_amount) AS paid_amount
    FROM {{ ref('mart_review__stg_pharmacy_claim') }}
    GROUP BY data_source
    , patient_id
    , year_month
)
, final as(
SELECT
    mm.data_source,
    mm.year_month,
    SUM(CASE WHEN mc.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS members_with_medical_claims,
    SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS members_with_pharmacy_claims,
    SUM(CASE WHEN pc.patient_id IS NOT NULL THEN 1
             WHEN mc.patient_id is not null THEN 1 ELSE 0 END) AS members_with_claims,
    COUNT(*) AS total_member_months
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__member_months')}} mm
LEFT JOIN medical_claim mc
    ON mm.patient_id = mc.patient_id
    AND mm.data_source = mc.data_source
    AND mm.year_month = mc.year_month
LEFT JOIN pharmacy_claim pc
    ON mm.patient_id = pc.patient_id
    AND mm.data_source = pc.data_source
    AND mm.year_month = pc.year_month
GROUP BY mm.data_source, mm.year_month
)

select
    data_source
    , year_month
    , members_with_medical_claims
    , members_with_pharmacy_claims
    , members_with_claims
    , total_member_months
    , cast(members_with_claims/ total_member_months as {{ dbt.type_numeric()}}) as percent_members_with_claims
    , cast(members_with_medical_claims/ total_member_months  as {{ dbt.type_numeric()}}) as percent_members_with_medical_claims
    , cast(members_with_pharmacy_claims/ total_member_months as {{ dbt.type_numeric()}})  as  percent_members_with_pharmacy_claims
from final

