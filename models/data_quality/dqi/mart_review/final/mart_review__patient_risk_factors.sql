
select *
,dense_rank() over (
order by person_id, model_version, payment_year ) as patient_risk_sk
from {{ ref('cms_hcc__patient_risk_factors') }} as p
