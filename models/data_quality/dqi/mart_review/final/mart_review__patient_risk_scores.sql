

select *
from {{ ref('cms_hcc__patient_risk_scores') }} as p
