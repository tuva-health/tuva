SELECT DISTINCT
    encounter_id
    ,service_category_sk
from {{ ref('power_bi__fact_claims') }}