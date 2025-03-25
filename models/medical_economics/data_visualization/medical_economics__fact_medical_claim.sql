select 
      aa.person_id 
    , aa.claim_id
    , aa.encounter_id
    , aa.payer 
    , aa.claim_start_date
    , aa.claim_end_date 
    , case 
        when datediff('DAY',aa.claim_start_date, aa.claim_end_date) = 0
        then 1 
        else  datediff('DAY',aa.claim_start_date, aa.claim_end_date)
    end as length_of_stay
    , aa.claim_line_number 
    , aa.revenue_center_code
    , aa.revenue_center_description
    , aa.rendering_id
    , aa.rendering_tin
    , aa.facility_id 
    , aa.facility_name
    , cc.service_category_id
    , aa.ms_drg_code
    , aa.apr_drg_code
    , aa.hcpcs_code
    , aa.paid_amount
    , aa.allowed_amount
    , bb.condition_grouper_id
    , dd.specialty_provider_id
    , ee.comparative_population_id
    , ff.high_cost_claimant_flag
    , case 
        when ff.high_cost_claimant_flag = 0
        then paid_amount
        else 0 
    end as paid_amount_not_high_cost_claimant
from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }} aa
left join {{ ref('medical_economics__dim_condition_group') }} bb 
    on aa.condition_grouper_1 = bb.condition_grouper_1
    and aa.condition_grouper_2 = bb.condition_grouper_2
    and aa.condition_grouper_3 = bb.condition_grouper_3
left join {{ ref('medical_economics__dim_service_category') }} cc
    on aa.service_category_1 = cc.service_category_1
    and aa.service_category_2 = cc.service_category_2
    and aa.service_category_3 = cc.service_category_3
left join {{ ref('medical_economics__dim_specialty_provider') }} dd
    on aa.specialty_provider = dd.specialty_provider
left join {{ ref('medical_economics__fact_comparative_population') }} ee
    on aa.person_id = ee.person_id 
left join {{ ref('medical_economics__high_cost_claimant') }} ff
    on aa.person_id = ff.person_id  
    and replace(left(aa.claim_start_date,7),'-','') = ff.year_month
    and aa.payer = ff.payer 
