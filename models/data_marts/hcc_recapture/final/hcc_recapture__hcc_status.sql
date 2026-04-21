{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

select distinct
    hccs.person_id
    , hccs.payer
    , hccs.data_source
    , coalesce(gap.payment_year, {{ date_part('year', 'hccs.recorded_date') }} + 1) as payment_year
    , hccs.recorded_date
    , hccs.claim_id
    , hccs.rendering_npi
    , hccs.model_version
    , hccs.hcc_code
    , hccs.hcc_description
    , hccs.hcc_hierarchy_group
    , hccs.hcc_hierarchy_group_rank
    , hccs.suspect_hcc_flag
    -- Latest risk_model_code per person/year/model_version based on recorded_date
    , first_value(hccs.risk_model_code) over (
        partition by 
            hccs.person_id,
            coalesce(gap.payment_year, {{ date_part('year', 'hccs.recorded_date') }} + 1),
            hccs.model_version
        order by hccs.recorded_date desc
    ) as risk_model_code
    , hccs.hcc_type
    , hccs.hcc_source
    , coalesce(gap.gap_status,'ineligible for recapture') as gap_status
    -- Filters that may lead to an 'ineligible for recapture' gap status
    , hccs.hcc_chronic_flag
    , hccs.recapturable_flag
    , hccs.eligible_claim_flag
    , hccs.eligible_bene_flag
    , coalesce(gap.filtered_by_hierarchy_flag, recap.filtered_by_hierarchy_flag,0) as filtered_by_hierarchy_flag
from {{ ref('hcc_recapture__int_all_hccs') }} as hccs
left join {{ ref('hcc_recapture__int_recapturable_hccs') }} as recap
    on hccs.person_id = recap.person_id
    and hccs.hcc_code = recap.hcc_code
    and hccs.data_source = recap.data_source
    and hccs.payer = recap.payer
    and hccs.model_version = recap.model_version
    and hccs.collection_year = recap.collection_year
    and hccs.hcc_hierarchy_group = recap.hcc_hierarchy_group
    and coalesce(hccs.claim_id, '') = coalesce(recap.claim_id, '')
left join {{ ref('hcc_recapture__int_gap_status') }} as gap
    on
        hccs.person_id = gap.person_id
        and hccs.payer = gap.payer
        and hccs.model_version = gap.model_version
        and hccs.hcc_code = gap.hcc_code
        -- For TUVA gaps, +2 is needed because we’re comparing collection year to payment year - we already need a +1 for that comparison, and an additional +1 to account for closure in the following year.
        -- For suspect HCCs, we only apply +1, since it’s ok for those HCCs to close themselves within the same year (e.g., CY 2025 suspect list HCCs can be closed in CY 2025 based on claims rather than CY 2026).
        and (case
            when gap.gap_status = 'open' and hccs.hcc_type = 'coded' then hccs.collection_year + 2
            else hccs.collection_year + 1
        end) = gap.payment_year
where hccs.eligible_bene_flag = 1