{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select distinct
      stg.person_id
    , stg.payer
    , stg.data_source
    , coalesce(gap.payment_year, {{ date_part('year', 'recorded_date') }} + 1) as payment_year
    , stg.recorded_date
    , stg.claim_id
    , stg.rendering_npi
    , stg.model_version
    , stg.hcc_code
    , stg.hcc_description
    , stg.hcc_hierarchy_group
    , stg.hcc_hierarchy_group_rank
    , stg.suspect_hcc_flag
    , stg.risk_model_code
    , stg.eligible_claim_indicator
    , stg.eligible_bene
    , stg.reason
    , gap.gap_status
    , gap.recapture_flag
from {{ ref('hcc_recapture__int_hccs') }} as stg
left outer join {{ ref('hcc_recapture__gap_status') }} as gap
  on stg.person_id = gap.person_id
  and stg.payer = gap.payer
  and stg.model_version = gap.model_version
  and stg.hcc_code = gap.hcc_code
  and stg.suspect_hcc_flag = gap.suspect_hcc_flag
  and (case
          when gap.gap_status = 'open' then stg.collection_year + 2
          else stg.collection_year + 1
      end) = gap.payment_year
where eligible_bene = 1
