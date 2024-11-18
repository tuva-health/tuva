{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with medical as (
  select
      claim_id
    , count(distinct claim_id) as claim_id_count
    , count(distinct p.patient_id) as patient_id_count
    , max(
        case 
          when startdts.month_start_date is null then 1
          when enddts.month_start_date is null then 1
          else 0
        end
      ) as missing_eligibility
  from {{ ref('medical_claim') }} p
  left join {{ ref('data_quality__eligibility_dq_stage') }} startdts
    on p.patient_id = startdts.patient_id
    and p.claim_start_date between startdts.month_start_date and startdts.month_end_date
  left join {{ ref('data_quality__eligibility_dq_stage') }} enddts
    on p.patient_id = enddts.patient_id
    and p.claim_end_date between enddts.month_start_date and enddts.month_end_date
  group by
      claim_id
)

, aggregate_check as(
select
    sum(claim_id_count) as total_claim_count
    , sum(case when patient_id_count > 1 then 1 else 0 end) as multiple_patient_id_count
    , sum(missing_eligibility) as orphaned_claim_count
from medical
)

select
    'multiple medical_claim patient_ids' as data_quality_check
    ,  multiple_patient_id_count as result_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from aggregate_check

union all

select
    'percent orphaned medical_claim claims' as data_quality_check
    , (orphaned_claim_count/total_claim_count)*100 as result_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from aggregate_check

