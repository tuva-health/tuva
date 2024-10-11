{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with medical as (
  select
      claim_id
    , count(distinct p.patient_id) as patient_id_count
    , max(case when p.patient_id is null then 1 else 0 end) as missing_patient_id
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

, final as (
  select
      'multiple patient_ids' as data_quality_check
    , sum(case when patient_id_count > 1 then 1 else 0 end) as result_count
  from medical

  union all

  select
      'missing patient_id' as data_quality_check
    , sum(missing_patient_id) as result_count
  from medical

  union all

  select
      'orphaned claims' as data_quality_check
    , sum(missing_eligibility) as result_count
  from medical
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
