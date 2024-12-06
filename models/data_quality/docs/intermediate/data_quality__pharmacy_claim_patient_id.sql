{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with pharmacy as (
  select
      claim_id
    , count(distinct claim_id) as claim_id_count
    , count(distinct p.patient_id) as patient_id_count
    , max(case when e.month_start_date is null then 1 else 0 end) as missing_eligibility
  from {{ ref('pharmacy_claim') }} p
  left join {{ ref('data_quality__eligibility_dq_stage') }} e
    on p.patient_id = e.patient_id
    and coalesce(p.paid_date, p.dispensing_date) between e.month_start_date and e.month_end_date
  group by
      claim_id
)
, aggregate_check as(
select
    sum(claim_id_count) as total_claim_count
    , sum(case when patient_id_count > 1 then 1 else 0 end) as multiple_patient_id_count
    , sum(missing_eligibility) as orphaned_claim_count
from pharmacy
)
, final as (
  select
      'multiple pharmacy_claim patient_ids' as data_quality_check
    , sum(case when patient_id_count > 1 then 1 else 0 end) as result_count
  from pharmacy

  union all

  select
      'percent orphaned pharmacy_claim claims' as data_quality_check
    , (orphaned_claim_count/total_claim_count)*100 as result_count
  from pharmacy
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
