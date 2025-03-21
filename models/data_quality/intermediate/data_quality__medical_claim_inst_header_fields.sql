{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with inst_header as (
  select
      m.claim_id
    , max(case when m.claim_type = 'institutional' and btc.bill_type_code is null and m.bill_type_code is not null then 1 else 0 end) as invalid_bill_type_code
    , max(case when m.claim_type = 'institutional' and dd.discharge_disposition_code is null and m.discharge_disposition_code is not null then 1 else 0 end) as invalid_discharge_disposition_code
    , max(case when d.claim_id is not null and (ms.ms_drg_code is null and apr.apr_drg_code is null) and m.drg_code is not null then 1 else 0 end) as invalid_drg_code
    , max(case when m.claim_type = 'institutional' and m.bill_type_code is null then 1 else 0 end) as missing_bill_type_code
    , max(case when m.claim_type = 'institutional' and m.discharge_disposition_code is null then 1 else 0 end) as missing_discharge_disposition_code
    , max(case when d.claim_id is not null and m.drg_code is null then 1 else 0 end) as missing_drg_code
    , count(distinct case when m.claim_type = 'institutional' then m.bill_type_code else null end) as bill_type_code_count
    , count(distinct case when m.claim_type = 'institutional' then m.discharge_disposition_code else null end) as discharge_disposition_code_count
    , count(distinct case when d.claim_id is not null then m.drg_code else null end) as drg_code_count
  from {{ ref('input_layer__medical_claim') }} as m
  left join {{ ref('data_quality__inpatient_dq_stage') }} as d
    on m.claim_id = d.claim_id
  left join {{ ref('terminology__bill_type') }} as btc
    on m.bill_type_code = btc.bill_type_code
  left join {{ ref('terminology__ms_drg') }} as ms
    on m.drg_code = ms.ms_drg_code
    and m.drg_code_type = 'ms-drg'
  left join {{ ref('terminology__apr_drg') }} as apr
    on m.drg_code = apr.apr_drg_code
    and m.drg_code_type = 'apr-drg'
  left join {{ ref('terminology__discharge_disposition') }} as dd
    on m.discharge_disposition_code = dd.discharge_disposition_code
  group by m.claim_id
)

, final as (
  select
      'invalid bill_type_code' as data_quality_check
    , sum(invalid_bill_type_code) as result_count
  from inst_header

  union all

  select
      'invalid drg_code' as data_quality_check
    , sum(invalid_drg_code) as result_count
  from inst_header

  union all

  select
      'invalid discharge_disposition_code' as data_quality_check
    , sum(invalid_discharge_disposition_code) as result_count
  from inst_header

  union all

  select
      'missing bill_type_code' as data_quality_check
    , sum(missing_bill_type_code) as result_count
  from inst_header

  union all

  select
      'missing drg_code' as data_quality_check
    , sum(missing_drg_code) as result_count
  from inst_header

  union all

  select
      'missing discharge_disposition_code' as data_quality_check
    , sum(missing_discharge_disposition_code) as result_count
  from inst_header

  union all

  select
      'bill_type_code multiple' as data_quality_check
    , count(distinct claim_id) as result_count
  from inst_header
  where bill_type_code_count > 1

  union all

  select
      'drg_code multiple' as data_quality_check
    , count(distinct claim_id) as result_count
  from inst_header
  where drg_code_count > 1

  union all

  select
      'discharge_disposition_code multiple' as data_quality_check
    , count(distinct claim_id) as result_count
  from inst_header
  where discharge_disposition_code_count > 1
)

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from final
