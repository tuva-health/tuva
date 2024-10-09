{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with inst_header as (
    select m.claim_id,
           max(case when m.claim_type = 'institutional' and btc.bill_type_code is null and m.bill_type_code is not null then 1 else 0 end) as invalid_bill_type_code,
           max(case when m.claim_type = 'institutional' and dd.discharge_disposition_code is null and m.discharge_disposition_code is not null then 1 else 0 end) as invalid_discharge_disposition_code,
           max(case when  d.claim_id is not null and ms.ms_drg_code is null and m.ms_drg_code is not null then 1 else 0 end) as invalid_ms_drg_code,
           max(case when  d.claim_id is not null and apr.apr_drg_code is null and m.apr_drg_code is not null then 1 else 0 end) as invalid_apr_drg_code,

           max(case when m.claim_type = 'institutional' and m.bill_type_code is null then 1 else 0 end) as missing_bill_type_code,
           max(case when m.claim_type = 'institutional' and m.discharge_disposition_code is null then 1 else 0 end) as missing_discharge_disposition_code,
           max(case when  d.claim_id is not null and m.ms_drg_code is null then 1 else 0 end) as missing_ms_drg_code,
           max(case when  d.claim_id is not null and m.apr_drg_code is null then 1 else 0 end) as missing_apr_drg_code,

           count(distinct case when m.claim_type = 'institutional' then m.bill_type_code else null end) as bill_type_code_count,
           count(distinct case when m.claim_type = 'institutional' then m.discharge_disposition_code else null end) as discharge_disposition_code_count,
           count(distinct case when d.claim_id is not null then m.ms_drg_code  else null end) as ms_drg_code_count,
           count(distinct case when d.claim_id is not null then m.apr_drg_code  else null end) as apr_drg_code_count,
    from {{ ref('medical_claim')}} m
    left join {{ ref('data_quality__dq_inpatient_stage')}} d on m.claim_id = d.claim_id
    left join {{ ref('terminology__bill_type')}} as btc on m.bill_type_code = btc.bill_type_code
    left join {{ ref('terminology__ms_drg')}} as ms on m.ms_drg_code = ms.ms_drg_code
    left join {{ ref('terminology__apr_drg')}} as apr on m.ms_drg_code = apr.apr_drg_code
    left join {{ ref('terminology__discharge_disposition')}} as dd on m.discharge_disposition_code = dd.discharge_disposition_code
    group by m.claim_id
)

select 'invalid bill_type_code' as data_quality_check,
       sum(invalid_bill_type_code) as result_count
from inst_header

union all

select 'invalid ms_drg_code' as data_quality_check,
       sum(invalid_ms_drg_code) as result_count
from inst_header

union all

select 'invalid apr_drg_code' as data_quality_check,
       sum(invalid_apr_drg_code) as result_count
from inst_header

union all

select 'invalid discharge_disposition_code' as data_quality_check,
       sum(invalid_discharge_disposition_code) as result_count
from inst_header

union all

select 'missing bill_type_code' as data_quality_check,
       sum(missing_bill_type_code) as result_count
from inst_header

union all

select 'missing ms_drg_code' as data_quality_check,
       sum(missing_ms_drg_code) as result_count
from inst_header

union all

select 'missing apr_drg_code' as data_quality_check,
       sum(missing_apr_drg_code) as result_count
from inst_header

union all

select 'missing discharge_disposition_code' as data_quality_check,
       sum(missing_discharge_disposition_code) as result_count
from inst_header

union all

select 'bill_type_code multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from inst_header
where bill_type_code_count > 1

union all

select 'ms_drg_code multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from inst_header
where ms_drg_code_count > 1

union all

select 'apr_drg_code multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from inst_header
where apr_drg_code_count > 1

union all

select 'discharge_disposition_code multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from inst_header
where discharge_disposition_code_count > 1