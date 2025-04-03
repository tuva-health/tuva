{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with medical_claim as (
    select m.claim_id,
           max(case when claim_start_date is null then 1 else 0 end) as missing_claim_start_date,
           max(case when claim_end_date is null then 1 else 0 end) as missing_claim_end_date,
           max(case when claim_line_start_date is null then 1 else 0 end) as missing_claim_line_start_date,
           max(case when claim_line_end_date is null then 1 else 0 end) as missing_claim_line_end_date,
           max(case when admission_date is null and d.claim_id is not null then 1 else 0 end) as missing_admission_date,
           max(case when discharge_date is null and d.claim_id is not null then 1 else 0 end) as missing_discharge_date,
           max(case when paid_date is null then 1 else 0 end) as missing_paid_date,
           count(distinct case when d.claim_id is not null then claim_start_date else null end) as claim_start_count,
           count(distinct case when d.claim_id is not null then claim_end_date else null end) as claim_end_count,
           count(distinct case when d.claim_id is not null then admission_date else null end) as admission_date_count,
           count(distinct case when d.claim_id is not null then discharge_date else null end) as discharge_date_count
    from {{ ref('medical_claim')}} m
    left join {{ ref('data_quality__inpatient_dq_stage')}} d on m.claim_id = d.claim_id
    group by m.claim_id
)

,final as (
select 'missing claim_start_date' as data_quality_check,
       sum(missing_claim_start_date) as result_count
from medical_claim

union all

select 'missing claim_end_date' as data_quality_check,
       sum(missing_claim_end_date) as result_count
from medical_claim

union all

select 'missing claim_line_start_date' as data_quality_check,
       sum(missing_claim_line_start_date) as result_count
from medical_claim

union all

select 'missing claim_line_end_date' as data_quality_check,
       sum(missing_claim_line_end_date) as result_count
from medical_claim

union all

select 'missing admission_date' as data_quality_check,
       sum(missing_admission_date) as result_count
from medical_claim

union all

select 'missing discharge_date' as data_quality_check,
       sum(missing_discharge_date) as result_count
from medical_claim

union all

select 'missing medical_claim paid_date' as data_quality_check,
       sum(missing_paid_date) as result_count
from medical_claim

union all

select 'claim start multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where claim_start_count > 1

union all

select 'claim end multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where claim_end_count > 1

union all

select 'admission date multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where admission_date_count > 1

union all

select 'discharge date multiple' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where discharge_date_count > 1
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final