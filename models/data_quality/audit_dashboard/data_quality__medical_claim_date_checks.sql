{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with medical_claim as (
    select m.claim_id,
           count(distinct case when d.claim_id is not null then claim_start_date else null end) as claim_start_count,
           count(distinct case when d.claim_id is not null then claim_end_date else null end) as claim_end_count,
           count(distinct case when d.claim_id is not null then admission_date else null end) as admission_date_count,
           count(distinct case when d.claim_id is not null then discharge_date else null end) as discharge_date_count,
           max(case when claim_start_date > claim_end_date then 1 else 0 end) as start_date_after_end_date,
           max(case when admission_date > discharge_date then 1 else 0 end) as admission_after_discharge,
           max(case when claim_start_date > getdate() then 1 else 0 end) as future_start_date,
           max(case when admission_date > getdate() then 1 else 0 end) as future_admission,
           max(case when claim_end_date > getdate() then 1 else 0 end) as future_end_date,
           max(case when discharge_date > getdate() then 1 else 0 end) as future_discharge
    from {{ ref('medical_claim')}} m
    left join {{ ref('data_quality__inpatient_dq_stage')}} d on m.claim_id = d.claim_id
    group by m.claim_id
)

,final as (
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

union all

select 'claim_start_date after claim_end_date' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where start_date_after_end_date > 1

union all

select 'admission_date after discharge_date' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where admission_after_discharge > 1

union all

select 'claim_start_date in the future' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where future_start_date > 1

union all

select 'claim_end_date in the future' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where future_admission > 1

union all

select 'admission_date in the future' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where future_end_date > 1

union all

select 'discharge_date in the future' as data_quality_check,
       count(distinct claim_id) as result_count
from medical_claim
where future_discharge > 1

)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final