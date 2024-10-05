{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with medical_claim_null as (
select 
    claim_id
    , claim_line_number
    , case when claim_start_date is null then 1 else 0 end as claim_start_date_null
    , case when claim_end_date is null then 1 else 0 end as claim_end_date_null 
    , case when claim_line_start_date is null then 1 else 0 end as claim_line_start_date_null
    , case when claim_line_end_date is null then 1 else 0 end as claim_line_end_date_null 
    , case when admission_date is null then 1 else 0 end as admission_date_null 
    , case when discharge_date is null then 1 else 0 end as discharge_date_null 
    , case when paid_date IS NULL THEN 1 ELSE 0 END as paid_date_null
from 
    {{ref('core__medical_claim')}}
) 

, claim_dates as (
    select 
        claim_id
        , count(distinct claim_start_date) as distinct_claim_start_dates
        , count(distinct claim_end_date) as distinct_claim_end_dates
        , count(distinct admission_date) as distinct_admission_dates
        , count(distinct discharge_date) as distinct_discharge_dates
    from 
        {{ref('core__medical_claim')}}
    GROUP BY 
        claim_id
)

, summary as (
select 
    'missing claim start date' as data_quality_check
    , coalesce(sum(claim_start_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing claim end date' as data_quality_check
    , coalesce(sum(claim_end_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing claim line start date' as data_quality_check
    , coalesce(sum(claim_line_start_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing claim line end date' as data_quality_check
    , coalesce(sum(claim_line_end_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing admission date' as data_quality_check
    , coalesce(sum(admission_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing discharge date' as data_quality_check
    , coalesce(sum(discharge_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'missing paid date' as data_quality_check
    , coalesce(sum(paid_date_null),0) as result
from 
    medical_claim_null

union all 

select 
    'multiple claim start dates' as data_quality_check
    , coalesce(count(distinct_claim_start_dates),0) as result
from 
    claim_dates
where 
    distinct_claim_start_dates > 1 

union all 

select 
    'multiple claim end dates' as data_quality_check
    , coalesce(count(distinct_claim_end_dates),0) as result
from 
    claim_dates
where 
    distinct_claim_end_dates > 1 

union all 

select 
    'multiple admission dates' as data_quality_check
    , coalesce(count(distinct_admission_dates),0) as result
from 
    claim_dates
where 
    distinct_admission_dates > 1 

union all 

select 
    'multiple discharge dates' as data_quality_check
    , coalesce(count(distinct_discharge_dates),0) as result
from 
    claim_dates
where 
    distinct_discharge_dates > 1 
) 

select  
    data_quality_check
    , result 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from  
    summary 