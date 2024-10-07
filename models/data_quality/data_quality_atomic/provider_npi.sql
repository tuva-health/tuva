{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with medical_claim_npi as (
select 
    claim_id 
    , claim_line_number
    , billing_npi
    , rendering_npi
    , facility_npi
    , claim_type
from 
    {{ref('core__medical_claim')}}
) 

, invalid_npi_prep as ( 
select 
    claim_id
    , claim_line_number
    {% if target.type == 'fabric' %}
    , case when billing_npi is null then 0 when strlen(billing_npi) != 10 then 1 else 0 end as billing_npi_invalid_npi 
    , case when rendering_npi is null then 0 when strlen(rendering_npi) != 10 then 1 else 0 end as rendering_npi_invalid_npi
    , case when facility_npi is null then 0 when strlen(facility_npi) != 10 then 1 else 0 end as facility_npi_invalid_npi 
    {% else %}
    , case when billing_npi is null then 0 when length(billing_npi) != 10 then 1 else 0 end as billing_npi_invalid_npi 
    , case when rendering_npi is null then 0 when length(rendering_npi) != 10 then 1 else 0 end as rendering_npi_invalid_npi 
    , case when facility_npi is null then 0 when length(facility_npi) != 10 then 1 else 0 end as facility_npi_invalid_npi 
from 
    medical_claim_npi
) 

, invalid_npi as (
select 
claim_id
, case when sum(billing_npi_invalid_npi) > 0 then 1 else 0 end as billing_npi_invalid_npi 
, case when sum(rendering_npi_invalid_npi) > 0 then 1 else 0 end as rendering_npi_invalid_npi
, case when sum(facility_npi_invalid_npi) > 0 then 1 else 0 end as facility_npi_invalid_npi
from 
invalid_npi_prep
group by 
claim_id
)

, billing_npi_checks_line_number as (
select 
    claim_id 
    , claim_line_number 
    , billing_npi
    , case when billing_npi is null then 1 else 0 end as missing_billing_npi
from 
    medical_claim_npi
) 

, billing_npi_missing_checks as (
select 
    claim_id
    , case when sum(missing_billing_npi) > 0 then 1 else 0 end as missing_billing_npi 
from 
    billing_npi_checks_line_number
group by 
    claim_id
)

, billing_npi_multiple_checks_prep as (
select 
    claim_id
    , case when billing_npi is null then 0 when count(distinct billing_npi) > 1 then 1 else 0 end as multiple_billing_npi 
from 
    billing_npi_checks_line_number
group by 
    claim_id
    , billing_npi
)

, billing_npi_multiple_checks as (
select 
    claim_id
    , case when sum(multiple_billing_npi) > 1 then 1 else 0 end as multiple_billing_npi 
from 
    billing_npi_multiple_checks_prep
group by 
    claim_id
) 

, rendering_npi_checks_prep as (
select 
    claim_id 
    , case when rendering_npi is null then 1 else 0 end as missing_rendering_npi
from 
    medical_claim_npi
group by 
    claim_id
    , rendering_npi
) 

, rendering_npi_missing_checks as (
select 
    claim_id
    , case when sum(missing_rendering_npi) > 0 then 1 else 0 end as missing_rendering_npi 
from 
    rendering_npi_checks_prep
group by 
    claim_id
)

, rendering_npi_entity_type_prep as (
select 
    medical_claim_npi.claim_id
    , medical_claim_npi.rendering_npi 
    , provider.npi 
    , provider.entity_type_code
    , provider.entity_type_description
    , case when medical_claim_npi.rendering_npi is null then 0 when medical_claim_npi.rendering_npi is not null and entity_type_code = 2 THEN 1 else 0 end as wrong_entity_type_rendering_npi 
from 
    medical_claim_npi
left join 
    terminology.provider 
    on medical_claim_npi.rendering_npi = provider.npi 
where 
medical_claim_npi.rendering_npi is not null
)

, rendering_npi_entity_type as (
select 
claim_id 
, case when sum(wrong_entity_type_rendering_npi) > 0 then 1 else 0 end as wrong_entity_type_rendering_npi
from 
rendering_npi_entity_type_prep
group by 
claim_id
)

, facility_npi_checks_line_number as (
select 
    claim_id 
    , claim_line_number
    , facility_npi 
    , claim_type 
    , case when (facility_npi is null and claim_type = 'institutional') then 1 else 0 end as missing_facility_npi
    , case when (facility_npi is null and claim_type = 'institutional') then 1 when (facility_npi is not null and claim_type != 'institutional') then 1 else 0 end as wrong_entity_type_facility_id 
from 
    medical_claim_npi
) 

, facility_npi_wrong_entity_type as (
select 
    claim_id
    , case when sum(wrong_entity_type_facility_id) > 0 then 1 else 0 end as wrong_entity_type_facility_id 
from 
    facility_npi_checks_line_number
group by 
    claim_id
)

, facility_npi_missing_npi as (
select 
claim_id
, case when sum(missing_facility_npi) > 0 then 1 else 0 end as missing_facility_npi 
from 
facility_npi_checks_line_number
group by 
claim_id
)

, facility_npi_multiple_checks_prep as (
select 
    claim_id
    , case when facility_npi is null then 0 when count(distinct facility_npi) > 1 then 1 else 0 end as multiple_facility_npi 
from 
    facility_npi_checks_line_number
group by 
    claim_id
    , facility_npi
)

, facility_npi_multiple_checks as (
select 
    claim_id
    , case when sum(multiple_facility_npi) > 1 then 1 else 0 end as multiple_facility_npi 
from 
    facility_npi_multiple_checks_prep
group by 
    claim_id
) 

, summary as (
select  
    'invalid billing npis' AS data_quality_check
    , COALESCE(SUM(billing_npi_invalid_npi),0) AS result_count
from 
    invalid_npi

union all

select  
    'invalid rendering npis' AS data_quality_check
    , COALESCE(SUM(rendering_npi_invalid_npi),0) AS result_count
from 
    invalid_npi

union all

select  
    'invalid facility npis' AS data_quality_check
    , COALESCE(SUM(facility_npi_invalid_npi),0) AS result_count
from 
    invalid_npi

union all 

select 
    'missing billing npi' AS data_quality_check
    , COALESCE(SUM(missing_billing_npi),0) AS result_count
from 
    billing_npi_missing_checks

union all 

select 
    'multiple billing npi' AS data_quality_check
    , COALESCE(SUM(multiple_billing_npi),0) AS result_count
from 
    billing_npi_multiple_checks

union all 

select 
    'missing rendering npi' AS data_quality_check
    , COALESCE(SUM(missing_rendering_npi),0) AS result_count
from 
    rendering_npi_missing_checks

union all 

select 
    'wrong entity type rendering npi' AS data_quality_check 
    , COALESCE(SUM(wrong_entity_type_rendering_npi),0) AS result_count
from 
    rendering_npi_entity_type

union all 

select 
    'missing facility npi' AS data_quality_check
    , COALESCE(SUM(missing_facility_npi),0) AS result_count
from 
    facility_npi_missing_npi

union all 

select 
    'multiple facility npi' AS data_quality_check
    , COALESCE(SUM(multiple_facility_npi),0) AS result_count
from 
    facility_npi_multiple_checks

union all 

select 
    'wrong entity type facility npi' AS data_quality_check
    , COALESCE(SUM(wrong_entity_type_facility_id),0) AS result_count
from 
    facility_npi_wrong_entity_type
) 

select  
    data_quality_check 
    , result_count 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from  
    summary 
