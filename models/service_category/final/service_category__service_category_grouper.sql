{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  a.claim_id
, a.claim_line_number
, a.claim_type
, case
    when service_category_2 = 'Acute Inpatient'               then 'Inpatient'
    when service_category_2 = 'Ambulance'                     then 'Ancillary'
    when service_category_2 = 'Ambulatory Surgery'            then 'Outpatient'
    when service_category_2 = 'Dialysis'                      then 'Outpatient'
    when service_category_2 = 'Durable Medical Equipment'     then 'Ancillary'
    when service_category_2 = 'Emergency Department'          then 'Outpatient'
    when service_category_2 = 'Home Health'                   then 'Outpatient'
    when service_category_2 = 'Hospice'                       then 'Outpatient'
    when service_category_2 = 'Inpatient Psychiatric'         then 'Inpatient'
    when service_category_2 = 'Inpatient Rehabilitation'      then 'Inpatient'
    when service_category_2 = 'Lab'                           then 'Ancillary'
    when service_category_2 = 'Office Visit'                  then 'Office Visit'
    when service_category_2 = 'Outpatient Hospital or Clinic' then 'Outpatient'
    when service_category_2 = 'Outpatient Psychiatric'        then 'Outpatient'
    when service_category_2 = 'Outpatient Rehabilitation'     then 'Outpatient'
    when service_category_2 = 'Skilled Nursing'               then 'Inpatient'
    when service_category_2 = 'Urgent Care'                   then 'Outpatient'
    when service_category_2 is null                           then 'Other'
  end service_category_1
, case
    when service_category_2 is null then 'Other'
    else service_category_2
  end service_category_2
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__stg_medical_claim') }} a
left join {{ ref('service_category__combined_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where a.claim_type = 'professional'

union all

select distinct 
  a.claim_id
, a.claim_line_number
, a.claim_type
, case
    when service_category_2 = 'Acute Inpatient'               then 'Inpatient'
    when service_category_2 = 'Ambulatory Surgery'            then 'Outpatient'
    when service_category_2 = 'Dialysis'                      then 'Outpatient'
    when service_category_2 = 'Emergency Department'          then 'Outpatient'
    when service_category_2 = 'Home Health'                   then 'Outpatient'
    when service_category_2 = 'Hospice'                       then 'Outpatient'
    when service_category_2 = 'Inpatient Psychiatric'         then 'Inpatient'
    when service_category_2 = 'Inpatient Rehabilitation'      then 'Inpatient'
    when service_category_2 = 'Lab'                           then 'Ancillary'
    when service_category_2 = 'Office Visit'                  then 'Office Visit'
    when service_category_2 = 'Outpatient Hospital or Clinic' then 'Outpatient'
    when service_category_2 = 'Outpatient Psychiatric'        then 'Outpatient'
    when service_category_2 = 'Skilled Nursing'               then 'Inpatient'
    when service_category_2 = 'Urgent Care'                   then 'Outpatient'
    when service_category_2 is null                           then 'Other'
  end service_category_1
, case
    when service_category_2 is null then 'Other'
    else service_category_2
  end service_category_2
, '{{ var('last_update')}}' as last_update
from {{ ref('service_category__stg_medical_claim') }} a
left join {{ ref('service_category__combined_institutional') }} b
  on a.claim_id = b.claim_id
where a.claim_type = 'institutional'
