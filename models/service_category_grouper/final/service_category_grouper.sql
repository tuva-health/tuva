{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  claim_id
, claim_line_number
, case
    when service_category = 'Acute Inpatient'               then 'Inpatient'
    when service_category = 'Ambulance'                     then 'Ancillary'
    when service_category = 'Ambulatory Surgery'            then 'Outpatient'
    when service_category = 'Dialysis'                      then 'Outpatient'
    when service_category = 'Durable Medical Equipment'     then 'Ancillary'
    when service_category = 'Emergency Department'          then 'Outpatient'
    when service_category = 'Home Health'                   then 'Outpatient'
    when service_category = 'Hospice'                       then 'Outpatient'
    when service_category = 'Inpatient Psychiatric'         then 'Inpatient'
    when service_category = 'Inpatient Rehab'               then 'Inpatient'
    when service_category = 'Lab'                           then 'Ancillary'
    when service_category = 'Office Visit'                  then 'Office Visit'
    when service_category = 'Other'                         then 'Other'
    when service_category = 'Outpatient Hospital or Clinic' then 'Outpatient'
    when service_category = 'Outpatient Psychiatric'        then 'Outpatient'
    when service_category = 'Skilled Nursing'               then 'Inpatient'
    when service_category = 'Urgent Care'                   then 'Outpatient'
    else null
  end service_category_1
, service_category as service_category_2
from {{ ref('combined_professional')}}

union

select distinct 
  claim_id
, claim_line_number
, case
    when service_category = 'Acute Inpatient'               then 'Inpatient'
    when service_category = 'Ambulance'                     then 'Ancillary'
    when service_category = 'Ambulatory Surgery'            then 'Outpatient'
    when service_category = 'Dialysis'                      then 'Outpatient'
    when service_category = 'Durable Medical Equipment'     then 'Ancillary'
    when service_category = 'Emergency Department'          then 'Outpatient'
    when service_category = 'Home Health'                   then 'Outpatient'
    when service_category = 'Hospice'                       then 'Outpatient'
    when service_category = 'Inpatient Psychiatric'         then 'Inpatient'
    when service_category = 'Inpatient Rehab'               then 'Inpatient'
    when service_category = 'Lab'                           then 'Ancillary'
    when service_category = 'Office Visit'                  then 'Office Visit'
    when service_category = 'Other'                         then 'Other'
    when service_category = 'Outpatient Hospital or Clinic' then 'Outpatient'
    when service_category = 'Outpatient Psychiatric'        then 'Outpatient'
    when service_category = 'Skilled Nursing'               then 'Inpatient'
    when service_category = 'Urgent Care'                   then 'Outpatient'
    else null
  end service_category_1
, service_category as service_category_2
from {{ ref('combined_institutional')}}