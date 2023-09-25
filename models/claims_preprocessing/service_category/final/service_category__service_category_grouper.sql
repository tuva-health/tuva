{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
with service_category_1_mapping as(
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
        , '{{ var('tuva_last_run')}}' as tuva_last_run
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
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('service_category__stg_medical_claim') }} a
    left join {{ ref('service_category__combined_institutional') }} b
    on a.claim_id = b.claim_id
    where a.claim_type = 'institutional'
)
, service_category_2_deduplication as(
    select 
        claim_id
        , claim_line_number
        , claim_type
        , service_category_1
        , service_category_2
        , row_number() over (partition by claim_id, claim_line_number order by 
            case
            when service_category_2 = 'Acute Inpatient'               then 3
            when service_category_2 = 'Ambulance'                     then 7
            when service_category_2 = 'Ambulatory Surgery'            then 8
            when service_category_2 = 'Dialysis'                      then 17
            when service_category_2 = 'Durable Medical Equipment'     then 1
            when service_category_2 = 'Emergency Department'          then 5
            when service_category_2 = 'Home Health'                   then 9
            when service_category_2 = 'Hospice'                       then 10
            when service_category_2 = 'Inpatient Psychiatric'         then 11
            when service_category_2 = 'Inpatient Rehabilitation'      then 12
            when service_category_2 = 'Lab'                           then 13
            when service_category_2 = 'Office Visit'                  then 4
            when service_category_2 = 'Outpatient Hospital or Clinic' then 14
            when service_category_2 = 'Outpatient Psychiatric'        then 15
            when service_category_2 = 'Outpatient Rehabilitation'     then 16
            when service_category_2 = 'Skilled Nursing'               then 6
            when service_category_2 = 'Urgent Care'                   then 2
            when service_category_2 is null                           then 18
                else 99 end) as duplicate_row_number
    from service_category_1_mapping
)

select
    claim_id
    , claim_line_number
    , claim_type
    , service_category_1
    , service_category_2
from service_category_2_deduplication
where duplicate_row_number = 1