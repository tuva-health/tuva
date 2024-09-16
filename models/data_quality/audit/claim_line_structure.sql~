{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}



select
  claim_id,
  claim_line_number,
  data_source,

  discharge_disposition_code,
  case
    when length(discharge_disposition_code) <> 2 then 1
    else 0
  end as ddp_invalid_length,

  place_of_service_code,
  case
    when length(place_of_service_code) <> 2 then 1
    else 0
  end as pos_invalid_length,

  bill_type_code,
  case
    when length(bill_type_code) <> 3 then 1
    else 0
  end as bill_invalid_length,
  
  ms_drg_code,
  case
    when length(ms_drg_code) <> 3 then 1
    else 0
  end as msdrg_invalid_length,
  
  apr_drg_code,
  case
    when length(apr_drg_code) <> 3 then 1
    else 0
  end as aprdrg_invalid_length,
  
  revenue_center_code,
  case
    when length(revenue_center_code) <> 4 then 1
    else 0
  end as rev_invalid_length


from {{ ref('medical_claim') }}
