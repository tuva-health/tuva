{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  a.claim_id
, a.claim_line_number
, case
    when a.claim_id = b.claim_id and a.claim_line_number = b.claim_line_number then 'Acute Inpatient'
    when a.claim_id = c.claim_id and a.claim_line_number = c.claim_line_number then 'Ambulance'
    when a.claim_id = d.claim_id and a.claim_line_number = d.claim_line_number then 'Dialysis'
    when a.claim_id = e.claim_id and a.claim_line_number = e.claim_line_number then 'Durable Medical Equipment'
    when a.claim_id = f.claim_id and a.claim_line_number = f.claim_line_number then 'Emergency Department'
    when a.claim_id = g.claim_id and a.claim_line_number = g.claim_line_number then 'Home Health'  
    when a.claim_id = h.claim_id and a.claim_line_number = h.claim_line_number then 'Hospice'
    when a.claim_id = i.claim_id and a.claim_line_number = i.claim_line_number then 'Lab'
    when a.claim_id = j.claim_id and a.claim_line_number = j.claim_line_number then 'Outpatient Hospital or Clinic'  
    when a.claim_id = k.claim_id and a.claim_line_number = k.claim_line_number then 'Outpatient Psychiatric'
    when a.claim_id = l.claim_id and a.claim_line_number = l.claim_line_number then 'Skilled Nursing'
    when a.claim_id = m.claim_id and a.claim_line_number = m.claim_line_number then 'Urgent Care'       
    else 'Other'
  end service_category
from {{ ref('input_layer__medical_claim') }} a
left join {{ ref('service_category__acute_inpatient_institutional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
left join {{ ref('service_category__ambulance_institutional') }} c
  on a.claim_id = c.claim_id
  and a.claim_line_number = c.claim_line_number
left join {{ ref('service_category__dialysis_institutional') }} d
  on a.claim_id = d.claim_id
  and a.claim_line_number = d.claim_line_number
left join {{ ref('service_category__dme_institutional') }} e
  on a.claim_id = e.claim_id
  and a.claim_line_number = e.claim_line_number
left join {{ ref('service_category__emergency_department_institutional') }} f
  on a.claim_id = f.claim_id
  and a.claim_line_number = f.claim_line_number
left join {{ ref('service_category__home_health_institutional') }} g
  on a.claim_id = g.claim_id
  and a.claim_line_number = g.claim_line_number
left join {{ ref('service_category__hospice_institutional') }} h
  on a.claim_id = h.claim_id
  and a.claim_line_number = h.claim_line_number
left join {{ ref('service_category__lab_institutional') }} i
  on a.claim_id = i.claim_id
  and a.claim_line_number = i.claim_line_number
left join {{ ref('service_category__outpatient_hospital_or_clinic_institutional') }} j
  on a.claim_id = j.claim_id
  and a.claim_line_number = j.claim_line_number
left join {{ ref('service_category__outpatient_psychiatric_institutional') }} k
  on a.claim_id = k.claim_id
  and a.claim_line_number = k.claim_line_number
left join {{ ref('service_category__skilled_nursing_institutional') }} l
  on a.claim_id = l.claim_id
  and a.claim_line_number = l.claim_line_number
left join {{ ref('service_category__urgent_care_institutional') }} m
  on a.claim_id = m.claim_id
  and a.claim_line_number = m.claim_line_number
where a.claim_type = 'institutional'
