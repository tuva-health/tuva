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
    when a.claim_id = n.claim_id and a.claim_line_number = n.claim_line_number then 'Ambulatory Surgery'
    when a.claim_id = o.claim_id and a.claim_line_number = o.claim_line_number then 'Inpatient Psychiatric'
    when a.claim_id = p.claim_id and a.claim_line_number = p.claim_line_number then 'Inpatient Rehab'
    when a.claim_id = q.claim_id and a.claim_line_number = q.claim_line_number then 'Office Visit'
    when a.claim_id = r.claim_id and a.claim_line_number = r.claim_line_number then 'Outpatient Rehab'
    else 'Other'
  end service_category
from {{ ref('input_layer__medical_claim') }} a
left join {{ ref('acute_inpatient_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
left join {{ ref('ambulance_professional') }} c
  on a.claim_id = c.claim_id
  and a.claim_line_number = c.claim_line_number
left join {{ ref('dialysis_professional') }} d
  on a.claim_id = d.claim_id
  and a.claim_line_number = d.claim_line_number
left join {{ ref('dme_professional') }} e
  on a.claim_id = e.claim_id
  and a.claim_line_number = e.claim_line_number
left join {{ ref('emergency_department_professional') }} f
  on a.claim_id = f.claim_id
  and a.claim_line_number = f.claim_line_number
left join {{ ref('home_health_professional') }} g
  on a.claim_id = g.claim_id
  and a.claim_line_number = g.claim_line_number
left join {{ ref('hospice_professional') }} h
  on a.claim_id = h.claim_id
  and a.claim_line_number = h.claim_line_number
left join {{ ref('lab_professional') }} i
  on a.claim_id = i.claim_id
  and a.claim_line_number = i.claim_line_number
left join {{ ref('outpatient_hospital_or_clinic_professional') }} j
  on a.claim_id = j.claim_id
  and a.claim_line_number = j.claim_line_number
left join {{ ref('outpatient_psychiatric_professional') }} k
  on a.claim_id = k.claim_id
  and a.claim_line_number = k.claim_line_number
left join {{ ref('skilled_nursing_professional') }} l
  on a.claim_id = l.claim_id
  and a.claim_line_number = l.claim_line_number
left join {{ ref('urgent_care_professional') }} m
  on a.claim_id = m.claim_id
  and a.claim_line_number = m.claim_line_number
left join {{ ref('ambulatory_surgery_professional') }} n
  on a.claim_id = n.claim_id
  and a.claim_line_number = n.claim_line_number
left join {{ ref('inpatient_psychiatric_professional') }} o
  on a.claim_id = o.claim_id
  and a.claim_line_number = o.claim_line_number
left join {{ ref('inpatient_rehab_professional') }} p
  on a.claim_id = p.claim_id
  and a.claim_line_number = p.claim_line_number
left join {{ ref('office_visit_professional') }} q
  on a.claim_id = q.claim_id
  and a.claim_line_number = q.claim_line_number
left join {{ ref('outpatient_rehab_professional') }} r
  on a.claim_id = r.claim_id
  and a.claim_line_number = r.claim_line_number
where a.claim_type = 'professional'
