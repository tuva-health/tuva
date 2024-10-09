{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}


select c.year_month_int
,count(distinct(claim_id)) as claim_volume
,sum(p.paid_amount) as paid_amount
from {{ ref('pharmacy_claim')}} p 
left join {{ ref('reference_data__calendar')}} c on coalesce(p.paid_date,p.dispensing_date) = c.full_date
group by 
c.year_month_int

