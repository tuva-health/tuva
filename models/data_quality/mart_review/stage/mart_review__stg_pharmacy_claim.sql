{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select mc.*,
cast(c.year_month_int as varchar(6)) as year_month
FROM {{ ref('core__pharmacy_claim')}}  mc
left join {{ ref('reference_data__calendar') }} c on coalesce(mc.paid_date,mc.dispensing_date) = c.full_date
