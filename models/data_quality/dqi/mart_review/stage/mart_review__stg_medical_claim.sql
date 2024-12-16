{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select mc.*,
cast(c.year_month_int as {{ dbt.type_string() }}) as year_month
FROM {{ ref('core__medical_claim')}}  mc
left join {{ ref('reference_data__calendar') }} c on coalesce(mc.claim_line_start_date,mc.claim_start_date) = c.full_date
