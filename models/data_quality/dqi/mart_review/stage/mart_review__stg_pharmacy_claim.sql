{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select mc.*
, cast(c.year_month_int as {{ dbt.type_string() }}) as year_month
from {{ ref('core__pharmacy_claim') }} as mc
left outer join {{ ref('reference_data__calendar') }} as c on coalesce(mc.paid_date,mc.dispensing_date) = c.full_date
