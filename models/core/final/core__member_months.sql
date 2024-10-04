{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}



with month_start_and_end_dates as (
  select
    {{ dbt.concat(["year",
                  dbt.right(dbt.concat(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year, month, year_month
),


final_before_attribution_fields as (
select distinct
    a.patient_id
  , year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , data_source
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__eligibility') }} a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date
),


add_attribution_fields as (
select
    a.patient_id
  , a.year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , a.data_source
  , a.tuva_last_run
  
  , b.payer_attributed_provider
  , b.payer_attributed_provider_practice
  , b.payer_attributed_provider_organization
  , b.payer_attributed_provider_lob
  , b.custom_attributed_provider
  , b.custom_attributed_provider_practice
  , b.custom_attributed_provider_organization
  , b.custom_attributed_provider_lob

from final_before_attribution_fields a
left join {{ ref('financial_pmpm__stg_provider_attribution') }} b
on a.patient_id = b.patient_id
and a.year_month = b.year_month
and a.payer = b.payer
and a.{{ quote_column('plan') }} = b.{{ quote_column('plan') }}
and a.data_source = b.data_source
)


select *
from add_attribution_fields
