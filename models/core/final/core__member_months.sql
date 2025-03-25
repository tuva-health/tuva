{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with month_start_and_end_dates as (
  select
    {{ concat_custom(["year",
                  dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year, month, year_month
),


final_before_attribution_fields as (
select distinct
    a.person_id
  , a.member_id
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
    a.person_id
  , a.member_id
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
on a.person_id = b.person_id
and a.year_month = b.year_month
and a.payer = b.payer
and a.{{ quote_column('plan') }} = b.{{ quote_column('plan') }}
and a.data_source = b.data_source
),

final_with_sk as (
select 
    dense_rank() over (
      order by 
        person_id
      , year_month
      , payer
      , {{ quote_column('plan') }}
      , data_source
  ) as member_month_key
  , person_id
  , member_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , tuva_last_run
  , payer_attributed_provider
  , payer_attributed_provider_practice
  , payer_attributed_provider_organization
  , payer_attributed_provider_lob
  , custom_attributed_provider
  , custom_attributed_provider_practice
  , custom_attributed_provider_organization
  , custom_attributed_provider_lob
from add_attribution_fields
)

select * from final_with_sk