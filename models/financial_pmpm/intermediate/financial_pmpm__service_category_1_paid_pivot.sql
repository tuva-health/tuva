
with service_cat_1 as (
  select
    person_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_1
  , data_source
  , sum(total_paid) as total_paid
  from {{ ref('financial_pmpm__patient_spend_with_service_categories') }}
  group by
    person_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_1
  , data_source
)

select
  person_id
, year_month
, payer
, {{ quote_column('plan') }}
, data_source
, {{ dbt_utils.pivot(
      column='service_category_1'
    , values=('inpatient','outpatient','office-based','ancillary','other','pharmacy')
    , agg='sum'
    , then_value='total_paid'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_paid'
  ) }}
, '{{ var('tuva_last_run') }}' as tuva_last_run
from service_cat_1
group by
  person_id
, year_month
, payer
, {{ quote_column('plan') }}
, data_source
