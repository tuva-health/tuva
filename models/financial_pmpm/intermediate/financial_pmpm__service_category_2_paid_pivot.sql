{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with service_cat_2 as (
  select
    person_id
  , member_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_2
  , data_source
  , sum(total_paid) as total_paid
  from {{ ref('financial_pmpm__patient_spend_with_service_categories') }}
  group by
    person_id
  , member_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , service_category_2
  , data_source
)

select
  person_id
, member_id
, year_month
, payer
, {{ quote_column('plan') }}
, data_source
, {{ dbt_utils.pivot(
    column='service_category_2'
  , values=(
      'acute inpatient', 
      'ambulance', 
      'ambulatory surgery center', 
      'dialysis', 
      'durable medical equipment', 
      'emergency department', 
      'home health', 
      'inpatient hospice', 
      'inpatient psychiatric', 
      'inpatient rehabilitation', 
      'lab', 
      'observation', 
      'office-based other', 
      'office-based pt/ot/st', 
      'office-based radiology', 
      'office-based surgery', 
      'office-based visit', 
      'other', 
      'outpatient hospice', 
      'outpatient hospital or clinic', 
      'outpatient pt/ot/st', 
      'outpatient psychiatric', 
      'outpatient radiology', 
      'outpatient rehabilitation', 
      'outpatient surgery', 
      'pharmacy', 
      'skilled nursing', 
      'telehealth visit', 
      'urgent care'
  )
    , agg='sum'
    , then_value='total_paid'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_paid'
  ) }}
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from service_cat_2
group by
  person_id
, member_id
, year_month
, payer
, {{ quote_column('plan') }}
, data_source
