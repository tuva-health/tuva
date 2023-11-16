{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with service_cat_1 as (
select
  patient_id
, year_month
, payer
, plan
, service_category_1
, data_source
, sum(total_allowed) as total_allowed
from {{ ref('financial_pmpm__patient_spend_with_service_categories') }}
group by 1,2,3,4,5,6
)

select
  patient_id
, year_month
, payer
, plan
, data_source
, {{ dbt_utils.pivot(
      column='service_category_1'
    , values=('Inpatient','Outpatient','Office Visit','Ancillary','Other','Pharmacy')
    , agg='sum'
    , then_value='total_allowed'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_allowed'
  ) }}
, '{{ var('tuva_last_run')}}' as tuva_last_run
from service_cat_1
group by 1,2,3,4,5
