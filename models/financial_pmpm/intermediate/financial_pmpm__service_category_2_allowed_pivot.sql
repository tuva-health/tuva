{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
    )
}}

with service_cat_2 as (
select
  patient_id
, year_month
, payer
, plan
, service_category_2
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
    column='service_category_2'
  , values=('Acute Inpatient',
            'Ambulance',
            'Ambulatory Surgery',
            'Dialysis',
            'Durable Medical Equipment',
            'Emergency Department',
            'Home Health',
            'Hospice',
            'Inpatient Psychiatric',
            'Inpatient Rehabilitation',
            'Lab',
            'Office Visit',
            'Outpatient Hospital or Clinic',
            'Outpatient Psychiatric',
            'Outpatient Rehabilitation',
            'Skilled Nursing',
            'Urgent Care'                                                 
            )
  , agg='sum'
  , then_value='total_allowed'
  , else_value= 0
  , quote_identifiers = False
  , suffix='_allowed'
) }}
, '{{ var('tuva_last_run')}}' as tuva_last_run
from service_cat_2
group by 1,2,3,4,5