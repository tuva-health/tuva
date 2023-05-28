{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with service_cat_1 as (
select
  patient_id
, year_month
, service_category_1
, sum(total_paid) as total_paid
from {{ ref('pmpm__patient_spend_with_service_categories') }}
group by 1,2,3
)

select
  patient_id
, year_month
, {{ dbt_utils.pivot(
      column='service_category_1'
    , values=('Inpatient','Outpatient','Office Visit','Ancillary','Other','Pharmacy')
    , agg='sum'
    , then_value='total_paid'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_paid'
  ) }}
-- , inpatient_paid
-- , outpatient_paid
-- , office_visit_paid
-- , ancillary_paid
-- , other_paid
-- , pharmacy_paid
from service_cat_1
-- pivot(sum(total_paid) for service_category_1 in ('Inpatient','Outpatient','Office Visit','Ancillary','Other','Pharmacy')) 
--   as p (patient_id, year_month, inpatient_paid, outpatient_paid, office_visit_paid, ancillary_paid, other_paid, pharmacy_paid)
group by 1,2
