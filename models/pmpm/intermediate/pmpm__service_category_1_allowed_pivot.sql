{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with service_cat_1 as (
select
  patient_id
, year_month
, service_category_1
, sum(total_allowed) as total_allowed
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
    , then_value='total_allowed'
    , else_value= 0
    , quote_identifiers = False
    , suffix='_allowed'
  ) }}
-- , inpatient_allowed
-- , outpatient_allowed
-- , office_visit_allowed
-- , ancillary_allowed
-- , other_allowed
-- , pharmacy_allowed
from service_cat_1
-- pivot(sum(total_allowed) for service_category_1 in ('Inpatient','Outpatient','Office Visit','Ancillary','Other','Pharmacy')) 
--   as p (patient_id, year_month, inpatient_allowed, outpatient_allowed, office_visit_allowed, ancillary_allowed, other_allowed, pharmacy_allowed)
group by 1,2
