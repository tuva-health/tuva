{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


select *
       , total_paid * member_months as total_paid_absolute
       , medical_paid * member_months as medical_paid_absolute
       , pharmacy_paid * member_months as pharmacy_paid_absolute
       , inpatient_paid * member_months as inpatient_paid_absolute
       , outpatient_paid * member_months as outpatient_paid_absolute
       , office_based_paid * member_months as office_based_paid_absolute
       , ancillary_paid * member_months as ancillary_paid_absolute
       , other_paid * member_months as other_paid_absolute
        , {{ concat_custom([
            'data_source',
            "'|'",
            'year_month']) }} as data_source_month_key
from {{ ref('financial_pmpm__pmpm_payer') }}
