{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


SELECT *,
       total_paid * member_months AS total_paid_absolute,
       medical_paid * member_months AS medical_paid_absolute,
       pharmacy_paid * member_months AS pharmacy_paid_absolute,
       inpatient_paid * member_months AS inpatient_paid_absolute,
       outpatient_paid * member_months AS outpatient_paid_absolute,
       office_based_paid * member_months AS office_based_paid_absolute,
       ancillary_paid * member_months AS ancillary_paid_absolute,
       other_paid * member_months AS other_paid_absolute,
        {{ concat_custom([
            'data_source',
            "'|'",
            'year_month']) }} as data_source_month_key
FROM {{ ref('financial_pmpm__pmpm_payer') }}
