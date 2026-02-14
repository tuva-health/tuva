{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select m.*
    , COALESCE(p.total_paid, 0) as total_paid
    , COALESCE(p.medical_paid, 0) as medical_paid
    , COALESCE(p.pharmacy_paid, 0) as pharmacy_paid
    , {{ concat_custom([
        'm.person_id',
        "'|'",
        'm.data_source'
    ]) }} as patient_data_source_key
from {{ ref('core__member_months') }} as m
left outer join {{ ref('financial_pmpm__pmpm_prep') }} as p on m.person_id = p.person_id
    and m.member_id = p.member_id
    and m.data_source = p.data_source
    and m.year_month = p.year_month
    and m.payer = p.payer
    and m.{{ quote_column('plan') }} = p.{{ quote_column('plan') }}
