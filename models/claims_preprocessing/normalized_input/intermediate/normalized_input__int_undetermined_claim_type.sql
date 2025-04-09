{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select
    claim_id
    , claim_line_number
    , claim_type
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , ad_src.admit_source_code
    , ad_src.admit_source_description
    , ad_type.admit_type_code
    , ad_type.admit_type_description
    , dis.discharge_disposition_code
    , dis.discharge_disposition_description
    , pos.place_of_service_code
    , pos.place_of_service_description
    , tob.bill_type_code
    , tob.bill_type_description
    , med.drg_code_type
    , coalesce(msdrg.ms_drg_code, aprdrg.apr_drg_code) as drg_code
    , coalesce(msdrg.ms_drg_description, aprdrg.apr_drg_description) as drg_description
    , rev.revenue_center_code
    , rev.revenue_center_description
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_npi
    , rendnpi.npi as rendering_name
    , billing_npi
    , billnpi.npi as billing_name
    , facility_npi
    , facnpi.npi as facility_name
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , diagnosis_code_2
    , diagnosis_code_3
    , diagnosis_code_4
    , diagnosis_code_5
    , diagnosis_code_6
    , diagnosis_code_7
    , diagnosis_code_8
    , diagnosis_code_9
    , diagnosis_code_10
    , diagnosis_code_11
    , diagnosis_code_12
    , diagnosis_code_13
    , diagnosis_code_14
    , diagnosis_code_15
    , diagnosis_code_16
    , diagnosis_code_17
    , diagnosis_code_18
    , diagnosis_code_19
    , diagnosis_code_20
    , diagnosis_code_21
    , diagnosis_code_22
    , diagnosis_code_23
    , diagnosis_code_24
    , diagnosis_code_25
    , diagnosis_poa_1
    , diagnosis_poa_2
    , diagnosis_poa_3
    , diagnosis_poa_4
    , diagnosis_poa_5
    , diagnosis_poa_6
    , diagnosis_poa_7
    , diagnosis_poa_8
    , diagnosis_poa_9
    , diagnosis_poa_10
    , diagnosis_poa_11
    , diagnosis_poa_12
    , diagnosis_poa_13
    , diagnosis_poa_14
    , diagnosis_poa_15
    , diagnosis_poa_16
    , diagnosis_poa_17
    , diagnosis_poa_18
    , diagnosis_poa_19
    , diagnosis_poa_20
    , diagnosis_poa_21
    , diagnosis_poa_22
    , diagnosis_poa_23
    , diagnosis_poa_24
    , diagnosis_poa_25
    , procedure_code_type
    , procedure_code_1
    , procedure_code_2
    , procedure_code_3
    , procedure_code_4
    , procedure_code_5
    , procedure_code_6
    , procedure_code_7
    , procedure_code_8
    , procedure_code_9
    , procedure_code_10
    , procedure_code_11
    , procedure_code_12
    , procedure_code_13
    , procedure_code_14
    , procedure_code_15
    , procedure_code_16
    , procedure_code_17
    , procedure_code_18
    , procedure_code_19
    , procedure_code_20
    , procedure_code_21
    , procedure_code_22
    , procedure_code_23
    , procedure_code_24
    , procedure_code_25
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} as med
left outer join {{ ref('terminology__admit_source') }} as ad_src
    on med.admit_source_code = ad_src.admit_source_code
left outer join {{ ref('terminology__admit_type') }} as ad_type
    on med.admit_type_code = ad_type.admit_type_code
left outer join {{ ref('terminology__discharge_disposition') }} as dis
    on med.discharge_disposition_code = dis.discharge_disposition_code
left outer join {{ ref('terminology__place_of_service') }} as pos
    on med.place_of_service_code = pos.place_of_service_code
left outer join {{ ref('terminology__bill_type') }} as tob
    on med.bill_type_code = tob.bill_type_code
left outer join {{ ref('terminology__ms_drg') }} as msdrg
    on med.drg_code_type = 'ms-drg'
    and med.drg_code = msdrg.ms_drg_code
left outer join {{ ref('terminology__apr_drg') }} as aprdrg
    on med.drg_code_type = 'apr-drg'
    and med.drg_code = aprdrg.apr_drg_code
left outer join {{ ref('terminology__revenue_center') }} as rev
    on med.revenue_center_code = rev.revenue_center_code
left outer join {{ ref('terminology__provider') }} as rendnpi
    on med.rendering_npi = rendnpi.npi
left outer join {{ ref('terminology__provider') }} as billnpi
    on med.billing_npi = billnpi.npi
left outer join {{ ref('terminology__provider') }} as facnpi
    on med.facility_npi = facnpi.npi
where claim_type in ('undetermined')
