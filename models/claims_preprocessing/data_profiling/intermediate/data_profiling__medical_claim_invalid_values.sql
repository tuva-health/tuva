{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with valid_bill_type as(
    select
        'bill_type_code invalid' as test_name
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.bill_type_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__bill_type')}} tob
        on med.bill_type_code = tob.bill_type_code
    where med.claim_type = 'institutional'
    and tob.bill_type_code is null
    and med.bill_type_code is not null
    group by
        claim_id

)
, valid_revenue_center as(
    select 
          'revenue_center_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.revenue_center_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__revenue_center') }} rev
        on med.revenue_center_code = rev.revenue_center_code
    where med.claim_type = 'institutional'
    and rev.revenue_center_code is null
    and med.revenue_center_code is not null
    group by
        claim_id
)
, valid_discharge_disposition as(
    select 
          'discharge_disposition_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.discharge_disposition_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__discharge_disposition') }} discharge
        on med.discharge_disposition_code = discharge.discharge_disposition_code
    where med.claim_type = 'institutional'
    and discharge.discharge_disposition_code is null
    and med.discharge_disposition_code is not null
    group by
         claim_id

)
, valid_admit_source as(
    select 
          'admit_source_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.admit_source_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__admit_source') }} adsource
        on med.admit_source_code = adsource.admit_source_code
    where med.claim_type = 'institutional'
    and adsource.admit_source_code is null
    and med.admit_source_code is not null
    group by
         claim_id
)
, valid_admit_type as(
    select 
          'admit_type_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.admit_type_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__admit_type') }} adtype
        on med.admit_type_code = adtype.admit_type_code
    where med.claim_type = 'institutional'
    and adtype.admit_type_code is null
    and med.admit_type_code is not null
    group by
         claim_id
)
, valid_ms_drg as(
    select 
          'ms_drg_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.ms_drg_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__ms_drg') }} msdrg
        on med.ms_drg_code = msdrg.ms_drg_code
    where med.claim_type = 'institutional'
    and msdrg.ms_drg_code is null
    and med.ms_drg_code is not null
    group by
         claim_id
)
, valid_apr_drg as(
    select 
          'apr_drg_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.apr_drg_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__apr_drg') }} aprdrg
        on med.apr_drg_code = aprdrg.apr_drg_code
        and severity = '1'
    where med.claim_type = 'institutional'
    and aprdrg.apr_drg_code is null
    and med.apr_drg_code is not null
    group by
         claim_id
)
, valid_present_on_admission as(
    select 
          'diagnosis_poa_1 invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.diagnosis_poa_1) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__present_on_admission') }} poa
        on med.diagnosis_poa_1 = poa.present_on_admit_code
    where med.claim_type = 'institutional'
    and poa.present_on_admit_code is null
    and med.diagnosis_poa_1 is not null
    group by
         claim_id
)
, valid_procedure_code_type as(
    select 
          'procedure_code_type invalid' as test_name 
        , 'medical_claim' as source_table
        , 'institutional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.procedure_code_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__code_type') }} codetype
        on med.procedure_code_type = codetype.code_type
    where claim_type = 'institutional'
    and codetype.code_type is null
    and med.procedure_code_type is not null
    group by
         claim_id
)
, valid_place_of_service as(
    select 
          'place_of_service_code invalid' as test_name 
        , 'medical_claim' as source_table
        , 'professional' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.place_of_service_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__place_of_service') }} pos
        on med.place_of_service_code = pos.place_of_service_code
    where claim_type = 'professional'
    and pos.place_of_service_code is null
    and med.place_of_service_code is not null
    group by
         claim_id
)
, valid_diagnosis_code_type as(
    select 
          'diagnosis_code_type invalid' as test_name 
        , 'medical_claim' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.diagnosis_code_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__code_type') }} codetype
        on med.diagnosis_code_type = codetype.code_type
    where codetype.code_type is null
    and med.diagnosis_code_type is not null
    group by
         claim_id
)

, valid_diagnosis_code as(
    select 
          'diagnosis_code_1 invalid' as test_name 
        , 'medical_claim' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.diagnosis_code_1) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__icd_10_cm') }} icd
        on med.diagnosis_code_1 = icd.icd_10_cm
    where diagnosis_code_type = 'icd-10-cm'
    and icd.icd_10_cm is null
    and med.diagnosis_code_1 is not null
    group by
         claim_id

)

, valid_claim_type as(
    select 
          'claim_type invalid' as test_name 
        , 'medical_claim' as source_table
        , 'all' as claim_type
        , 'claim_type' as test_category
        , 'claim_id' as grain
        , claim_id
        , count(med.claim_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim') }} med
    left join {{ ref('terminology__claim_type') }} claimtype
        on med.claim_type = claimtype.claim_type
    where claimtype.claim_type is null
    and med.claim_type is not null
    group by
         claim_id
)
select * from valid_bill_type
union all 
select * from valid_revenue_center
union all 
select * from valid_discharge_disposition
union all 
select * from valid_admit_source
union all 
select * from valid_admit_type
union all 
select * from valid_ms_drg
union all 
select * from valid_apr_drg
union all 
select * from valid_present_on_admission
union all 
select * from valid_diagnosis_code_type
union all 
select * from valid_procedure_code_type
union all 
select * from valid_diagnosis_code
union all 
select * from valid_claim_type
union all 
select * from valid_place_of_service