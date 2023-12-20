{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with medical_claim as (

    select *
    from {{ ref('normalized_input__medical_claim') }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

, valid_bill_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.bill_type_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__bill_type')}} tob
           on medical_claim.bill_type_code = tob.bill_type_code
         left join test_catalog
           on test_catalog.test_name = 'bill_type_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and tob.bill_type_code is null
    and medical_claim.bill_type_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_revenue_center as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.revenue_center_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__revenue_center') }} rev
           on medical_claim.revenue_center_code = rev.revenue_center_code
         left join test_catalog
           on test_catalog.test_name = 'revenue_center_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and rev.revenue_center_code is null
    and medical_claim.revenue_center_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_discharge_disposition as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.discharge_disposition_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__discharge_disposition') }} discharge
           on medical_claim.discharge_disposition_code = discharge.discharge_disposition_code
         left join test_catalog
           on test_catalog.test_name = 'discharge_disposition_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and discharge.discharge_disposition_code is null
    and medical_claim.discharge_disposition_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_admit_source as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.admit_source_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__admit_source') }} adsource
           on medical_claim.admit_source_code = adsource.admit_source_code
         left join test_catalog
           on test_catalog.test_name = 'admit_source_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and adsource.admit_source_code is null
    and medical_claim.admit_source_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_admit_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.admit_type_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__admit_type') }} adtype
           on medical_claim.admit_type_code = adtype.admit_type_code
         left join test_catalog
           on test_catalog.test_name = 'admit_type_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and adtype.admit_type_code is null
    and medical_claim.admit_type_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_ms_drg as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.ms_drg_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__ms_drg') }} msdrg
           on medical_claim.ms_drg_code = msdrg.ms_drg_code
         left join test_catalog
           on test_catalog.test_name = 'ms_drg_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and msdrg.ms_drg_code is null
    and medical_claim.ms_drg_code is not null
    group by
           medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_apr_drg as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.apr_drg_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__apr_drg') }} aprdrg
           on medical_claim.apr_drg_code = aprdrg.apr_drg_code
           and severity = '1'
         left join test_catalog
           on test_catalog.test_name = 'apr_drg_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and aprdrg.apr_drg_code is null
    and medical_claim.apr_drg_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_present_on_admission as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.diagnosis_poa_1) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__present_on_admission') }} poa
           on medical_claim.diagnosis_poa_1 = poa.present_on_admit_code
         left join test_catalog
           on test_catalog.test_name = 'diagnosis_poa_1 invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and poa.present_on_admit_code is null
    and medical_claim.diagnosis_poa_1 is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_procedure_code_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.procedure_code_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__code_type') }} codetype
           on medical_claim.procedure_code_type = codetype.code_type
         left join test_catalog
           on test_catalog.test_name = 'procedure_code_type invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where claim_type = 'institutional'
    and codetype.code_type is null
    and medical_claim.procedure_code_type is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_place_of_service as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.place_of_service_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__place_of_service') }} pos
           on medical_claim.place_of_service_code = pos.place_of_service_code
         left join test_catalog
           on test_catalog.test_name = 'place_of_service_code invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where claim_type = 'professional'
    and pos.place_of_service_code is null
    and medical_claim.place_of_service_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_diagnosis_code_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.diagnosis_code_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__code_type') }} codetype
           on medical_claim.diagnosis_code_type = codetype.code_type
         left join test_catalog
           on test_catalog.test_name = 'diagnosis_code_type invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where codetype.code_type is null
    and medical_claim.diagnosis_code_type is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_diagnosis_code as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.diagnosis_code_1) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__icd_10_cm') }} icd
           on medical_claim.diagnosis_code_1 = icd.icd_10_cm
         left join test_catalog
           on test_catalog.test_name = 'diagnosis_code_1 invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where diagnosis_code_type = 'icd-10-cm'
    and icd.icd_10_cm is null
    and medical_claim.diagnosis_code_1 is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_claim_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.claim_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join {{ ref('terminology__claim_type') }} claimtype
           on medical_claim.claim_type = claimtype.claim_type
         left join test_catalog
           on test_catalog.test_name = 'claim_type invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where claimtype.claim_type is null
    and medical_claim.claim_type is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

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