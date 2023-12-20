{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
with use_case_stage as(
    select
        'encounters' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__medical_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('revenue_center_code missing'
                    ,'revenue_center_code invalid'
                    ,'discharge_disposition_code missing'
                    ,'discharge_disposition_code invalid'
                    ,'ms_drg_code missing'
                    ,'ms_drg_code invalid'
                    ,'bill_type_code missing'
                    ,'bill_type_code invalid'
                    ,'place_of_service_code missing'
                    ,'place_of_service_code invalid'
                    ,'claim_type invalid'
                    ,'claim_type missing'
                    ,'claim_id missing'
                    ,'patient_id missing'
                    ,'claim_start_date missing'
                    ,'claim_end_date missing'
                    ,'facility_npi missing'
                    )
    and source_table = 'normalized_input__medical_claim'
    group by
        source_table
        , test_name

    union all

    select 
        'service_grouper' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__medical_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('revenue_center_code missing'
                    ,'revenue_center_code invalid'
                    ,'bill_type_code missing'
                    ,'bill_type_code invalid'
                    ,'place_of_service_code missing'
                    ,'place_of_service_code invalid'
                    ,'claim_type invalid'
                    ,'claim_type missing'
                    ,'claim_id missing'
                    ,'patient_id missing'
                    ,'hcpcs_code missing'
                    )
    and source_table = 'normalized_input__medical_claim'
    group by
        source_table
        , test_name

    union all 

    select 
        'readmissions' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__medical_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('diagnosis_code_1 missing'
                    ,'diagnosis_code_1 invalid'
                    ,'revenue_center_code missing'
                    ,'revenue_center_code invalid'
                    ,'discharge_disposition_code missing'
                    ,'discharge_disposition_code invalid'
                    ,'ms_drg_code missing'
                    ,'ms_drg_code invalid'
                    ,'bill_type_code missing'
                    ,'bill_type_code invalid'
                    ,'place_of_service_code missing'
                    ,'place_of_service_code invalid'
                    ,'claim_type invalid'
                    ,'claim_type missing'
                    ,'claim_id missing'
                    ,'patient_id missing'
                    ,'claim_start_date missing'
                    ,'claim_end_date missing'
                    ,'facility_npi missing'
                    )
    and source_table = 'normalized_input__medical_claim'
    group by
        source_table
        , test_name

    union all 

    select 
        'pmpm' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__medical_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('patient_id missing'
                    ,'claim_start_date missing'
                    ,'claim_type invalid'
                    ,'claim_type missing'

                    )
    and source_table = 'normalized_input__medical_claim'
    group by
        source_table
        , test_name

    union all 

    select 
        'readmissions' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__pharmacy_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('patient_id missing'
                    ,'dispensing_date missing'
                    ,'claim_type invalid'
                    ,'claim_type missing'
                    ,'paid_amount missing'
                    )
    and source_table = 'normalized_input__pharmacy_claim'
    group by
        source_table
        , test_name

    union all 

    select 
        'member_months' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct patient_id||data_source) from {{ ref('normalized_input__eligibility') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('patient_id missing'
                    ,'enrollment_start_date missing'
                    ,'enrollment_end_date invalid'
                    ,'payer missing'
                    ,'payer missing'
                    ,'payer_type invalid'
                    )
    and source_table = 'normalized_input__eligibility'
    group by
        source_table
        , test_name

    union all 

    select 
        'chronic_conditions' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__medical_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('diagnosis_code_1 missing'
                    ,'diagnosis_code_1 invalid'
                    ,'claim_start_date missing'
                    ,'claim_start_date invalid'
                    ,'patient_id missing'
                    ,'patient_id invalid'
                    ,'diagnosis_code_type missing'
                    ,'diagnosis_code_type invalid'
                    ,'procedure_code_1 missing'
                    ,'procedure_code_1 invalid'
                    ,'procedure_code_type invalid'
                    ,'procedure_code_type missing'
                    /**  encounter fields  **/
                    ,'diagnosis_code_1 missing'
                    ,'diagnosis_code_1 invalid'
                    ,'revenue_center_code missing'
                    ,'revenue_center_code invalid'
                    ,'discharge_disposition_code missing'
                    ,'discharge_disposition_code invalid'
                    ,'ms_drg_code missing'
                    ,'ms_drg_code invalid'
                    ,'bill_type_code missing'
                    ,'bill_type_code invalid'
                    ,'place_of_service_code missing'
                    ,'place_of_service_code invalid'
                    ,'claim_type invalid'
                    ,'claim_type missing'
                    ,'claim_id missing'
                    ,'patient_id missing'
                    ,'claim_start_date missing'
                    ,'claim_end_date missing'
                    ,'facility_npi missing'
                    )
    and source_table = 'normalized_input__medical_claim'
    group by
        source_table
        , test_name

    union all 

    select 
        'chronic_conditions' as use_case
        , source_table
        , test_name
        , count(distinct foreign_key||data_source) as failures
        , (select count(distinct claim_id||data_source) from {{ ref('normalized_input__pharmacy_claim') }} ) as denominator
    from {{ ref('data_quality__claims_preprocessing_test_detail')}}
    where 1=1
    and test_name in ('ndc_code missing'
                    ,'patient_id missing'
                    ,'paid_date missing'
                    )
    and source_table = 'normalized_input__pharmacy_claim'
    group by
        source_table
        , test_name
)

select *, '{{ var('tuva_last_run')}}' as tuva_last_run from use_case_stage