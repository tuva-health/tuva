{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_type.claim_id
    , claim_type.calculated_claim_type

    , bill.missing_bill_type_code
    , bill.always_invalid_bill_type_code
    , bill.valid_and_invalid_bill_type_code
    , bill.always_valid_bill_type_code
    , bill.unique_bill_type_code
    , bill.determinable_bill_type_code
    , bill.undeterminable_bill_type_code
    , bill.usable_bill_type_code
    , bill.assigned_bill_type_code

    , drg.missing_drg_code
    , drg.always_invalid_drg_code
    , drg.valid_and_invalid_drg_code
    , drg.always_valid_drg_code
    , drg.unique_drg_code
    , drg.determinable_drg_code
    , drg.undeterminable_drg_code
    , drg.usable_drg_code
    , drg.assigned_drg_code

    , atco.missing_admit_type_code
    , atco.always_invalid_admit_type_code
    , atco.valid_and_invalid_admit_type_code
    , atco.always_valid_admit_type_code
    , atco.unique_admit_type_code
    , atco.determinable_admit_type_code
    , atco.undeterminable_admit_type_code
    , atco.usable_admit_type_code
    , atco.assigned_admit_type_code

    , asco.missing_admit_source_code
    , asco.always_invalid_admit_source_code
    , asco.valid_and_invalid_admit_source_code
    , asco.always_valid_admit_source_code
    , asco.unique_admit_source_code
    , asco.determinable_admit_source_code
    , asco.undeterminable_admit_source_code
    , asco.usable_admit_source_code
    , asco.assigned_admit_source_code

    , ddco.missing_discharge_disposition_code
    , ddco.always_invalid_discharge_disposition_code
    , ddco.valid_and_invalid_discharge_disposition_code
    , ddco.always_valid_discharge_disposition_code
    , ddco.unique_discharge_disposition_code
    , ddco.determinable_discharge_disposition_code
    , ddco.undeterminable_discharge_disposition_code
    , ddco.usable_discharge_disposition_code
    , ddco.assigned_discharge_disposition_code

    , dx1.missing_diagnosis_code_1
    , dx1.always_invalid_diagnosis_code_1
    , dx1.valid_and_invalid_diagnosis_code_1
    , dx1.always_valid_diagnosis_code_1
    , dx1.unique_diagnosis_code_1
    , dx1.determinable_diagnosis_code_1
    , dx1.undeterminable_diagnosis_code_1
    , dx1.usable_diagnosis_code_1
    , dx1.assigned_diagnosis_code_1
    , '{{ var('tuva_last_run')}}' as tuva_last_run

from {{ ref('data_quality__claim_type') }} claim_type

left join {{ ref('data_quality__bill_type_code_summary') }} bill
    on claim_type.claim_id = bill.claim_id

left join {{ ref('data_quality__drg_code_summary') }} drg
    on bill.claim_id = drg.claim_id

left join {{ ref('data_quality__admit_type_code_summary') }} atco
    on bill.claim_id = atco.claim_id

left join {{ ref('data_quality__admit_source_code_summary') }} asco
    on bill.claim_id = asco.claim_id

left join {{ ref('data_quality__discharge_disposition_code_summary') }} ddco
    on bill.claim_id = ddco.claim_id

left join {{ ref('data_quality__diagnosis_code_1_summary') }} dx1
    on bill.claim_id = dx1.claim_id
    