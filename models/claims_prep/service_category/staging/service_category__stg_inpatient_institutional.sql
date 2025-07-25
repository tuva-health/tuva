/*
 * Determines whether a claims service type is "inpatient". It must be an institutional claim, have a valid DRG code,
 * and a bill type code that indicates inpatient place of care.
 */
with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
ms_drg as (
    select *
    from {{ ref('tuva_data_assets', 'ms_drg') }}
),
apr_drg as (
    select *
    from {{ ref('tuva_data_assets', 'apr_drg') }}
)
select
    med.*
    , ms_drg.mdc_code
from service_category__stg_medical_claim as med
    left outer join ms_drg
    on med.drg_code_type = 'ms-drg'
    and med.drg_code = ms_drg.ms_drg_code
    left outer join apr_drg
    on med.drg_code_type = 'apr-drg'
    and med.drg_code = apr_drg.apr_drg_code
where med.claim_type = 'institutional'
    and (
        (ms_drg.ms_drg_code is not null or apr_drg.apr_drg_code is not null)
        or bill_type_code in (
            '11'  -- Hospital Inpatient (Part A)
            , '12'  -- Hospital Inpatient (Part B)
            , '21'  -- Skilled Nursing Facility (SNF) Inpatient (Part A)
            , '82'  -- Hospital-based Hospice (Inpatient)
            , '15'  -- Hospital Intermediate Care - Level I
            , '16'  -- Hospital Intermediate Care - Level II
            , '17'  -- Hospital Subacute Inpatient
            , '18'  -- Hospital Swing Beds
            , '22'  -- Skilled Nursing Facility (SNF) Inpatient (Part B)
            , '25'  -- SNF Intermediate Care - Level I
            , '26'  -- SNF Intermediate Care - Level II
            , '27'  -- SNF Subacute Inpatient
            , '28'  -- SNF Swing Beds
            , '31'  -- Home Health Inpatient (Part A)
            , '41'  -- Religious Nonmedical Hospital Inpatient (Part A)
            , '42'  -- Religious Nonmedical Hospital Inpatient (Part B)
            , '45'  -- Religious Nonmedical Hospital Intermediate Care - Level I
            , '46'  -- Religious Nonmedical Hospital Intermediate Care - Level II
            , '47'  -- Religious Nonmedical Hospital Subacute Inpatient
            , '48'  -- Religious Nonmedical Hospital Swing Beds
            , '61'  -- Intermediate Care Inpatient (Part A)
            , '62'  -- Intermediate Care Inpatient (Part B)
            , '65'  -- Intermediate Care - Level I
            , '66'  -- Intermediate Care - Level II
            , '67'  -- Intermediate Care Subacute Inpatient
            , '68'  -- Intermediate Care Swing Beds
        )
        )