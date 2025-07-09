with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'outpatient hospital or clinic' as service_category_2
    , 'outpatient hospital or clinic' as service_category_3
from service_category__stg_outpatient_institutional
where ccs_category = '227' -- Consultation, evaluation, and preventative care
    or bill_type_code in (
        '13'  -- Hospital Outpatient
        , '43'  -- Religious Nonmedical Hospital Outpatient
        , '44'  -- Religious Nonmedical Hospital Other (Part B)
        , '71'  -- Clinic Rural Health Center (RHC)
        , '72'  -- Clinic Hospital-based or Independent Renal Dialysis Center
        , '73'  -- Clinic Federally Qualified Health Center (FQHC)
        , '74'  -- Clinic Other Rehabilitation Facility (ORF)
        , '75'  -- Clinic Comprehensive Outpatient Rehabilitation Facility (CORF)
        , '76'  -- Clinic Community Mental Health Center (CMHC)
        , '77'  -- Clinic Free-standing Provider-based FQHC
        , '79'   -- Clinic Other
    )
