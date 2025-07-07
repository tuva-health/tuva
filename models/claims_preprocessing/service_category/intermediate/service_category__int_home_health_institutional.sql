with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'home health' as service_category_2
    , 'home health' as service_category_3
from service_category__stg_medical_claim
where claim_type = 'institutional'
    and bill_type_code in (
        '31'  -- Home Health Inpatient (Part A) - Typically considered inpatient
        , '32'  -- Home Health Inpatient (Part B) - Outpatient services billed by home health agencies
        , '33'  -- Home Health Outpatient
        , '34'  -- Home Health Other (Part B)
        , '35'  -- Home Health Intermediate Care - Level I
        , '36'  -- Home Health Intermediate Care - Level II
        , '37'  -- Home Health Subacute Inpatient
        , '38'   -- Home Health Swing Beds
    )
