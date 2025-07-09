with service_category__stg_office_based as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_office_based') }}
),
service_category__pharmacy_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_pharmacy_professional') }}
),
service_category__office_based_radiology as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_office_based_radiology') }}
),
service_category__office_based_visit_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_office_based_visit_professional') }}
),
service_category__office_based_surgery_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_office_based_surgery_professional') }}
),
service_category__office_based_physical_therapy_professional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__int_office_based_physical_therapy_professional') }}
)
select
    med.medical_claim_sk
    , 'office-based' as service_category_1
    , 'office-based other' as service_category_2
    , 'office-based other' as service_category_3
from service_category__stg_office_based as med
    left outer join service_category__pharmacy_professional as pharm
    on med.medical_claim_sk = pharm.medical_claim_sk
    left outer join service_category__office_based_radiology as rad
    on med.medical_claim_sk = rad.medical_claim_sk
    left outer join service_category__office_based_visit_professional as visit
    on med.medical_claim_sk = visit.medical_claim_sk
    left outer join service_category__office_based_surgery_professional as surg
    on med.medical_claim_sk = surg.medical_claim_sk
    left outer join service_category__office_based_physical_therapy_professional as pt
    on med.medical_claim_sk = pt.medical_claim_sk
where pharm.medical_claim_sk is null
    and rad.medical_claim_sk is null
    and visit.medical_claim_sk is null
    and surg.medical_claim_sk is null
    and pt.medical_claim_sk is null
