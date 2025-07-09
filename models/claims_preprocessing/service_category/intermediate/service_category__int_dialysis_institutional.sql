with service_category__stg_outpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_outpatient_institutional') }}
)
select
    medical_claim_sk
    , 'outpatient' as service_category_1
    , 'dialysis' as service_category_2
    , 'dialysis' as service_category_3
from service_category__stg_outpatient_institutional
where
    bill_type_code in ('72')
    or primary_taxonomy_code in (
        '2472R0900X'
        , '163WD1100X'
        , '163WH0500X'
        , '261QE0700X'
        )
    or ccs_category in ('91', '58', '57')
    or revenue_center_code in ('0082', '0083', '0084', '0085', '0088') -- TODO: incorrect codes?
