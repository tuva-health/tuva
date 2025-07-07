with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
combined_service_categories as (
    select *
    from {{ ref('service_category__int_all_unioned') }}
),
service_category as (
    select *
    from {{ ref('tuva_data_assets', 'service_category') }}
),
service_category_mapping as (
    -- Each row may qualify for > 1 category, or the same category more than once.
    select distinct
        b.medical_claim_sk
        , case when s.service_category_1 is null and b.service_category_1 is not null then concat(b.service_category_1, ' (not in seed)')
            else b.service_category_1
            end as service_category_1
        , case when s.service_category_2 is null and b.service_category_2 is not null then concat(b.service_category_2, ' (not in seed)')
            else s.service_category_2
            end as service_category_2
        , case
            when s.service_category_3 is null and b.service_category_3 is not null then concat(b.service_category_3, ' (not in seed)')
            else s.service_category_3
          end as service_category_3
        , b.service_category_1 as original_service_cat_1
        , b.service_category_2 as original_service_cat_2
        , b.service_category_3 as original_service_cat_3
        , s.priority
    from combined_service_categories as b
        left outer join service_category as s
        on b.service_category_1 = s.service_category_1
        and b.service_category_2 = s.service_category_2
        and b.service_category_3 = s.service_category_3
)
/*
 * We bring in ALL priorities to the final table for use in encounter flags
 * (For example, regardless of final priority, we might still want to know if a claim could
 * ever be considered "ED" to flag inpatient events that came in through ED.
 * Filter on this table where duplicate_row_number = 1 to avoid duplicates
 */
select
    d.medical_claim_sk
    , coalesce(d.service_category_1, 'other') as service_category_1
    , coalesce(d.service_category_2, 'other') as service_category_2
    , coalesce(d.service_category_3, 'other') as service_category_3
--    , d.original_service_cat_2
--    , d.original_service_cat_3
    , row_number() over (partition by d.medical_claim_sk order by coalesce(d.priority, 999)) as priority
    -- TODO: Probably remove everything below.
--    , s.ccs_category
--    , s.ccs_category_description
--    , s.drg_code
--    , s.drg_description
--    , s.place_of_service_code
--    , s.place_of_service_description
--    , s.revenue_center_code
--    , s.revenue_center_description
--    , s.hcpcs_code
--    , s.default_ccsr_category_ip
--    , s.default_ccsr_category_op
--    , s.default_ccsr_category_description_ip
--    , s.default_ccsr_category_description_op
--    , s.primary_taxonomy_code
--    , s.primary_specialty_description
--    , s.modality
--    , s.bill_type_code
--    , s.bill_type_description
from service_category_mapping as d
--    inner join service_category__stg_medical_claim as s
--    on d.medical_claim_sk = s.medical_claim_sk
