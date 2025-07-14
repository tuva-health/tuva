with encounters__stg_office_based as (
    select *
    from {{ ref('encounters__stg_office_based') }}
)
select *
    , {{ dbt_utils.generate_surrogate_key(['patient_sk', 'start_date']) }} as encounter_id
    , case when service_category_2 = 'office-based visit' then 1 else 0 end as em_flag
    , case when substring(hcpcs_code, 1, 1) = 'J' then 1 else 0 end as injections_flag
    , case when service_category_2 = 'office-based pt/ot/st' then 1 else 0 end as ptotst_flag
    , case when service_category_2 = 'office-based radiology' then 1 else 0 end as radiology_flag
    , case when service_category_2 = 'office-based surgery' then 1 else 0 end as surgery_flag
    , case when service_category_2 = 'telehealth visit' then 1 else 0 end as telehealth_flag
from encounters__stg_office_based
