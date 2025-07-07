with service_category__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_medical_claim') }}
),
service_category__stg_inpatient_institutional as (
    select *
    from {{ ref('the_tuva_project', 'service_category__stg_inpatient_institutional') }}
)
select
    med.medical_claim_sk
    , 'inpatient' as service_category_1
    , 'acute inpatient' as service_category_2
    , case
        when coalesce(ms.medical_surgical, apr.medical_surgical) = 'M' then 'medical'
        when coalesce(ms.medical_surgical, apr.medical_surgical) = 'P' then 'surgical'
        when coalesce(ms.medical_surgical, apr.medical_surgical) = 'surgical' then 'surgical'
        else 'acute inpatient - other'
    end as service_category_3
from service_category__stg_medical_claim as med
    inner join service_category__stg_inpatient_institutional as a
    on med.medical_claim_sk = a.medical_claim_sk
    inner join {{ ref('tuva_data_assets', 'ms_drg') }} as ms
    on med.drg_code = ms.ms_drg_code
    and med.drg_code_type = 'ms-drg'
    inner join {{ ref('tuva_data_assets', 'apr_drg') }} as apr
    on med.drg_code = apr.apr_drg_code
    and med.drg_code_type = 'apr-drg'