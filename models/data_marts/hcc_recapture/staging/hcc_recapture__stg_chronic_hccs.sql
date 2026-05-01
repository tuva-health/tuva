
{% if var('hcc_recapture_chronic_hccs') %}

select
    cast(hcc_code as {{dbt.type_string()}}) as hcc_code
    , cast(model_version as {{dbt.type_string()}}) as model_version
    , cast(chronic_flag as {{dbt.type_int()}}) as chronic_flag
from {{ ref('chronic_hccs') }}

{% else %}

with hcc_diagnosis as (
    select
          payment_year
        , diagnosis_code
        , cms_hcc_v28 as hcc_code
        , 'CMS-HCC-V28' as model_version
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}
    where cms_hcc_v28_flag = 'Yes'

    union all

    select
          payment_year
        , diagnosis_code
        , cms_hcc_v24 as hcc_code
        , 'CMS-HCC-V24' as model_version
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}
    where cms_hcc_v24_flag = 'Yes'
)

, chronic_hccs as (
select distinct
      diag.hcc_code
    , diag.model_version
    , 1 as chronic_flag
from {{ ref('chronic_conditions__cms_chronic_conditions_hierarchy') }} as hier
inner join hcc_diagnosis as diag
  on hier.code = diag.diagnosis_code
where diag.payment_year = (select max(payment_year) from hcc_diagnosis)
)

select 
    hcc_code
    , model_version
    , chronic_flag
from chronic_hccs

{% endif %}