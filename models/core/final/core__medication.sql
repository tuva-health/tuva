{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


with source_mapping as (
{% if var('enable_normalize_engine',false) != true %}
    select
     meds.MEDICATION_ID
   , meds.PATIENT_ID
   , meds.ENCOUNTER_ID
   , meds.DISPENSING_DATE
   , meds.PRESCRIBING_DATE
   , meds.SOURCE_CODE_TYPE
   , meds.SOURCE_CODE
   , meds.SOURCE_DESCRIPTION
   , coalesce(
       meds.ndc_code
       , ndc.ndc
       ) as NDC_CODE
   ,  coalesce(
       meds.ndc_description
       , ndc.fda_description
       , ndc.rxnorm_description
       ) as NDC_DESCRIPTION
   , case
        when meds.ndc_code is not null then 'manual'
        when ndc.ndc is not null then 'automatic'
        end as ndc_mapping_method
   , coalesce(
        meds.rxnorm_code
        , rxatc.rxcui
        ) as RXNORM_CODE
   , coalesce(
       meds.rxnorm_description
       , rxatc.rxnorm_description
       ) as RXNORM_DESCRIPTION
   , case
        when meds.rxnorm_code is not null then 'manual'
        when rxatc.rxcui is not null then 'automatic'
        end as rxnorm_mapping_method
   , coalesce(
        meds.atc_code
        , rxatc.atc_3_code
        ) as ATC_CODE
   , coalesce(
        meds.atc_description
        , rxatc.atc_4_name
        ) as ATC_DESCRIPTION
   , case
        when meds.atc_code is not null then 'manual'
        when rxatc.atc_3_name is not null then 'automatic'
        end as atc_mapping_method
   , meds.ROUTE
   , meds.STRENGTH
   , meds.QUANTITY
   , meds.QUANTITY_UNIT
   , meds.DAYS_SUPPLY
   , meds.PRACTITIONER_ID
   , meds.DATA_SOURCE
   , meds.TUVA_LAST_RUN
from {{ ref('core__stg_clinical_medication')}} meds
    left join {{ref('terminology__ndc')}} ndc
        on meds.source_code_type = 'ndc'
        and meds.source_code = ndc.ndc
    left join {{ref('terminology__rxnorm_to_atc')}} rxatc
        on meds.source_code_type = 'rxnorm'
        and meds.source_code = rxatc.rxcui


{% else %}

 select
     meds.MEDICATION_ID
   , meds.PATIENT_ID
   , meds.ENCOUNTER_ID
   , meds.DISPENSING_DATE
   , meds.PRESCRIBING_DATE
   , meds.SOURCE_CODE_TYPE
   , meds.SOURCE_CODE
   , meds.SOURCE_DESCRIPTION
   , coalesce(
        meds.ndc_code
        , ndc.ndc
        , custom_mapped_ndc.normalized_code
        ) as NDC_CODE
   , coalesce(
        meds.ndc_description
        , ndc.fda_description
        , ndc.rxnorm_description
        , custom_mapped_ndc.normalized_description
        ) as NDC_DESCRIPTION
   , case
        when meds.ndc_code is not null then 'manual'
        when ndc.ndc is not null then 'automatic'
        when custom_mapped_ndc.not_mapped is not null then custom_mapped_ndc.not_mapped
        when custom_mapped_ndc.normalized_code is not null then 'custom'
        end as ndc_mapping_method
   , coalesce(
        meds.RXNORM_CODE
        , rxatc.rxcui
        , custom_mapped_rxnorm.normalized_code
        ) as RXNORM_CODE
   , coalesce(
        meds.RXNORM_CODE
        , rxatc.rxnorm_description
        , custom_mapped_rxnorm.normalized_description
        ) as RXNORM_DESCRIPTION
   , case
        when meds.rxnorm_code is not null then 'manual'
        when rxatc.rxcui is not null then 'automatic'
        when custom_mapped_rxnorm.not_mapped is not null then custom_mapped_rxnorm.not_mapped
        when custom_mapped_rxnorm.normalized_code is not null then 'custom'
        end as rxnorm_mapping_method
   , coalesce(
        meds.atc_code
        , rxatc.atc_3_code
        , custom_mapped_atc.normalized_code
        ) as ATC_CODE
   , coalesce(
        meds.atc_description
        , rxatc.atc_3_name
        , custom_mapped_atc.normalized_description
        ) as ATC_DESCRIPTION
   , case
        when meds.atc_code is not null then 'manual'
        when rxatc.atc_3_code is not null then 'automatic'
        when custom_mapped_atc.not_mapped is not null then custom_mapped_atc.not_mapped
        when custom_mapped_atc.normalized_code is not null then 'custom'
        end as atc_mapping_method
   , meds.ROUTE
   , meds.STRENGTH
   , meds.QUANTITY
   , meds.QUANTITY_UNIT
   , meds.DAYS_SUPPLY
   , meds.PRACTITIONER_ID
   , meds.DATA_SOURCE
   , meds.TUVA_LAST_RUN
from {{ ref('core__stg_clinical_medication')}} meds
    left join {{ref('terminology__ndc')}} ndc
        on meds.source_code_type = 'ndc'
        and meds.source_code = ndc.ndc
    left join {{ref('terminology__rxnorm_to_atc')}} rxatc
        on meds.source_code_type = 'rxnorm'
        and meds.source_code = rxatc.rxcui
    left join {{ ref('custom_mapped') }} custom_mapped_ndc
        on custom_mapped_ndc.normalized_code_type = 'ndc'
        and ( lower(meds.source_code_type) = lower(custom_mapped_ndc.source_code_type)
            or ( meds.source_code_type is null and custom_mapped_ndc.source_code_type is null)
            )
        and (meds.source_code = custom_mapped_ndc.source_code
            or ( meds.source_code is null and custom_mapped_ndc.source_code is null)
            )
        and (meds.source_description = custom_mapped_ndc.source_description
            or ( meds.source_description is null and custom_mapped_ndc.source_description is null)
            )
        and not (meds.source_code is null and meds.source_description is null)
    left join {{ ref('custom_mapped') }} custom_mapped_rxnorm
        on custom_mapped_rxnorm.normalized_code_type = 'rxnorm'
        and ( lower(meds.source_code_type) = lower(custom_mapped_rxnorm.source_code_type)
            or ( meds.source_code_type is null and custom_mapped_rxnorm.source_code_type is null)
            )
        and (meds.source_code = custom_mapped_rxnorm.source_code
            or ( meds.source_code is null and custom_mapped_rxnorm.source_code is null)
            )
        and (meds.source_description = custom_mapped_rxnorm.source_description
            or ( meds.source_description is null and custom_mapped_rxnorm.source_description is null)
            )
        and not (meds.source_code is null and meds.source_description is null)
    left join {{ ref('custom_mapped') }} custom_mapped_atc
        on custom_mapped_atc.normalized_code_type = 'atc'
        and ( lower(meds.source_code_type) = lower(custom_mapped_atc.source_code_type)
            or ( meds.source_code_type is null and custom_mapped_atc.source_code_type is null)
            )
        and (meds.source_code = custom_mapped_atc.source_code
            or ( meds.source_code is null and custom_mapped_atc.source_code is null)
            )
        and (meds.source_description = custom_mapped_atc.source_description
            or ( meds.source_description is null and custom_mapped_atc.source_description is null)
            )
        and not (meds.source_code is null and meds.source_description is null)
{% endif %}
   )


-- add auto rxnorm + atc
select
     sm.MEDICATION_ID
   , sm.PATIENT_ID
   , sm.ENCOUNTER_ID
   , sm.DISPENSING_DATE
   , sm.PRESCRIBING_DATE
   , sm.SOURCE_CODE_TYPE
   , sm.SOURCE_CODE
   , sm.SOURCE_DESCRIPTION
   , sm.NDC_CODE
   , sm.NDC_DESCRIPTION
   , sm.ndc_mapping_method
   , coalesce(
        sm.rxnorm_code
        , ndc.rxcui
        ) as RXNORM_CODE
   , coalesce(
        sm.RXNORM_description
        , ndc.rxnorm_description
        ) as RXNORM_DESCRIPTION
   , case
        when sm.rxnorm_mapping_method is not null then sm.rxnorm_mapping_method
        when ndc.rxcui is not null then 'automatic'
        end as rxnorm_mapping_method
   , coalesce(
        sm.atc_code
        , rxatc.atc_3_code
        ) as ATC_CODE
   , coalesce(
        sm.atc_description
        , rxatc.atc_3_name
        ) as ATC_DESCRIPTION
   , case
        when sm.atc_mapping_method is not null then sm.atc_mapping_method
        when rxatc.atc_3_name is not null then 'automatic'
        end as atc_mapping_method
   , sm.ROUTE
   , sm.STRENGTH
   , sm.QUANTITY
   , sm.QUANTITY_UNIT
   , sm.DAYS_SUPPLY
   , sm.PRACTITIONER_ID
   , sm.DATA_SOURCE
   , sm.TUVA_LAST_RUN
from source_mapping sm
    left join {{ref('terminology__ndc')}} ndc
        on sm.ndc_code = ndc.ndc
    left join {{ref('terminology__rxnorm_to_atc')}} rxatc
        on coalesce( sm.rxnorm_code, ndc.rxcui ) = rxatc.rxcui
