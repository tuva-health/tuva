{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


with source_mapping as (
{% if var('enable_normalize_engine',false) != true %}
    select
     meds.medication_id
   , meds.person_id
   , meds.patient_id
   , meds.encounter_id
   , meds.dispensing_date
   , meds.prescribing_date
   , meds.source_code_type
   , meds.source_code
   , meds.source_description
   , coalesce(
       meds.ndc_code
       , ndc.ndc
       ) as ndc_code
   , coalesce(
       meds.ndc_description
       , ndc.fda_description
       , ndc.rxnorm_description
       ) as ndc_description
   , case
        when meds.ndc_code is not null then 'manual'
        when ndc.ndc is not null then 'automatic'
        end as ndc_mapping_method
   , coalesce(
        meds.rxnorm_code
        , rxatc.rxcui
        ) as rxnorm_code
   , coalesce(
       meds.rxnorm_description
       , rxatc.rxnorm_description
       ) as rxnorm_description
   , case
        when meds.rxnorm_code is not null then 'manual'
        when rxatc.rxcui is not null then 'automatic'
        end as rxnorm_mapping_method
   , coalesce(
        meds.atc_code
        , rxatc.atc_3_code
        ) as atc_code
   , coalesce(
        meds.atc_description
        , rxatc.atc_4_name
        ) as atc_description
   , case
        when meds.atc_code is not null then 'manual'
        when rxatc.atc_3_name is not null then 'automatic'
        end as atc_mapping_method
   , meds.route
   , meds.strength
   , meds.quantity
   , meds.quantity_unit
   , meds.days_supply
   , meds.practitioner_id
   , meds.data_source
   , meds.tuva_last_run
from {{ ref('core__stg_clinical_medication') }} as meds
    left outer join {{ ref('terminology__ndc') }} as ndc
        on meds.source_code_type = 'ndc'
        and meds.source_code = ndc.ndc
    left outer join {{ ref('terminology__rxnorm_to_atc') }} as rxatc
        on meds.source_code_type = 'rxnorm'
        and meds.source_code = rxatc.rxcui


{% else %}

 select
     meds.medication_id
   , meds.person_id
   , meds.patient_id
   , meds.encounter_id
   , meds.dispensing_date
   , meds.prescribing_date
   , meds.source_code_type
   , meds.source_code
   , meds.source_description
   , coalesce(
        meds.ndc_code
        , ndc.ndc
        , custom_mapped_ndc.normalized_code
        ) as ndc_code
   , coalesce(
        meds.ndc_description
        , ndc.fda_description
        , ndc.rxnorm_description
        , custom_mapped_ndc.normalized_description
        ) as ndc_description
   , case
        when meds.ndc_code is not null then 'manual'
        when ndc.ndc is not null then 'automatic'
        when custom_mapped_ndc.not_mapped is not null then custom_mapped_ndc.not_mapped
        when custom_mapped_ndc.normalized_code is not null then 'custom'
        end as ndc_mapping_method
   , coalesce(
        meds.rxnorm_code
        , rxatc.rxcui
        , custom_mapped_rxnorm.normalized_code
        ) as rxnorm_code
   , coalesce(
        meds.rxnorm_code
        , rxatc.rxnorm_description
        , custom_mapped_rxnorm.normalized_description
        ) as rxnorm_description
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
        ) as atc_code
   , coalesce(
        meds.atc_description
        , rxatc.atc_3_name
        , custom_mapped_atc.normalized_description
        ) as atc_description
   , case
        when meds.atc_code is not null then 'manual'
        when rxatc.atc_3_code is not null then 'automatic'
        when custom_mapped_atc.not_mapped is not null then custom_mapped_atc.not_mapped
        when custom_mapped_atc.normalized_code is not null then 'custom'
        end as atc_mapping_method
   , meds.route
   , meds.strength
   , meds.quantity
   , meds.quantity_unit
   , meds.days_supply
   , meds.practitioner_id
   , meds.data_source
   , meds.tuva_last_run
from {{ ref('core__stg_clinical_medication') }} meds
    left join {{ ref('terminology__ndc') }} ndc
        on meds.source_code_type = 'ndc'
        and meds.source_code = ndc.ndc
    left join {{ ref('terminology__rxnorm_to_atc') }} rxatc
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
     sm.medication_id
   , sm.person_id
   , sm.encounter_id
   , sm.dispensing_date
   , sm.prescribing_date
   , sm.source_code_type
   , sm.source_code
   , sm.source_description
   , sm.ndc_code
   , sm.ndc_description
   , sm.ndc_mapping_method
   , coalesce(
        sm.rxnorm_code
        , ndc.rxcui
        ) as rxnorm_code
   , coalesce(
        sm.rxnorm_description
        , ndc.rxnorm_description
        ) as rxnorm_description
   , case
        when sm.rxnorm_mapping_method is not null then sm.rxnorm_mapping_method
        when ndc.rxcui is not null then 'automatic'
        end as rxnorm_mapping_method
   , coalesce(
        sm.atc_code
        , rxatc.atc_3_code
        ) as atc_code
   , coalesce(
        sm.atc_description
        , rxatc.atc_3_name
        ) as atc_description
   , case
        when sm.atc_mapping_method is not null then sm.atc_mapping_method
        when rxatc.atc_3_name is not null then 'automatic'
        end as atc_mapping_method
   , sm.route
   , sm.strength
   , sm.quantity
   , sm.quantity_unit
   , sm.days_supply
   , sm.practitioner_id
   , sm.data_source
   , sm.tuva_last_run
from source_mapping as sm
    left outer join {{ ref('terminology__ndc') }} as ndc
        on sm.ndc_code = ndc.ndc
    left outer join {{ ref('terminology__rxnorm_to_atc') }} as rxatc
        on coalesce(sm.rxnorm_code, ndc.rxcui) = rxatc.rxcui
