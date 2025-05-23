{{ config(
    enabled = var('claims_enabled', False)
) }}

with core__medical_claim as (

    select count(*) as row_count
    from {{ ref('core__medical_claim') }}

)

, core__pharmacy_claim as (

    select count(*) as row_count 
    from {{ ref('core__pharmacy_claim') }}

)

, core__eligibility as (

    select count(*) as row_count 
    from {{ ref('core__eligibility') }}

)

, core__patient as (

    select count(*) as row_count 
    from {{ ref('core__patient') }}

)

, core__encounter as (

    select count(*) as row_count 
    from {{ ref('core__encounter') }}

)

, core__condition as (
    
    select count(*) as row_count 
    from {{ ref('core__condition') }}

)

, core__procedure as (

    select count(*) as row_count 
    from {{ ref('core__procedure') }}

)

, core__practitioner as (

    select count(*) as row_count 
    from {{ ref('core__practitioner') }}

)

, core__location as (

    select count(*) as row_count 
    from {{ ref('core__location') }}

)

, ahrq_measures__pqi_denom_long as (

    select count(*) as row_count 
    from {{ ref('ahrq_measures__pqi_denom_long') }}

)

, ahrq_measures__pqi_exclusion_long as (

    select count(*) as row_count 
    from {{ ref('ahrq_measures__pqi_exclusion_long') }}

)

, ahrq_measures__pqi_num_long as (

    select count(*) as row_count 
    from {{ ref('ahrq_measures__pqi_num_long') }}

)

, ahrq_measures__pqi_rate as (

    select count(*) as row_count 
    from {{ ref('ahrq_measures__pqi_rate') }}

)

, ahrq_measures__pqi_summary as (

    select count(*) as row_count 
    from {{ ref('ahrq_measures__pqi_summary') }}

)

, ccsr__long_condition_category as (

    select count(*) as row_count 
    from {{ ref('ccsr__long_condition_category') }}

)

, ccsr__long_procedure_category as (

    select count(*) as row_count 
    from {{ ref('ccsr__long_procedure_category') }}

)

, ccsr__singular_condition_category as (

    select count(*) as row_count 
    from {{ ref('ccsr__singular_condition_category') }}

)

, chronic_conditions__cms_chronic_conditions_long as (

    select count(*) as row_count 
    from {{ ref('chronic_conditions__cms_chronic_conditions_long') }}

)

, chronic_conditions__cms_chronic_conditions_wide as (

    select count(*) as row_count 
    from {{ ref('chronic_conditions__cms_chronic_conditions_wide') }}

)

, chronic_conditions__tuva_chronic_conditions_long as (

    select count(*) as row_count 
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}

)

, chronic_conditions__tuva_chronic_conditions_wide as (

    select count(*) as row_count 
    from {{ ref('chronic_conditions__tuva_chronic_conditions_wide') }}

)

, cms_hcc__patient_risk_factors as (

    select count(*) as row_count 
    from {{ ref('cms_hcc__patient_risk_factors') }}

)

, cms_hcc__patient_risk_factors_monthly as (

    select count(*) as row_count 
    from {{ ref('cms_hcc__patient_risk_factors_monthly') }}

)

, cms_hcc__patient_risk_scores as (

    select count(*) as row_count 
    from {{ ref('cms_hcc__patient_risk_scores') }}

)

, cms_hcc__patient_risk_scores_monthly as (

    select count(*) as row_count 
    from {{ ref('cms_hcc__patient_risk_scores_monthly') }}

)

, ed_classification__summary as (
    
    select count(*) as row_count 
    from {{ ref('ed_classification__summary') }}

)

, financial_pmpm__pmpm_prep as (

    select count(*) as row_count 
    from {{ ref('financial_pmpm__pmpm_prep') }}

)

, financial_pmpm__pmpm_payer_plan as (

    select count(*) as row_count 
    from {{ ref('financial_pmpm__pmpm_payer_plan') }}

)

, financial_pmpm__pmpm_payer as (

    select count(*) as row_count 
    from {{ ref('financial_pmpm__pmpm_payer') }}

)

, hcc_suspecting__list as (

    select count(*) as row_count 
    from {{ ref('hcc_suspecting__list') }}

)

, hcc_suspecting__list_rollup as (

    select count(*) as row_count 
    from {{ ref('hcc_suspecting__list_rollup') }}

)

, hcc_suspecting__summary as (

    select count(*) as row_count 
    from {{ ref('hcc_suspecting__summary') }}

)

, pharmacy__brand_generic_opportunity as (

    select count(*) as row_count 
    from {{ ref('pharmacy__brand_generic_opportunity') }}

)

, pharmacy__generic_available_list as (

    select count(*) as row_count 
    from {{ ref('pharmacy__generic_available_list') }}

)

, pharmacy__pharmacy_claim_expanded as (

    select count(*) as row_count 
    from {{ ref('pharmacy__pharmacy_claim_expanded') }}

)

, quality_measures__summary_counts as (

    select count(*) as row_count 
    from {{ ref('quality_measures__summary_counts') }}

)

, quality_measures__summary_long as (

    select count(*) as row_count 
    from {{ ref('quality_measures__summary_long') }}

)

, quality_measures__summary_wide as (

    select count(*) as row_count 
    from {{ ref('quality_measures__summary_wide') }}

)

, readmissions__readmission_summary as (

    select count(*) as row_count 
    from {{ ref('readmissions__readmission_summary') }}

)

, readmissions__encounter_augmented as (

    select count(*) as row_count 
    from {{ ref('readmissions__encounter_augmented') }}

)

, final as (
    
    select
        'Core' as data_mart
        , 'Medical Claim' as table_name
        , row_count
    from core__medical_claim

    union all

    select
        'Core' as data_mart
        , 'Pharmacy Claim' as table_name
        , row_count
    from core__pharmacy_claim

    union all

    select
        'Core' as data_mart
        , 'Pharmacy Claim' as table_name
        , row_count
    from core__eligibility

    union all

    select
        'Core' as data_mart
        , 'Patient' as table_name
        , row_count
    from core__patient

    union all

    select
        'Core' as data_mart
        , 'Encounter' as table_name
        , row_count
    from core__encounter

    union all

    select
        'Core' as data_mart
        , 'Condition' as table_name
        , row_count
    from core__condition

    union all

    select
        'Core' as data_mart
        , 'Procedure' as table_name
        , row_count
    from core__procedure

    union all

    select
        'Core' as data_mart
        , 'Practitioner' as table_name
        , row_count
    from core__practitioner

    union all

    select
        'Core' as data_mart
        , 'Location' as table_name
        , row_count
    from core__location

    union all

    select
        'AHRQ Measures' as data_mart
        , 'PQI Denom Long' as table_name
        , row_count
    from ahrq_measures__pqi_denom_long

    union all

    select
        'AHRQ Measures' as data_mart
        , 'PQI Exclusion Long' as table_name
        , row_count
    from ahrq_measures__pqi_exclusion_long

    union all

    select
        'AHRQ Measures' as data_mart
        , 'PQI Num Long' as table_name
        , row_count
    from ahrq_measures__pqi_num_long

    union all

    select
        'AHRQ Measures' as data_mart
        , 'PQI Rate' as table_name
        , row_count
    from ahrq_measures__pqi_rate

    union all

    select
        'AHRQ Measures' as data_mart
        , 'PQI Summary' as table_name
        , row_count
    from ahrq_measures__pqi_summary

    union all

    select
        'CCSR' as data_mart
        , 'Long Condition Category' as table_name
        , row_count
    from ccsr__long_condition_category

    union all

    select
        'CCSR' as data_mart
        , 'Long Procedure Category' as table_name
        , row_count
    from ccsr__long_procedure_category

    union all

    select
        'CCSR' as data_mart
        , 'Singular Condition Category' as table_name
        , row_count
    from ccsr__singular_condition_category

    union all

    select
        'Chronic Conditions' as data_mart
        , 'CMS Chronic Conditions Long' as table_name
        , row_count
    from chronic_conditions__cms_chronic_conditions_long

    union all

    select
        'Chronic Conditions' as data_mart
        , 'CMS Chronic Conditions Wide' as table_name
        , row_count
    from chronic_conditions__cms_chronic_conditions_wide

    union all

    select
        'Chronic Conditions' as data_mart
        , 'Tuva Chronic Conditions Long' as table_name
        , row_count
    from chronic_conditions__tuva_chronic_conditions_long

    union all

    select
        'Chronic Conditions' as data_mart
        , 'Tuva Chronic Conditions Wide' as table_name
        , row_count
    from chronic_conditions__tuva_chronic_conditions_wide

    union all

    select
        'CMS HCC' as data_mart
        , 'Patient Risk Factors' as table_name
        , row_count
    from cms_hcc__patient_risk_factors

    union all

    select
        'CMS HCC' as data_mart
        , 'Patient Risk Factors Monthly' as table_name
        , row_count
    from cms_hcc__patient_risk_factors_monthly

    union all

    select
        'CMS HCC' as data_mart
        , 'Patient Risk Scores' as table_name
        , row_count
    from cms_hcc__patient_risk_scores

    union all

    select
        'CMS HCC' as data_mart
        , 'Patient Risk Scores Monthly' as table_name
        , row_count
    from cms_hcc__patient_risk_scores_monthly

    union all

    select
        'ED Classification' as data_mart
        , 'Summary' as table_name
        , row_count
    from ed_classification__summary

    union all

    select
        'Financial PMPM' as data_mart
        , 'PMPM Prep' as table_name
        , row_count
    from financial_pmpm__pmpm_prep

    union all

    select
        'Financial PMPM' as data_mart
        , 'PMPM Payer Plan' as table_name
        , row_count
    from financial_pmpm__pmpm_payer_plan

    union all

    select
        'Financial PMPM' as data_mart
        , 'PMPM Payer' as table_name
        , row_count
    from financial_pmpm__pmpm_payer

    union all

    select
        'HCC Suspecting' as data_mart
        , 'List' as table_name
        , row_count
    from hcc_suspecting__list

    union all

    select
        'HCC Suspecting' as data_mart
        , 'List Rollup' as table_name
        , row_count
    from hcc_suspecting__list_rollup

    union all

    select
        'HCC Suspecting' as data_mart
        , 'Summary' as table_name
        , row_count
    from hcc_suspecting__summary

    union all

    select
        'Pharmacy' as data_mart
        , 'Brand Generic Opportunity' as table_name
        , row_count
    from pharmacy__brand_generic_opportunity

    union all

    select
        'Pharmacy' as data_mart
        , 'Generic Available List' as table_name
        , row_count
    from pharmacy__generic_available_list

    union all

    select
        'Pharmacy' as data_mart
        , 'Pharmacy Claim Expanded' as table_name
        , row_count
    from pharmacy__pharmacy_claim_expanded

    union all

    select
        'Quality Measures' as data_mart
        , 'Summary Counts' as table_name
        , row_count
    from quality_measures__summary_counts

    union all

    select
        'Quality Measures' as data_mart
        , 'Summary Long' as table_name
        , row_count
    from quality_measures__summary_long

    union all

    select
        'Quality Measures' as data_mart
        , 'Summary Wide' as table_name
        , row_count
    from quality_measures__summary_wide

    union all

    select
        'Readmissions' as data_mart
        , 'Readmission Summary' as table_name
        , row_count
    from readmissions__readmission_summary

    union all

    select
        'Readmissions' as data_mart
        , 'Encounter Augmented' as table_name
        , row_count
    from readmissions__encounter_augmented

)

select
    data_mart
    , table_name
    , cast(row_count as {{ dbt.type_int() }}) as row_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
