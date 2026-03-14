---
id: overview
title: "6. Data Marts"
sidebar_label: "Overview"
github_path: "/tuva/models/data_marts"
hide_table_of_contents: true
---

import InputLayerDictionaryTable from '@site/src/components/InputLayerDictionaryTable';

export const DATA_MART_TABLE_OPTIONS = [
  {
    groupLabel: 'AHRQ Measures',
    label: 'pqi_denom_long',
    modelName: 'ahrq_measures__pqi_denom_long',
    yamlPath: 'models/data_marts/ahrq_measures/pqi/pqi_models.yml',
  },
  {
    groupLabel: 'AHRQ Measures',
    label: 'pqi_exclusion_long',
    modelName: 'ahrq_measures__pqi_exclusion_long',
    yamlPath: 'models/data_marts/ahrq_measures/pqi/pqi_models.yml',
  },
  {
    groupLabel: 'AHRQ Measures',
    label: 'pqi_num_long',
    modelName: 'ahrq_measures__pqi_num_long',
    yamlPath: 'models/data_marts/ahrq_measures/pqi/pqi_models.yml',
  },
  {
    groupLabel: 'AHRQ Measures',
    label: 'pqi_rate',
    modelName: 'ahrq_measures__pqi_rate',
    yamlPath: 'models/data_marts/ahrq_measures/pqi/pqi_models.yml',
  },
  {
    groupLabel: 'AHRQ Measures',
    label: 'pqi_summary',
    modelName: 'ahrq_measures__pqi_summary',
    yamlPath: 'models/data_marts/ahrq_measures/pqi/pqi_models.yml',
  },
  {
    groupLabel: 'CCSR',
    label: 'long_condition_category',
    modelName: 'ccsr__long_condition_category',
    yamlPath: 'models/data_marts/ccsr/ccsr_models.yml',
  },
  {
    groupLabel: 'CCSR',
    label: 'long_procedure_category',
    modelName: 'ccsr__long_procedure_category',
    yamlPath: 'models/data_marts/ccsr/ccsr_models.yml',
  },
  {
    groupLabel: 'CCSR',
    label: 'singular_condition_category',
    modelName: 'ccsr__singular_condition_category',
    yamlPath: 'models/data_marts/ccsr/ccsr_models.yml',
  },
  {
    groupLabel: 'Chronic Conditions',
    label: 'cms_chronic_conditions_long',
    modelName: 'chronic_conditions__cms_chronic_conditions_long',
    yamlPath: 'models/data_marts/chronic_conditions/cms_chronic_conditions_models.yml',
  },
  {
    groupLabel: 'Chronic Conditions',
    label: 'cms_chronic_conditions_wide',
    modelName: 'chronic_conditions__cms_chronic_conditions_wide',
    yamlPath: 'models/data_marts/chronic_conditions/cms_chronic_conditions_models.yml',
  },
  {
    groupLabel: 'Chronic Conditions',
    label: 'tuva_chronic_conditions_long',
    modelName: 'chronic_conditions__tuva_chronic_conditions_long',
    yamlPath: 'models/data_marts/chronic_conditions/tuva_chronic_conditions_models.yml',
  },
  {
    groupLabel: 'Chronic Conditions',
    label: 'tuva_chronic_conditions_wide',
    modelName: 'chronic_conditions__tuva_chronic_conditions_wide',
    yamlPath: 'models/data_marts/chronic_conditions/tuva_chronic_conditions_models.yml',
  },
  {
    groupLabel: 'CMS-HCCs',
    label: 'patient_risk_factors',
    modelName: 'cms_hcc__patient_risk_factors',
    yamlPath: 'models/data_marts/cms_hcc/cms_hcc_models.yml',
  },
  {
    groupLabel: 'CMS-HCCs',
    label: 'patient_risk_factors_monthly',
    modelName: 'cms_hcc__patient_risk_factors_monthly',
    yamlPath: 'models/data_marts/cms_hcc/cms_hcc_models.yml',
  },
  {
    groupLabel: 'CMS-HCCs',
    label: 'patient_risk_scores',
    modelName: 'cms_hcc__patient_risk_scores',
    yamlPath: 'models/data_marts/cms_hcc/cms_hcc_models.yml',
  },
  {
    groupLabel: 'CMS-HCCs',
    label: 'patient_risk_scores_monthly',
    modelName: 'cms_hcc__patient_risk_scores_monthly',
    yamlPath: 'models/data_marts/cms_hcc/cms_hcc_models.yml',
  },
  {
    groupLabel: 'ED Classification',
    label: 'summary',
    modelName: 'ed_classification__summary',
    yamlPath: 'models/data_marts/ed_classification/ed_classification_models.yml',
  },
  {
    groupLabel: 'Encounter Grouper',
    label: 'encounter',
    modelName: 'core__encounter',
    yamlPath: 'models/core/core_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'condition',
    modelName: 'fhir_preprocessing__condition',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'coverage',
    modelName: 'fhir_preprocessing__coverage',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'explanation_of_benefit',
    modelName: 'fhir_preprocessing__explanation_of_benefit',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'medication_dispense',
    modelName: 'fhir_preprocessing__medication_dispense',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'observation',
    modelName: 'fhir_preprocessing__observation',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'patient',
    modelName: 'fhir_preprocessing__patient',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'FHIR Preprocessing',
    label: 'procedure',
    modelName: 'fhir_preprocessing__procedure',
    yamlPath: 'models/data_marts/fhir_preprocessing/fhir_preprocessing_models.yml',
  },
  {
    groupLabel: 'Financial PMPM',
    label: 'pmpm_prep',
    modelName: 'financial_pmpm__pmpm_prep',
    yamlPath: 'models/data_marts/financial_pmpm/financial_pmpm_models.yml',
  },
  {
    groupLabel: 'Financial PMPM',
    label: 'pmpm_payer_plan',
    modelName: 'financial_pmpm__pmpm_payer_plan',
    yamlPath: 'models/data_marts/financial_pmpm/financial_pmpm_models.yml',
  },
  {
    groupLabel: 'Financial PMPM',
    label: 'pmpm_payer',
    modelName: 'financial_pmpm__pmpm_payer',
    yamlPath: 'models/data_marts/financial_pmpm/financial_pmpm_models.yml',
  },
  {
    groupLabel: 'HCC Recapture',
    label: 'gap_status',
    modelName: 'hcc_recapture__gap_status',
    yamlPath: 'models/data_marts/hcc_recapture/final_models.yml',
  },
  {
    groupLabel: 'HCC Recapture',
    label: 'hcc_status',
    modelName: 'hcc_recapture__hcc_status',
    yamlPath: 'models/data_marts/hcc_recapture/final_models.yml',
  },
  {
    groupLabel: 'HCC Recapture',
    label: 'recapture_rates',
    modelName: 'hcc_recapture__recapture_rates',
    yamlPath: 'models/data_marts/hcc_recapture/final_models.yml',
  },
  {
    groupLabel: 'HCC Recapture',
    label: 'recapture_rates_monthly',
    modelName: 'hcc_recapture__recapture_rates_monthly',
    yamlPath: 'models/data_marts/hcc_recapture/final_models.yml',
  },
  {
    groupLabel: 'HCC Recapture',
    label: 'recapture_rates_monthly_ytd',
    modelName: 'hcc_recapture__recapture_rates_monthly_ytd',
    yamlPath: 'models/data_marts/hcc_recapture/final_models.yml',
  },
  {
    groupLabel: 'HCC Suspecting',
    label: 'list',
    modelName: 'hcc_suspecting__list',
    yamlPath: 'models/data_marts/hcc_suspecting/hcc_suspecting_models.yml',
  },
  {
    groupLabel: 'HCC Suspecting',
    label: 'list_rollup',
    modelName: 'hcc_suspecting__list_rollup',
    yamlPath: 'models/data_marts/hcc_suspecting/hcc_suspecting_models.yml',
  },
  {
    groupLabel: 'HCC Suspecting',
    label: 'summary',
    modelName: 'hcc_suspecting__summary',
    yamlPath: 'models/data_marts/hcc_suspecting/hcc_suspecting_models.yml',
  },
  {
    groupLabel: 'Pharmacy',
    label: 'brand_generic_opportunity',
    modelName: 'pharmacy__brand_generic_opportunity',
    yamlPath: 'models/data_marts/pharmacy/pharmacy_models.yml',
  },
  {
    groupLabel: 'Pharmacy',
    label: 'generic_available_list',
    modelName: 'pharmacy__generic_available_list',
    yamlPath: 'models/data_marts/pharmacy/pharmacy_models.yml',
  },
  {
    groupLabel: 'Pharmacy',
    label: 'pharmacy_claim_expanded',
    modelName: 'pharmacy__pharmacy_claim_expanded',
    yamlPath: 'models/data_marts/pharmacy/pharmacy_models.yml',
  },
  {
    groupLabel: 'Tuva Provider Attribution',
    label: 'assigned_beneficiaries_current',
    modelName: 'provider_attribution__assigned_beneficiaries_current',
    yamlPath: 'models/data_marts/provider_attribution/provider_attribution_models.yml',
  },
  {
    groupLabel: 'Tuva Provider Attribution',
    label: 'assigned_beneficiaries_yearly',
    modelName: 'provider_attribution__assigned_beneficiaries_yearly',
    yamlPath: 'models/data_marts/provider_attribution/provider_attribution_models.yml',
  },
  {
    groupLabel: 'Tuva Provider Attribution',
    label: 'provider_ranking',
    modelName: 'provider_attribution__provider_ranking',
    yamlPath: 'models/data_marts/provider_attribution/provider_attribution_models.yml',
  },
  {
    groupLabel: 'Quality Measures',
    label: 'summary_counts',
    modelName: 'quality_measures__summary_counts',
    yamlPath: 'models/data_marts/quality_measures/quality_measures_models.yml',
  },
  {
    groupLabel: 'Quality Measures',
    label: 'summary_long',
    modelName: 'quality_measures__summary_long',
    yamlPath: 'models/data_marts/quality_measures/quality_measures_models.yml',
  },
  {
    groupLabel: 'Quality Measures',
    label: 'summary_wide',
    modelName: 'quality_measures__summary_wide',
    yamlPath: 'models/data_marts/quality_measures/quality_measures_models.yml',
  },
  {
    groupLabel: 'Readmissions',
    label: 'readmission_summary',
    modelName: 'readmissions__readmission_summary',
    yamlPath: 'models/data_marts/readmissions/readmissions_models.yml',
  },
  {
    groupLabel: 'Readmissions',
    label: 'encounter_augmented',
    modelName: 'readmissions__encounter_augmented',
    yamlPath: 'models/data_marts/readmissions/readmissions_models.yml',
  },
  {
    groupLabel: 'Service Category Grouper',
    label: 'service_category_grouper',
    modelName: 'service_category__service_category_grouper',
    yamlPath: 'models/claims_preprocessing/service_category/service_category_models.yml',
  },
];

Data Marts run automatically on top of the Core Data Model to further enrich the data with higher-level concepts for analytics.  They create concepts like measures (cost, utilization, quality, outcomes), groupers (service categories, encounters, chronic conditions), and risk models (HCC Scores, RAFs, Suspecting).  

Data Marts are one of the most important parts of the Tuva Project because the concepts they create are what make doing interesting analytics possible.  For example, the Predictive Models and Dashboards + Reports that we build rely on many of the Data Marts and would not be possible without them.

Every Data Mart is fully documented in this section, including:

- Methods: Explains the methodology and rationale for how and why the Data Mart was constructed
- Data Dictionary: Fully describes the output tables of the Data Mart, which are intended for use in analytics
- Example SQL: Provides examples of how to query the output data tables to do analytics

<InputLayerDictionaryTable
  tableOptions={DATA_MART_TABLE_OPTIONS}
  defaultModelName="ahrq_measures__pqi_denom_long"
  showMappingInstructions={false}
  showRequiredForDataMart={false}
/>
