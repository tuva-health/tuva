create schema if not exists terminology;

CREATE OR REPLACE TABLE `terminology.admit_source`
(
  admit_source_code STRING,
  admit_source_description STRING,
  newborn_description STRING
);
load data into terminology.admit_source (
  admit_source_code STRING,
  admit_source_description STRING,
  newborn_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/admit_source.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.admit_type`
(
  admit_type_code STRING,
  admit_type_description STRING
);
load data into terminology.admit_type (
  admit_type_code STRING,
  admit_type_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/admit_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.ansi_fips_state`
(
  ansi_fips_state_code STRING,
  ansi_fips_state_abbreviation STRING,
  ansi_fips_state_name STRING
);
load data into terminology.ansi_fips_state (
  ansi_fips_state_code STRING,
  ansi_fips_state_abbreviation STRING,
  ansi_fips_state_name STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/ansi_fips_state.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.apr_drg`
(
  apr_drg_code STRING,
  severity STRING,
  apr_drg_description STRING
);
load data into terminology.apr_drg (
  apr_drg_code STRING,
  severity STRING,
  apr_drg_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/apr_drg.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.bill_type`
(
  bill_type_code STRING,
  bill_type_description STRING,
  deprecated INT64,
  deprecated_date DATE
);
load data into terminology.bill_type (
  bill_type_code STRING,
  bill_type_description STRING,
  deprecated INT64,
  deprecated_date DATE
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/bill_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.calendar`
(
  FULL_DATE DATE,
  YEAR INT64,
  MONTH INT64,
  DAY INT64,
  MONTH_NAME STRING,
  DAY_OF_WEEK_NUMBER INT64,
  DAY_OF_WEEK_NAME STRING,
  WEEK_OF_YEAR INT64,
  DAY_OF_YEAR INT64,
  YEAR_MONTH STRING,
  FIRST_DAY_OF_MONTH DATE,
  LAST_DAY_OF_MONTH DATE
);
load data into terminology.calendar (
  FULL_DATE DATE,
  YEAR INT64,
  MONTH INT64,
  DAY INT64,
  MONTH_NAME STRING,
  DAY_OF_WEEK_NUMBER INT64,
  DAY_OF_WEEK_NAME STRING,
  WEEK_OF_YEAR INT64,
  DAY_OF_YEAR INT64,
  YEAR_MONTH STRING,
  FIRST_DAY_OF_MONTH DATE,
  LAST_DAY_OF_MONTH DATE
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/calendar.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.claim_type`
(
  claim_type STRING
);
load data into terminology.claim_type (
  claim_type STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/claim_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.code_type`
(
  code_type STRING
);
load data into terminology.code_type (
  code_type STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/code_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.discharge_disposition`
(
  discharge_disposition_code STRING,
  discharge_disposition_description STRING
);
load data into terminology.discharge_disposition (
  discharge_disposition_code STRING,
  discharge_disposition_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/discharge_disposition.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.encounter_type`
(
  encounter_type STRING
);
load data into terminology.encounter_type (
  encounter_type STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/encounter_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.ethnicity`
(
  code STRING,
  description STRING
);
load data into terminology.ethnicity (
  code STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/ethnicity.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.fips_county`
(
  fips_code STRING,
  county STRING,
  state STRING
);
load data into terminology.fips_county (
  fips_code STRING,
  county STRING,
  state STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/fips_county.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.gender`
(
  gender STRING
);
load data into terminology.gender (
  gender STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/gender.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.hcpcs_level_2`
(
  hcpcs STRING,
  seqnum STRING,
  recid STRING,
  long_description STRING,
  short_description STRING
);
load data into terminology.hcpcs_level_2 (
  hcpcs STRING,
  seqnum STRING,
  recid STRING,
  long_description STRING,
  short_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/hcpcs_level_2.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.icd_10_cm`
(
  icd_10_cm STRING,
  valid_flag STRING,
  short_description STRING,
  long_description STRING
);
load data into terminology.icd_10_cm (
  icd_10_cm STRING,
  valid_flag STRING,
  short_description STRING,
  long_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/icd_10_cm.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.icd_10_pcs`
(
  icd_10_pcs STRING,
  valid_flag STRING,
  short_description STRING,
  long_description STRING
);
load data into terminology.icd_10_pcs (
  icd_10_pcs STRING,
  valid_flag STRING,
  short_description STRING,
  long_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/icd_10_pcs.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.icd_9_cm`
(
  icd_9_cm STRING,
  long_description STRING,
  short_description STRING

);
load data into terminology.icd_9_cm (
  icd_9_cm STRING,
  short_description STRING,
  long_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/icd_9_cm.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.mdc`
(
  mdc_code STRING,
  mdc_description STRING
);
load data into terminology.mdc (
  mdc_code STRING,
  mdc_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/mdc.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.medicare_dual_eligibility`
(
  dual_status_code STRING,
  dual_status_description STRING
);
load data into terminology.medicare_dual_eligibility (
  dual_status_code STRING,
  dual_status_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/medicare_dual_eligibility.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.medicare_status`
(
  medicare_status_code STRING,
  medicare_status_description STRING
);
load data into terminology.medicare_status (
  medicare_status_code STRING,
  medicare_status_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/medicare_status.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.ms_drg`
(
  ms_drg_code STRING,
  mdc_code STRING,
  medical_surgical STRING,
  ms_drg_description STRING,
  deprecated INT64,
  deprecated_date DATE
);
load data into terminology.ms_drg (
  ms_drg_code STRING,
  mdc_code STRING,
  medical_surgical STRING,
  ms_drg_description STRING,
  deprecated INT64,
  deprecated_date DATE
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/ms_drg.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.other_provider_taxonomy`
(
  npi STRING,
  taxonomy_code STRING,
  medicare_specialty_code STRING,
  description STRING,
  primary_flag INT64
);
load data into terminology.other_provider_taxonomy (
  npi STRING,
  taxonomy_code STRING,
  medicare_specialty_code STRING,
  description STRING,
  primary_flag INT64
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/other_provider_taxonomy.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.payer_type`
(
  payer_type STRING
);
load data into terminology.payer_type (
  payer_type STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/payer_type.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.place_of_service`
(
  place_of_service_code STRING,
  place_of_service_description STRING
);
load data into terminology.place_of_service (
  place_of_service_code STRING,
  place_of_service_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/place_of_service.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.present_on_admission`
(
  present_on_admit_code STRING,
  present_on_admit_description STRING
);
load data into terminology.present_on_admission (
  present_on_admit_code STRING,
  present_on_admit_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/present_on_admission.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.provider`
(
  npi STRING,
  entity_type_code STRING,
  entity_type_description STRING,
  primary_taxonomy_code STRING,
  primary_specialty_description STRING,
  provider_name STRING,
  parent_organization_name STRING,
  practice_address_line_1 STRING,
  practice_address_line_2 STRING,
  practice_city STRING,
  practice_state STRING,
  practice_zip_code STRING,
  last_updated DATE,
  deactivation_date DATE,
  deactivation_flag STRING
);
load data into terminology.provider (
  npi STRING,
  entity_type_code STRING,
  entity_type_description STRING,
  primary_taxonomy_code STRING,
  primary_specialty_description STRING,
  provider_name STRING,
  parent_organization_name STRING,
  practice_address_line_1 STRING,
  practice_address_line_2 STRING,
  practice_city STRING,
  practice_state STRING,
  practice_zip_code STRING,
  last_updated DATE,
  deactivation_date DATE,
  deactivation_flag STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/provider.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.race`
(
  code STRING,
  description STRING
);
load data into terminology.race (
  code STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/race.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.revenue_center`
(
  revenue_center_code STRING,
  revenue_center_description STRING
);
load data into terminology.revenue_center (
  revenue_center_code STRING,
  revenue_center_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/revenue_center.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE `terminology.ssa_fips_state`
(
  ssa_fips_state_code STRING,
  ssa_fips_state_name STRING
);
load data into terminology.ssa_fips_state (
  ssa_fips_state_code STRING,
  ssa_fips_state_name STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/terminology/ssa_fips_state.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


create schema if not exists value_sets;



CREATE OR REPLACE TABLE value_sets.acute_diagnosis_ccs (
  ccs_diagnosis_category STRING,
  description STRING
);
load data into value_sets.acute_diagnosis_ccs (
  ccs_diagnosis_category STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/acute_diagnosis_ccs.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.acute_diagnosis_icd_10_cm (
  icd_10_cm STRING,
  description STRING
);
load data into value_sets.acute_diagnosis_icd_10_cm (
  icd_10_cm STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/acute_diagnosis_icd_10_cm.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.always_planned_ccs_diagnosis_category (
  ccs_diagnosis_category STRING,
  description STRING
);
load data into value_sets.always_planned_ccs_diagnosis_category (
  ccs_diagnosis_category STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/always_planned_ccs_diagnosis_category.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.always_planned_ccs_procedure_category (
  ccs_procedure_category STRING,
  description STRING
);
load data into value_sets.always_planned_ccs_procedure_category (
  ccs_procedure_category STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/always_planned_ccs_procedure_category.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.cms_chronic_conditions_hierarchy (
  condition_id INT64,
  condition STRING,
  condition_column_name STRING,
  chronic_condition_type STRING,
  condition_category STRING,
  additional_logic STRING,
  claims_qualification STRING,
  inclusion_type STRING,
  code_system STRING,
  code STRING
);
load data into value_sets.cms_chronic_conditions_hierarchy (
  condition_id INT64,
  condition STRING,
  condition_column_name STRING,
  chronic_condition_type STRING,
  condition_category STRING,
  additional_logic STRING,
  claims_qualification STRING,
  inclusion_type STRING,
  code_system STRING,
  code STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/cms_chronic_conditions_hierarchy.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.exclusion_ccs_diagnosis_category (
  ccs_diagnosis_category STRING,
  description STRING,
  exclusion_category STRING
);
load data into value_sets.exclusion_ccs_diagnosis_category (
  ccs_diagnosis_category STRING,
  description STRING,
  exclusion_category STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/exclusion_ccs_diagnosis_category.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.icd_10_cm_to_ccs (
  icd_10_cm STRING,
  description STRING,
  ccs_diagnosis_category STRING,
  ccs_description STRING
);
load data into value_sets.icd_10_cm_to_ccs (
  icd_10_cm STRING,
  description STRING,
  ccs_diagnosis_category STRING,
  ccs_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/icd_10_cm_to_ccs.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.icd_10_pcs_to_ccs (
  icd_10_pcs STRING,
  description STRING,
  ccs_procedure_category STRING,
  ccs_description STRING
);
load data into value_sets.icd_10_pcs_to_ccs (
  icd_10_pcs STRING,
  description STRING,
  ccs_procedure_category STRING,
  ccs_description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/icd_10_pcs_to_ccs.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.potentially_planned_ccs_procedure_category (
  ccs_procedure_category STRING,
  description STRING
);
load data into value_sets.potentially_planned_ccs_procedure_category (
  ccs_procedure_category STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/potentially_planned_ccs_procedure_category.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.potentially_planned_icd_10_pcs (
  icd_10_pcs STRING,
  description STRING
);
load data into value_sets.potentially_planned_icd_10_pcs (
  icd_10_pcs STRING,
  description STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/potentially_planned_icd_10_pcs.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.service_category (
  service_category_1 STRING,
  service_category_2 STRING,
  claim_type STRING,
  hcpcs_code STRING,
  bill_type_code_first_2_digits STRING,
  revenue_center_code STRING,
  valid_drg_flag STRING,
  place_of_service_code STRING
);
load data into value_sets.service_category (
  service_category_1 STRING,
  service_category_2 STRING,
  claim_type STRING,
  hcpcs_code STRING,
  bill_type_code_first_2_digits STRING,
  revenue_center_code STRING,
  valid_drg_flag STRING,
  place_of_service_code STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/service_category.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.specialty_cohort (
  ccs STRING,
  description STRING,
  specialty_cohort STRING,
  procedure_or_diagnosis STRING
);
load data into value_sets.specialty_cohort (
  ccs STRING,
  description STRING,
  specialty_cohort STRING,
  procedure_or_diagnosis STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/specialty_cohort.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.surgery_gynecology_cohort (
  icd_10_pcs STRING,
  description STRING,
  ccs_code_and_description STRING,
  specialty_cohort STRING
);
load data into value_sets.surgery_gynecology_cohort (
  icd_10_pcs STRING,
  description STRING,
  ccs_code_and_description STRING,
  specialty_cohort STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/surgery_gynecology_cohort.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );


CREATE OR REPLACE TABLE value_sets.tuva_chronic_conditions_hierarchy (
  condition_family STRING,
  condition STRING,
  icd_10_cm_code STRING,
  icd_10_cm_description STRING,
  condition_column_name STRING
);
load data into value_sets.tuva_chronic_conditions_hierarchy (
  condition_family STRING,
  condition STRING,
  icd_10_cm_code STRING,
  icd_10_cm_description STRING,
  condition_column_name STRING
)
from files (format = 'csv',
    uris = ['gs://tuva-public-resources/value-sets/tuva_chronic_conditions_hierarchy.csv*'],
    compression = 'GZIP',
    quote = '"',
    null_marker = '\\N'
    );