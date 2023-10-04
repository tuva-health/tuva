create schema if not exists terminology;
-- with owner <user>;



DROP TABLE IF EXISTS terminology.admit_source CASCADE;
CREATE TABLE terminology.admit_source
(
	admit_source_code VARCHAR(256)   ENCODE lzo
	,admit_source_description VARCHAR(256)   ENCODE lzo
	,newborn_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.admit_source owner to <User>;
copy terminology.admit_source
  from 's3://tuva-public-resources/terminology/admit_source.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.admit_type CASCADE;
CREATE TABLE terminology.admit_type
(
	admit_type_code VARCHAR(256)   ENCODE lzo
	,admit_type_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.admit_type owner to <User>;
copy terminology.admit_type
  from 's3://tuva-public-resources/terminology/admit_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.ansi_fips_state CASCADE;
CREATE TABLE terminology.ansi_fips_state
(
	ansi_fips_state_code VARCHAR(256)   ENCODE lzo
	,ansi_fips_state_abbreviation VARCHAR(256)   ENCODE lzo
	,ansi_fips_state_name VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.ansi_fips_state owner to <User>;
copy terminology.ansi_fips_state
  from 's3://tuva-public-resources/terminology/ansi_fips_state.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.apr_drg CASCADE;
CREATE TABLE terminology.apr_drg
(
	apr_drg_code VARCHAR(256)   ENCODE lzo
	,severity VARCHAR(256)   ENCODE lzo
	,apr_drg_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.apr_drg owner to <User>;
copy terminology.apr_drg
  from 's3://tuva-public-resources/terminology/apr_drg.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.bill_type CASCADE;
CREATE TABLE terminology.bill_type
(
	bill_type_code VARCHAR(256)   ENCODE lzo
	,bill_type_description VARCHAR(256)   ENCODE lzo
	,deprecated INTEGER   ENCODE az64
	,deprecated_date DATE   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.bill_type owner to <User>;
copy terminology.bill_type
  from 's3://tuva-public-resources/terminology/bill_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.calendar CASCADE;
CREATE TABLE terminology.calendar
(
	full_date DATE   ENCODE az64
	,"year" INTEGER   ENCODE az64
	,"month" INTEGER   ENCODE az64
	,"day" INTEGER   ENCODE az64
	,month_name VARCHAR(3)   ENCODE lzo
	,day_of_week_number INTEGER   ENCODE az64
	,day_of_week_name VARCHAR(9)   ENCODE lzo
	,week_of_year INTEGER   ENCODE az64
	,day_of_year INTEGER   ENCODE az64
	,year_month VARCHAR(7)   ENCODE lzo
	,first_day_of_month DATE   ENCODE az64
	,last_day_of_month DATE   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.calendar owner to <User>;
copy terminology.calendar
  from 's3://tuva-public-resources/terminology/calendar.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.claim_type CASCADE;
CREATE TABLE terminology.claim_type
(
	claim_type VARCHAR(13)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.claim_type owner to <User>;
copy terminology.claim_type
  from 's3://tuva-public-resources/terminology/claim_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.code_type CASCADE;
CREATE TABLE terminology.code_type
(
	code_type VARCHAR(13)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.code_type owner to <User>;
copy terminology.code_type
  from 's3://tuva-public-resources/terminology/code_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.discharge_disposition CASCADE;
CREATE TABLE terminology.discharge_disposition
(
	discharge_disposition_code VARCHAR(256)   ENCODE lzo
	,discharge_disposition_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.discharge_disposition owner to <User>;
copy terminology.discharge_disposition
  from 's3://tuva-public-resources/terminology/discharge_disposition.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.encounter_type CASCADE;
CREATE TABLE terminology.encounter_type
(
	encounter_type VARCHAR(34)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.encounter_type owner to <User>;
copy terminology.encounter_type
  from 's3://tuva-public-resources/terminology/encounter_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.ethnicity CASCADE;
CREATE TABLE terminology.ethnicity
(
	code VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.ethnicity owner to <User>;
copy terminology.ethnicity
  from 's3://tuva-public-resources/terminology/ethnicity.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.fips_county CASCADE;
CREATE TABLE terminology.fips_county
(
	fips_code VARCHAR(256)   ENCODE lzo
	,county VARCHAR(256)   ENCODE lzo
	,state VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.fips_county owner to <User>;
copy terminology.fips_county
  from 's3://tuva-public-resources/terminology/fips_county.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.gender CASCADE;
CREATE TABLE terminology.gender
(
	gender VARCHAR(7)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.gender owner to <User>;
copy terminology.gender
  from 's3://tuva-public-resources/terminology/gender.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.hcpcs_level_2 CASCADE;
CREATE TABLE terminology.hcpcs_level_2
(
	hcpcs VARCHAR(256)   ENCODE lzo
	,seqnum VARCHAR(256)   ENCODE lzo
	,recid VARCHAR(256)   ENCODE lzo
	,long_description VARCHAR(2000)   ENCODE lzo
	,short_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.hcpcs_level_2 owner to <User>;
copy terminology.hcpcs_level_2
  from 's3://tuva-public-resources/terminology/hcpcs_level_2.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.icd_10_cm CASCADE;
CREATE TABLE terminology.icd_10_cm
(
	icd_10_cm VARCHAR(256)   ENCODE lzo
	,valid_flag VARCHAR(256)   ENCODE lzo
	,short_description VARCHAR(256)   ENCODE lzo
	,long_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.icd_10_cm owner to <User>;
copy terminology.icd_10_cm
  from 's3://tuva-public-resources/terminology/icd_10_cm.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.icd_10_pcs CASCADE;
CREATE TABLE terminology.icd_10_pcs
(
	icd_10_pcs VARCHAR(256)   ENCODE lzo
	,valid_flag VARCHAR(256)   ENCODE lzo
	,short_description VARCHAR(256)   ENCODE lzo
	,long_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.icd_10_pcs owner to <User>;
copy terminology.icd_10_pcs
  from 's3://tuva-public-resources/terminology/icd_10_pcs.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.icd_9_cm CASCADE;
CREATE TABLE terminology.icd_9_cm
(
	icd_9_cm VARCHAR(256)   ENCODE lzo
	,long_description VARCHAR(256)   ENCODE lzo
	,short_description VARCHAR(256)   ENCODE lzo

)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.icd_9_cm owner to <User>;
copy terminology.icd_9_cm
  from 's3://tuva-public-resources/terminology/icd_9_cm.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.mdc CASCADE;
CREATE TABLE terminology.mdc
(
	mdc_code VARCHAR(256)   ENCODE lzo
	,mdc_description VARCHAR(88)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.mdc owner to <User>;
copy terminology.mdc
  from 's3://tuva-public-resources/terminology/mdc.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.medicare_dual_eligibility CASCADE;
CREATE TABLE terminology.medicare_dual_eligibility
(
	dual_status_code VARCHAR(256)   ENCODE lzo
	,dual_status_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.medicare_dual_eligibility owner to <User>;
copy terminology.medicare_dual_eligibility
  from 's3://tuva-public-resources/terminology/medicare_dual_eligibility.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.medicare_status CASCADE;
CREATE TABLE terminology.medicare_status
(
	medicare_status_code VARCHAR(256)   ENCODE lzo
	,medicare_status_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.medicare_status owner to <User>;
copy terminology.medicare_status
  from 's3://tuva-public-resources/terminology/medicare_status.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.ms_drg CASCADE;
CREATE TABLE terminology.ms_drg
(
	ms_drg_code VARCHAR(256)   ENCODE lzo
	,mdc_code VARCHAR(256)   ENCODE lzo
	,medical_surgical VARCHAR(256)   ENCODE lzo
	,ms_drg_description VARCHAR(256)   ENCODE lzo
	,deprecated INTEGER   ENCODE az64
	,deprecated_date DATE   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.ms_drg owner to <User>;
copy terminology.ms_drg
  from 's3://tuva-public-resources/terminology/ms_drg.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.other_provider_taxonomy CASCADE;
CREATE TABLE terminology.other_provider_taxonomy
(
	npi VARCHAR(35)   ENCODE lzo
	,taxonomy_code VARCHAR(35)   ENCODE lzo
	,medicare_specialty_code VARCHAR(173)   ENCODE lzo
	,description VARCHAR(101)   ENCODE lzo
	,primary_flag INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.other_provider_taxonomy owner to <User>;
copy terminology.other_provider_taxonomy
  from 's3://tuva-public-resources/terminology/other_provider_taxonomy.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.payer_type CASCADE;
CREATE TABLE terminology.payer_type
(
	payer_type VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.payer_type owner to <User>;
copy terminology.payer_type
  from 's3://tuva-public-resources/terminology/payer_type.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.place_of_service CASCADE;
CREATE TABLE terminology.place_of_service
(
	place_of_service_code VARCHAR(256)   ENCODE lzo
	,place_of_service_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.place_of_service owner to <User>;
copy terminology.place_of_service
  from 's3://tuva-public-resources/terminology/place_of_service.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.present_on_admission CASCADE;
CREATE TABLE terminology.present_on_admission
(
	present_on_admit_code VARCHAR(256)   ENCODE lzo
	,present_on_admit_description VARCHAR(230)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.present_on_admission owner to <User>;
copy terminology.present_on_admission
  from 's3://tuva-public-resources/terminology/present_on_admission.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology."provider" CASCADE;
CREATE TABLE terminology."provider"
(
	npi VARCHAR(35)   ENCODE lzo
	,entity_type_code VARCHAR(26)   ENCODE lzo
	,entity_type_description VARCHAR(37)   ENCODE lzo
	,primary_taxonomy_code VARCHAR(35)   ENCODE lzo
	,primary_specialty_description VARCHAR(173)   ENCODE lzo
	,provider_name VARCHAR(95)   ENCODE lzo
	,parent_organization_name VARCHAR(95)   ENCODE lzo
	,practice_address_line_1 VARCHAR(80)   ENCODE lzo
	,practice_address_line_2 VARCHAR(80)   ENCODE lzo
	,practice_city VARCHAR(65)   ENCODE lzo
	,practice_state VARCHAR(65)   ENCODE lzo
	,practice_zip_code VARCHAR(42)   ENCODE lzo
	,last_updated DATE   ENCODE az64
	,deactivation_date DATE   ENCODE az64
	,deactivation_flag VARCHAR(80)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology."provider" owner to <User>;
copy terminology.provider
  from 's3://tuva-public-resources/terminology/provider.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.race CASCADE;
CREATE TABLE terminology.race
(
	code VARCHAR(6)   ENCODE lzo
	,description VARCHAR(41)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.race owner to <User>;
copy terminology.race
  from 's3://tuva-public-resources/terminology/race.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.revenue_center CASCADE;
CREATE TABLE terminology.revenue_center
(
	revenue_center_code VARCHAR(256)   ENCODE lzo
	,revenue_center_description VARCHAR(66)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.revenue_center owner to <User>;
copy terminology.revenue_center
  from 's3://tuva-public-resources/terminology/revenue_center.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS terminology.ssa_fips_state CASCADE;
CREATE TABLE terminology.ssa_fips_state
(
	ssa_fips_state_code VARCHAR(256)   ENCODE lzo
	,ssa_fips_state_name VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE terminology.ssa_fips_state owner to <User>;
copy terminology.ssa_fips_state
  from 's3://tuva-public-resources/terminology/ssa_fips_state.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';




create schema if not exists value_sets;
-- with owner <user>;



DROP TABLE IF EXISTS value_sets.acute_diagnosis_ccs CASCADE;
CREATE TABLE value_sets.acute_diagnosis_ccs
(
	ccs_diagnosis_category VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.acute_diagnosis_ccs owner to <User>;
copy value_sets.acute_diagnosis_ccs
  from 's3://tuva-public-resources/value-sets/acute_diagnosis_ccs.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.acute_diagnosis_icd_10_cm CASCADE;
CREATE TABLE value_sets.acute_diagnosis_icd_10_cm
(
	icd_10_cm VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.acute_diagnosis_icd_10_cm owner to <User>;
copy value_sets.acute_diagnosis_icd_10_cm
  from 's3://tuva-public-resources/value-sets/acute_diagnosis_icd_10_cm.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.always_planned_ccs_diagnosis_category CASCADE;
CREATE TABLE value_sets.always_planned_ccs_diagnosis_category
(
	ccs_diagnosis_category VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.always_planned_ccs_diagnosis_category owner to <User>;
copy value_sets.always_planned_ccs_diagnosis_category
  from 's3://tuva-public-resources/value-sets/always_planned_ccs_diagnosis_category.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.always_planned_ccs_procedure_category CASCADE;
CREATE TABLE value_sets.always_planned_ccs_procedure_category
(
	ccs_procedure_category VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.always_planned_ccs_procedure_category owner to <User>;
copy value_sets.always_planned_ccs_procedure_category
  from 's3://tuva-public-resources/value-sets/always_planned_ccs_procedure_category.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.cms_chronic_conditions_hierarchy CASCADE;
CREATE TABLE value_sets.cms_chronic_conditions_hierarchy
(
	condition_id INTEGER   ENCODE az64
	,condition VARCHAR(81)   ENCODE lzo
	,condition_column_name VARCHAR(79)   ENCODE lzo
	,chronic_condition_type VARCHAR(49)   ENCODE lzo
	,condition_category VARCHAR(25)   ENCODE lzo
	,additional_logic VARCHAR(248)   ENCODE lzo
	,claims_qualification VARCHAR(295)   ENCODE lzo
	,inclusion_type VARCHAR(7)   ENCODE lzo
	,code_system VARCHAR(10)   ENCODE lzo
	,code VARCHAR(11)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.cms_chronic_conditions_hierarchy owner to <User>;
copy value_sets.cms_chronic_conditions_hierarchy
  from 's3://tuva-public-resources/value-sets/cms_chronic_conditions_hierarchy.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.exclusion_ccs_diagnosis_category CASCADE;
CREATE TABLE value_sets.exclusion_ccs_diagnosis_category
(
	ccs_diagnosis_category VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,exclusion_category VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.exclusion_ccs_diagnosis_category owner to <User>;
copy value_sets.exclusion_ccs_diagnosis_category
  from 's3://tuva-public-resources/value-sets/exclusion_ccs_diagnosis_category.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.icd_10_cm_to_ccs CASCADE;
CREATE TABLE value_sets.icd_10_cm_to_ccs
(
	icd_10_cm VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,ccs_diagnosis_category VARCHAR(256)   ENCODE lzo
	,ccs_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.icd_10_cm_to_ccs owner to <User>;
copy value_sets.icd_10_cm_to_ccs
  from 's3://tuva-public-resources/value-sets/icd_10_cm_to_ccs.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.icd_10_pcs_to_ccs CASCADE;
CREATE TABLE value_sets.icd_10_pcs_to_ccs
(
	icd_10_pcs VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,ccs_procedure_category VARCHAR(256)   ENCODE lzo
	,ccs_description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.icd_10_pcs_to_ccs owner to <User>;
copy value_sets.icd_10_pcs_to_ccs
  from 's3://tuva-public-resources/value-sets/icd_10_pcs_to_ccs.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.potentially_planned_ccs_procedure_category CASCADE;
CREATE TABLE value_sets.potentially_planned_ccs_procedure_category
(
	ccs_procedure_category VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.potentially_planned_ccs_procedure_category owner to <User>;
copy value_sets.potentially_planned_ccs_procedure_category
  from 's3://tuva-public-resources/value-sets/potentially_planned_ccs_procedure_category.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.potentially_planned_icd_10_pcs CASCADE;
CREATE TABLE value_sets.potentially_planned_icd_10_pcs
(
	icd_10_pcs VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.potentially_planned_icd_10_pcs owner to <User>;
copy value_sets.potentially_planned_icd_10_pcs
  from 's3://tuva-public-resources/value-sets/potentially_planned_icd_10_pcs.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.service_category CASCADE;
CREATE TABLE value_sets.service_category
(
	service_category_1 VARCHAR(256)   ENCODE lzo
	,service_category_2 VARCHAR(256)   ENCODE lzo
	,claim_type VARCHAR(256)   ENCODE lzo
	,hcpcs_code VARCHAR(256)   ENCODE lzo
	,bill_type_code_first_2_digits VARCHAR(256)   ENCODE lzo
	,revenue_center_code VARCHAR(256)   ENCODE lzo
	,valid_drg_flag VARCHAR(256)   ENCODE lzo
	,place_of_service_code VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.service_category owner to <User>;
copy value_sets.service_category
  from 's3://tuva-public-resources/value-sets/service_category.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.specialty_cohort CASCADE;
CREATE TABLE value_sets.specialty_cohort
(
	ccs VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,specialty_cohort VARCHAR(256)   ENCODE lzo
	,procedure_or_diagnosis VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.specialty_cohort owner to <User>;
copy value_sets.specialty_cohort
  from 's3://tuva-public-resources/value-sets/specialty_cohort.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.surgery_gynecology_cohort CASCADE;
CREATE TABLE value_sets.surgery_gynecology_cohort
(
	icd_10_pcs VARCHAR(256)   ENCODE lzo
	,description VARCHAR(256)   ENCODE lzo
	,ccs_code_and_description VARCHAR(256)   ENCODE lzo
	,specialty_cohort VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.surgery_gynecology_cohort owner to <User>;
copy value_sets.surgery_gynecology_cohort
  from 's3://tuva-public-resources/value-sets/surgery_gynecology_cohort.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';



DROP TABLE IF EXISTS value_sets.tuva_chronic_conditions_hierarchy CASCADE;
CREATE TABLE value_sets.tuva_chronic_conditions_hierarchy
(
	condition_family VARCHAR(26)   ENCODE lzo
	,condition VARCHAR(47)   ENCODE lzo
	,icd_10_cm_code VARCHAR(7)   ENCODE lzo
	,icd_10_cm_description VARCHAR(225)   ENCODE lzo
	,condition_column_name VARCHAR(40)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE value_sets.tuva_chronic_conditions_hierarchy owner to <User>;
copy value_sets.tuva_chronic_conditions_hierarchy
  from 's3://tuva-public-resources/value-sets/tuva_chronic_conditions_hierarchy.csv'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  gzip
  region 'us-east-1';