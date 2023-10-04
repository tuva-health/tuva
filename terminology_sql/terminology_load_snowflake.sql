create schema if not exists TERMINOLOGY;
Use schema TERMINOLOGY;


create or replace TABLE TERMINOLOGY.ADMIT_SOURCE (
	ADMIT_SOURCE_CODE VARCHAR,
	ADMIT_SOURCE_DESCRIPTION VARCHAR,
	NEWBORN_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ADMIT_SOURCE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/admit_source\.csv.*';


create or replace TABLE TERMINOLOGY.ADMIT_TYPE (
	ADMIT_TYPE_CODE VARCHAR,
	ADMIT_TYPE_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ADMIT_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/admit_type\.csv.*';


create or replace TABLE TERMINOLOGY.ANSI_FIPS_STATE (
	ANSI_FIPS_STATE_CODE VARCHAR,
	ANSI_FIPS_STATE_ABBREVIATION VARCHAR,
	ANSI_FIPS_STATE_NAME VARCHAR
);
copy into TERMINOLOGY.ANSI_FIPS_STATE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/ansi_fips_state\.csv.*';


create or replace TABLE TERMINOLOGY.APR_DRG (
	APR_DRG_CODE VARCHAR,
	SEVERITY VARCHAR,
	APR_DRG_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.APR_DRG
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/apr_drg\.csv.*';


create or replace TABLE TERMINOLOGY.BILL_TYPE (
	BILL_TYPE_CODE VARCHAR,
	BILL_TYPE_DESCRIPTION VARCHAR,
	DEPRECATED NUMBER(38,0),
	DEPRECATED_DATE DATE
);
copy into TERMINOLOGY.BILL_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/bill_type\.csv.*';


create or replace TABLE TERMINOLOGY.CALENDAR (
	FULL_DATE DATE NOT NULL,
	YEAR NUMBER(38,0) NOT NULL,
	MONTH NUMBER(38,0) NOT NULL,
	DAY NUMBER(38,0) NOT NULL,
	MONTH_NAME VARCHAR(3) NOT NULL,
	DAY_OF_WEEK_NUMBER NUMBER(38,0) NOT NULL,
	DAY_OF_WEEK_NAME VARCHAR(9) NOT NULL,
	WEEK_OF_YEAR NUMBER(38,0) NOT NULL,
	DAY_OF_YEAR NUMBER(38,0) NOT NULL,
	YEAR_MONTH VARCHAR(7) NOT NULL,
	FIRST_DAY_OF_MONTH DATE NOT NULL,
	LAST_DAY_OF_MONTH DATE NOT NULL
);
copy into TERMINOLOGY.CALENDAR
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/calendar\.csv.*';


create or replace TABLE TERMINOLOGY.CLAIM_TYPE (
	CLAIM_TYPE VARCHAR
);
copy into TERMINOLOGY.CLAIM_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/claim_type\.csv.*';


create or replace TABLE TERMINOLOGY.CODE_TYPE (
	CODE_TYPE VARCHAR
);
copy into TERMINOLOGY.CODE_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/code_type\.csv.*';


create or replace TABLE TERMINOLOGY.DISCHARGE_DISPOSITION (
	DISCHARGE_DISPOSITION_CODE VARCHAR,
	DISCHARGE_DISPOSITION_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.DISCHARGE_DISPOSITION
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/discharge_disposition\.csv.*';


create or replace TABLE TERMINOLOGY.ENCOUNTER_TYPE (
	ENCOUNTER_TYPE VARCHAR
);
copy into TERMINOLOGY.ENCOUNTER_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/encounter_type\.csv.*';


create or replace TABLE TERMINOLOGY.ETHNICITY (
	CODE VARCHAR,
	DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ETHNICITY
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/ethnicity\.csv.*';


create or replace TABLE TERMINOLOGY.FIPS_COUNTY (
	FIPS_CODE VARCHAR,
	COUNTY VARCHAR,
	STATE VARCHAR
);
copy into TERMINOLOGY.FIPS_COUNTY
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/fips_county\.csv.*';


create or replace TABLE TERMINOLOGY.GENDER (
	GENDER VARCHAR
);
copy into TERMINOLOGY.GENDER
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/gender\.csv.*';


create or replace TABLE TERMINOLOGY.HCPCS_LEVEL_2 (
	HCPCS VARCHAR,
	SEQNUM VARCHAR,
	RECID VARCHAR,
	LONG_DESCRIPTION VARCHAR(2000),
	SHORT_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.HCPCS_LEVEL_2
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/hcpcs_level_2\.csv.*';


create or replace TABLE TERMINOLOGY.ICD_10_CM (
	ICD_10_CM VARCHAR,
	VALID_FLAG VARCHAR,
	SHORT_DESCRIPTION VARCHAR,
	LONG_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ICD_10_CM
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/icd_10_cm\.csv.*';


create or replace TABLE TERMINOLOGY.ICD_10_PCS (
	ICD_10_PCS VARCHAR,
	VALID_FLAG VARCHAR,
	SHORT_DESCRIPTION VARCHAR,
	LONG_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ICD_10_PCS
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/icd_10_pcs\.csv.*';


create or replace TABLE TERMINOLOGY.ICD_9_CM (
	ICD_9_CM VARCHAR,
	LONG_DESCRIPTION VARCHAR,
	SHORT_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.ICD_9_CM
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/icd_9_cm\.csv.*';


create or replace TABLE TERMINOLOGY.MDC (
	MDC_CODE VARCHAR,
	MDC_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.MDC
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/mdc\.csv.*';


create or replace TABLE TERMINOLOGY.MEDICARE_DUAL_ELIGIBILITY (
	DUAL_STATUS_CODE VARCHAR,
	DUAL_STATUS_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.MEDICARE_DUAL_ELIGIBILITY
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/medicare_dual_eligibility\.csv.*';


create or replace TABLE TERMINOLOGY.MEDICARE_STATUS (
	MEDICARE_STATUS_CODE VARCHAR,
	MEDICARE_STATUS_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.MEDICARE_STATUS
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/medicare_status\.csv.*';


create or replace TABLE TERMINOLOGY.MS_DRG (
	MS_DRG_CODE VARCHAR,
	MDC_CODE VARCHAR,
	MEDICAL_SURGICAL VARCHAR,
	MS_DRG_DESCRIPTION VARCHAR,
	DEPRECATED NUMBER(38,0),
	DEPRECATED_DATE DATE
);
copy into TERMINOLOGY.MS_DRG
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/ms_drg\.csv.*';


create or replace TABLE TERMINOLOGY.OTHER_PROVIDER_TAXONOMY (
	NPI VARCHAR(35),
	TAXONOMY_CODE VARCHAR(35),
	MEDICARE_SPECIALTY_CODE VARCHAR(173),
	DESCRIPTION VARCHAR(101),
	PRIMARY_FLAG NUMBER(38,0)
);
copy into TERMINOLOGY.OTHER_PROVIDER_TAXONOMY
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/other_provider_taxonomy\.csv.*';


create or replace TABLE TERMINOLOGY.PAYER_TYPE (
	PAYER_TYPE VARCHAR
);
copy into TERMINOLOGY.PAYER_TYPE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/payer_type\.csv.*';


create or replace TABLE TERMINOLOGY.PLACE_OF_SERVICE (
	PLACE_OF_SERVICE_CODE VARCHAR,
	PLACE_OF_SERVICE_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.PLACE_OF_SERVICE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/place_of_service\.csv.*';


create or replace TABLE TERMINOLOGY.PRESENT_ON_ADMISSION (
	PRESENT_ON_ADMIT_CODE VARCHAR,
	PRESENT_ON_ADMIT_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.PRESENT_ON_ADMISSION
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/present_on_admission\.csv.*';


create or replace TABLE TERMINOLOGY.PROVIDER (
	NPI VARCHAR(35),
	ENTITY_TYPE_CODE VARCHAR(26),
	ENTITY_TYPE_DESCRIPTION VARCHAR(37),
	PRIMARY_TAXONOMY_CODE VARCHAR(35),
	PRIMARY_SPECIALTY_DESCRIPTION VARCHAR(173),
	PROVIDER_NAME VARCHAR(95),
	PARENT_ORGANIZATION_NAME VARCHAR(95),
	PRACTICE_ADDRESS_LINE_1 VARCHAR(80),
	PRACTICE_ADDRESS_LINE_2 VARCHAR(80),
	PRACTICE_CITY VARCHAR(65),
	PRACTICE_STATE VARCHAR(65),
	PRACTICE_ZIP_CODE VARCHAR(42),
	LAST_UPDATED DATE,
	DEACTIVATION_DATE DATE,
	DEACTIVATION_FLAG VARCHAR(80)
);
copy into TERMINOLOGY.PROVIDER
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/provider\.csv.*';


create or replace TABLE TERMINOLOGY.RACE (
	CODE VARCHAR,
	DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.RACE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/race\.csv.*';


create or replace TABLE TERMINOLOGY.REVENUE_CENTER (
	REVENUE_CENTER_CODE VARCHAR,
	REVENUE_CENTER_DESCRIPTION VARCHAR
);
copy into TERMINOLOGY.REVENUE_CENTER
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/revenue_center\.csv.*';


create or replace TABLE TERMINOLOGY.SSA_FIPS_STATE (
	SSA_FIPS_STATE_CODE VARCHAR,
	SSA_FIPS_STATE_NAME VARCHAR
);
copy into TERMINOLOGY.SSA_FIPS_STATE
    from s3://tuva-public-resources/terminology/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/ssa_fips_state\.csv.*';



create schema if not exists VALUE_SETS;
Use schema VALUE_SETS;


create or replace TABLE VALUE_SETS.ACUTE_DIAGNOSIS_CCS (
	CCS_DIAGNOSIS_CATEGORY VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ACUTE_DIAGNOSIS_CCS
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/acute_diagnosis_ccs\.csv.*';


create or replace TABLE VALUE_SETS.ACUTE_DIAGNOSIS_ICD_10_CM (
	ICD_10_CM VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ACUTE_DIAGNOSIS_ICD_10_CM
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/acute_diagnosis_icd_10_cm\.csv.*';


create or replace TABLE VALUE_SETS.ALWAYS_PLANNED_CCS_DIAGNOSIS_CATEGORY (
	CCS_DIAGNOSIS_CATEGORY VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ALWAYS_PLANNED_CCS_DIAGNOSIS_CATEGORY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/always_planned_ccs_diagnosis_category\.csv.*';


create or replace TABLE VALUE_SETS.ALWAYS_PLANNED_CCS_PROCEDURE_CATEGORY (
	CCS_PROCEDURE_CATEGORY VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ALWAYS_PLANNED_CCS_PROCEDURE_CATEGORY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/always_planned_ccs_procedure_category\.csv.*';


create or replace TABLE VALUE_SETS.CMS_CHRONIC_CONDITIONS_HIERARCHY (
	CONDITION_ID NUMBER(38,0),
	CONDITION VARCHAR,
	CONDITION_COLUMN_NAME VARCHAR,
	CHRONIC_CONDITION_TYPE VARCHAR,
	CONDITION_CATEGORY VARCHAR,
	ADDITIONAL_LOGIC VARCHAR,
	CLAIMS_QUALIFICATION VARCHAR,
	INCLUSION_TYPE VARCHAR,
	CODE_SYSTEM VARCHAR,
	CODE VARCHAR
);
copy into VALUE_SETS.CMS_CHRONIC_CONDITIONS_HIERARCHY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/cms_chronic_conditions_hierarchy\.csv.*';


create or replace TABLE VALUE_SETS.EXCLUSION_CCS_DIAGNOSIS_CATEGORY (
	CCS_DIAGNOSIS_CATEGORY VARCHAR,
	DESCRIPTION VARCHAR,
	EXCLUSION_CATEGORY VARCHAR
);
copy into VALUE_SETS.EXCLUSION_CCS_DIAGNOSIS_CATEGORY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/exclusion_ccs_diagnosis_category\.csv.*';


create or replace TABLE VALUE_SETS.ICD_10_CM_TO_CCS (
	ICD_10_CM VARCHAR,
	DESCRIPTION VARCHAR,
	CCS_DIAGNOSIS_CATEGORY VARCHAR,
	CCS_DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ICD_10_CM_TO_CCS
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/icd_10_cm_to_ccs\.csv.*';


create or replace TABLE VALUE_SETS.ICD_10_PCS_TO_CCS (
	ICD_10_PCS VARCHAR,
	DESCRIPTION VARCHAR,
	CCS_PROCEDURE_CATEGORY VARCHAR,
	CCS_DESCRIPTION VARCHAR
);
copy into VALUE_SETS.ICD_10_PCS_TO_CCS
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/icd_10_pcs_to_ccs\.csv.*';


create or replace TABLE VALUE_SETS.POTENTIALLY_PLANNED_CCS_PROCEDURE_CATEGORY (
	CCS_PROCEDURE_CATEGORY VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.POTENTIALLY_PLANNED_CCS_PROCEDURE_CATEGORY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/potentially_planned_ccs_procedure_category\.csv.*';


create or replace TABLE VALUE_SETS.POTENTIALLY_PLANNED_ICD_10_PCS (
	ICD_10_PCS VARCHAR,
	DESCRIPTION VARCHAR
);
copy into VALUE_SETS.POTENTIALLY_PLANNED_ICD_10_PCS
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/potentially_planned_icd_10_pcs\.csv.*';


create or replace TABLE VALUE_SETS.SERVICE_CATEGORY (
	SERVICE_CATEGORY_1 VARCHAR,
	SERVICE_CATEGORY_2 VARCHAR,
	CLAIM_TYPE VARCHAR,
	HCPCS_CODE VARCHAR,
	BILL_TYPE_CODE_FIRST_2_DIGITS VARCHAR,
	REVENUE_CENTER_CODE VARCHAR,
	VALID_DRG_FLAG NUMBER(38,0),
	PLACE_OF_SERVICE_CODE VARCHAR
);
copy into VALUE_SETS.SERVICE_CATEGORY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/service_category\.csv.*';


create or replace TABLE VALUE_SETS.SPECIALTY_COHORT (
	CCS VARCHAR,
	DESCRIPTION VARCHAR,
	SPECIALTY_COHORT VARCHAR,
	PROCEDURE_OR_DIAGNOSIS VARCHAR
);
copy into VALUE_SETS.SPECIALTY_COHORT
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/specialty_cohort\.csv.*';


create or replace TABLE VALUE_SETS.SURGERY_GYNECOLOGY_COHORT (
	ICD_10_PCS VARCHAR,
	DESCRIPTION VARCHAR,
	CCS_CODE_AND_DESCRIPTION VARCHAR,
	SPECIALTY_COHORT VARCHAR
);
copy into VALUE_SETS.SURGERY_GYNECOLOGY_COHORT
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/surgery_gynecology_cohort\.csv.*';


create or replace TABLE VALUE_SETS.TUVA_CHRONIC_CONDITIONS_HIERARCHY (
	CONDITION_FAMILY VARCHAR,
	CONDITION VARCHAR,
	ICD_10_CM_CODE VARCHAR,
	ICD_10_CM_DESCRIPTION VARCHAR,
	CONDITION_COLUMN_NAME VARCHAR
);
copy into VALUE_SETS.TUVA_CHRONIC_CONDITIONS_HIERARCHY
    from s3://tuva-public-resources/value-sets/
    file_format = (type = CSV
    compression = 'GZIP'
    field_optionally_enclosed_by = '"'
)
pattern = '.*/tuva_chronic_conditions_hierarchy\.csv.*';