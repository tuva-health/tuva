CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.admit_source (
	admit_source_code string,
	admit_source_description string,
	newborn_description string
);
LOAD DATA INTO terminology.admit_source (
	admit_source_code string,
	admit_source_description string,
	newborn_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/admit_source.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.admit_type (
	admit_type_code string,
	admit_type_description string
);
LOAD DATA INTO terminology.admit_type (
	admit_type_code string,
	admit_type_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/admit_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.apr_drg (
	apr_drg_code string,
	medical_surgical string,
	mdc_code string,
	apr_drg_description string
);
LOAD DATA INTO terminology.apr_drg (
	apr_drg_code string,
	medical_surgical string,
	mdc_code string,
	apr_drg_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/apr_drg.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.bill_type (
	bill_type_code string,
	bill_type_description string,
	deprecated integer,
	deprecated_date date
);
LOAD DATA INTO terminology.bill_type (
	bill_type_code string,
	bill_type_description string,
	deprecated integer,
	deprecated_date date
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/bill_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.claim_type (
	claim_type string
);
LOAD DATA INTO terminology.claim_type (
	claim_type string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/claim_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.discharge_disposition (
	discharge_disposition_code string,
	discharge_disposition_description string
);
LOAD DATA INTO terminology.discharge_disposition (
	discharge_disposition_code string,
	discharge_disposition_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/discharge_disposition.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.encounter_type (
	encounter_type string
);
LOAD DATA INTO terminology.encounter_type (
	encounter_type string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/encounter_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.ethnicity (
	code string,
	description string
);
LOAD DATA INTO terminology.ethnicity (
	code string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/ethnicity.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.gender (
	gender string
);
LOAD DATA INTO terminology.gender (
	gender string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/gender.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.hcpcs_level_2 (
	hcpcs string,
	seqnum string,
	recid string,
	long_description string,
	short_description string
);
LOAD DATA INTO terminology.hcpcs_level_2 (
	hcpcs string,
	seqnum string,
	recid string,
	long_description string,
	short_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/hcpcs_level_2.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.icd_9_cm (
	icd_9_cm string,
	long_description string,
	short_description string
);
LOAD DATA INTO terminology.icd_9_cm (
	icd_9_cm string,
	long_description string,
	short_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/icd_9_cm.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.icd_9_pcs (
	icd_9_pcs string,
	long_description string,
	short_description string
);
LOAD DATA INTO terminology.icd_9_pcs (
	icd_9_pcs string,
	long_description string,
	short_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/icd_9_pcs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.icd_10_cm (
	icd_10_cm string,
	header_flag string,
	short_description string,
	long_description string
);
LOAD DATA INTO terminology.icd_10_cm (
	icd_10_cm string,
	header_flag string,
	short_description string,
	long_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/icd_10_cm.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.icd_10_pcs (
	icd_10_pcs string,
	description string
);
LOAD DATA INTO terminology.icd_10_pcs (
	icd_10_pcs string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/icd_10_pcs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.loinc (
	loinc string,
	short_name string,
	long_common_name string,
	component string,
	property string,
	time_aspect string,
	system string,
	scale_type string,
	method_type string,
	class_code string,
	class_description string,
	class_type_code string,
	class_type_description string,
	external_copyright_notice string,
	status string,
	version_first_released string,
	version_last_changed string
);
LOAD DATA INTO terminology.loinc (
	loinc string,
	short_name string,
	long_common_name string,
	component string,
	property string,
	time_aspect string,
	system string,
	scale_type string,
	method_type string,
	class_code string,
	class_description string,
	class_type_code string,
	class_type_description string,
	external_copyright_notice string,
	status string,
	version_first_released string,
	version_last_changed string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/loinc.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.loinc_deprecated_mapping (
	loinc string,
	map_to string,
	comment string,
	final_map_to string,
	all_comments string,
	depth string
);
LOAD DATA INTO terminology.loinc_deprecated_mapping (
	loinc string,
	map_to string,
	comment string,
	final_map_to string,
	all_comments string,
	depth string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/loinc_deprecated_mapping.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.mdc (
	mdc_code string,
	mdc_description string
);
LOAD DATA INTO terminology.mdc (
	mdc_code string,
	mdc_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/mdc.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.medicare_dual_eligibility (
	dual_status_code string,
	dual_status_description string
);
LOAD DATA INTO terminology.medicare_dual_eligibility (
	dual_status_code string,
	dual_status_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/medicare_dual_eligibility.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.medicare_orec (
	original_reason_entitlement_code string,
	original_reason_entitlement_description string
);
LOAD DATA INTO terminology.medicare_orec (
	original_reason_entitlement_code string,
	original_reason_entitlement_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/medicare_orec.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.medicare_status (
	medicare_status_code string,
	medicare_status_description string
);
LOAD DATA INTO terminology.medicare_status (
	medicare_status_code string,
	medicare_status_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/medicare_status.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.ms_drg (
	ms_drg_code string,
	mdc_code string,
	medical_surgical string,
	ms_drg_description string,
	deprecated integer,
	deprecated_date date
);
LOAD DATA INTO terminology.ms_drg (
	ms_drg_code string,
	mdc_code string,
	medical_surgical string,
	ms_drg_description string,
	deprecated integer,
	deprecated_date date
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/ms_drg.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.ndc (
	ndc string,
	rxcui string,
	rxnorm_description string,
	fda_description string
);
LOAD DATA INTO terminology.ndc (
	ndc string,
	rxcui string,
	rxnorm_description string,
	fda_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/ndc.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.other_provider_taxonomy (
	npi string,
	taxonomy_code string,
	medicare_specialty_code string,
	description string,
	primary_flag integer
);
LOAD DATA INTO terminology.other_provider_taxonomy (
	npi string,
	taxonomy_code string,
	medicare_specialty_code string,
	description string,
	primary_flag integer
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_provider_data/0.10.1/other_provider_taxonomy.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.payer_type (
	payer_type string
);
LOAD DATA INTO terminology.payer_type (
	payer_type string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/payer_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.place_of_service (
	place_of_service_code string,
	place_of_service_description string
);
LOAD DATA INTO terminology.place_of_service (
	place_of_service_code string,
	place_of_service_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/place_of_service.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.present_on_admission (
	present_on_admit_code string,
	present_on_admit_description string
);
LOAD DATA INTO terminology.present_on_admission (
	present_on_admit_code string,
	present_on_admit_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/present_on_admission.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.provider (
	npi string,
	entity_type_code string,
	entity_type_description string,
	primary_taxonomy_code string,
	primary_specialty_description string,
	provider_first_name string,
	provider_last_name string,
	provider_organization_name string,
	parent_organization_name string,
	practice_address_line_1 string,
	practice_address_line_2 string,
	practice_city string,
	practice_state string,
	practice_zip_code string,
	last_updated date,
	deactivation_date date,
	deactivation_flag string
);
LOAD DATA INTO terminology.provider (
	npi string,
	entity_type_code string,
	entity_type_description string,
	primary_taxonomy_code string,
	primary_specialty_description string,
	provider_first_name string,
	provider_last_name string,
	provider_organization_name string,
	parent_organization_name string,
	practice_address_line_1 string,
	practice_address_line_2 string,
	practice_city string,
	practice_state string,
	practice_zip_code string,
	last_updated date,
	deactivation_date date,
	deactivation_flag string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_provider_data/0.10.1/provider.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.race (
	code string,
	description string
);
LOAD DATA INTO terminology.race (
	code string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/race.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.revenue_center (
	revenue_center_code string,
	revenue_center_description string
);
LOAD DATA INTO terminology.revenue_center (
	revenue_center_code string,
	revenue_center_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/revenue_center.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.rxnorm_to_atc (
	rxcui string,
	rxnorm_description string,
	atc_1_code string,
	atc_1_name string,
	atc_2_code string,
	atc_2_name string,
	atc_3_code string,
	atc_3_name string,
	atc_4_code string,
	atc_4_name string
);
LOAD DATA INTO terminology.rxnorm_to_atc (
	rxcui string,
	rxnorm_description string,
	atc_1_code string,
	atc_1_name string,
	atc_2_code string,
	atc_2_name string,
	atc_3_code string,
	atc_3_name string,
	atc_4_code string,
	atc_4_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/rxnorm_to_atc.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.snomed_icd_10_map (
	id string,
	effective_time string,
	active string,
	module_id string,
	ref_set_id string,
	referenced_component_id string,
	referenced_component_name string,
	map_group string,
	map_priority string,
	map_rule string,
	map_advice string,
	map_target string,
	map_target_name string,
	correlation_id string,
	map_category_id string,
	map_category_name string
);
LOAD DATA INTO terminology.snomed_icd_10_map (
	id string,
	effective_time string,
	active string,
	module_id string,
	ref_set_id string,
	referenced_component_id string,
	referenced_component_name string,
	map_group string,
	map_priority string,
	map_rule string,
	map_advice string,
	map_target string,
	map_target_name string,
	correlation_id string,
	map_category_id string,
	map_category_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/snomed_icd_10_map.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.snomed_ct (
	snomed_ct string,
	description string,
	is_active string,
	created date,
	last_updated date
);
LOAD DATA INTO terminology.snomed_ct (
	snomed_ct string,
	description string,
	is_active string,
	created date,
	last_updated date
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/snomed_ct.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.snomed_ct_transitive_closures (
	parent_snomed_code string,
	parent_description string,
	child_snomed_code string,
	child_description string
);
LOAD DATA INTO terminology.snomed_ct_transitive_closures (
	parent_snomed_code string,
	parent_description string,
	child_snomed_code string,
	child_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/snomed_ct_transitive_closures.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS terminology;
CREATE OR REPLACE TABLE terminology.rxnorm_brand_generic (
	product_rxcui string,
	product_name string,
	product_tty string,
	brand_vs_generic string,
	brand_name string,
	clinical_product_rxcui string,
	clinical_product_name string,
	clinical_product_tty string,
	ingredient_name string,
	dose_form_name string
);
LOAD DATA INTO terminology.rxnorm_brand_generic (
	product_rxcui string,
	product_name string,
	product_tty string,
	brand_vs_generic string,
	brand_name string,
	clinical_product_rxcui string,
	clinical_product_name string,
	clinical_product_tty string,
	ingredient_name string,
	dose_form_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/rxnorm_brand_generic.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS clinical_concept_library;
CREATE OR REPLACE TABLE clinical_concept_library.clinical_concepts (
	concept_id string,
	concept_name string,
	status string,
	concept_oid string,
	last_update_date date,
	last_update_note string,
	concept_type string,
	content_source string,
	external_source_detail string,
	concept_scope string,
	value_set_search_notes string,
	code string,
	code_description string,
	coding_system_id string,
	coding_system_version string
);
LOAD DATA INTO clinical_concept_library.clinical_concepts (
	concept_id string,
	concept_name string,
	status string,
	concept_oid string,
	last_update_date date,
	last_update_note string,
	concept_type string,
	content_source string,
	external_source_detail string,
	concept_scope string,
	value_set_search_notes string,
	code string,
	code_description string,
	coding_system_id string,
	coding_system_version string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/tuva_clinical_concepts.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS clinical_concept_library;
CREATE OR REPLACE TABLE clinical_concept_library.value_set_members (
	value_set_member_id string,
	concept_id string,
	status string,
	last_update_date date,
	last_update_note string,
	code string,
	code_description string,
	coding_system_id string,
	coding_system_version string,
	include_descendants string,
	comment string
);
LOAD DATA INTO clinical_concept_library.value_set_members (
	value_set_member_id string,
	concept_id string,
	status string,
	last_update_date date,
	last_update_note string,
	code string,
	code_description string,
	coding_system_id string,
	coding_system_version string,
	include_descendants string,
	comment string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/tuva_value_set_members.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS clinical_concept_library;
CREATE OR REPLACE TABLE clinical_concept_library.coding_systems (
	coding_system_id string,
	coding_system_name string,
	coding_system_uri string,
	coding_system_oid string
);
LOAD DATA INTO clinical_concept_library.coding_systems (
	coding_system_id string,
	coding_system_name string,
	coding_system_uri string,
	coding_system_oid string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/tuva_coding_systems.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS pharmacy;
CREATE OR REPLACE TABLE pharmacy.rxnorm_generic_available (
	product_tty string,
	product_rxcui string,
	product_name string,
	ndc_product_tty string,
	ndc_product_rxcui string,
	ndc_product_name string,
	ndc string,
	product_startmarketingdate date,
	package_startmarketingdate date
);
LOAD DATA INTO pharmacy.rxnorm_generic_available (
	product_tty string,
	product_rxcui string,
	product_name string,
	ndc_product_tty string,
	ndc_product_rxcui string,
	ndc_product_name string,
	ndc string,
	product_startmarketingdate date,
	package_startmarketingdate date
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/rxnorm_generic_available.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE OR REPLACE TABLE data_quality._value_set_crosswalk_field_info (
	input_layer_table_name string,
	claim_type string,
	field_name string,
	red integer,
	green integer,
	unique_values_expected_flag integer
);
LOAD DATA INTO data_quality._value_set_crosswalk_field_info (
	input_layer_table_name string,
	claim_type string,
	field_name string,
	red integer,
	green integer,
	unique_values_expected_flag integer
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/data_quality_crosswalk_field_info.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE OR REPLACE TABLE data_quality._value_set_crosswalk_field_to_mart (
	input_layer_table_name string,
	claim_type string,
	field_name string,
	mart_name string
);
LOAD DATA INTO data_quality._value_set_crosswalk_field_to_mart (
	input_layer_table_name string,
	claim_type string,
	field_name string,
	mart_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/data_quality_crosswalk_field_to_mart.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE OR REPLACE TABLE data_quality._value_set_crosswalk_mart_to_outcome_measure (
	mart_name string,
	measure_name string
);
LOAD DATA INTO data_quality._value_set_crosswalk_mart_to_outcome_measure (
	mart_name string,
	measure_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/data_quality_crosswalk_mart_to_outcome_measure.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE OR REPLACE TABLE data_quality._value_set_crosswalk_measure_reasonable_ranges (
	measure_name string,
	payer_type string,
	lower_bound float,
	upper_bound float
);
LOAD DATA INTO data_quality._value_set_crosswalk_measure_reasonable_ranges (
	measure_name string,
	payer_type string,
	lower_bound float,
	upper_bound float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/data_quality_crosswalk_measure_reasonable_ranges.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS claims_preprocessing;
CREATE OR REPLACE TABLE claims_preprocessing._value_set_service_categories (
	service_category_1 string,
	service_category_2 string
);
LOAD DATA INTO claims_preprocessing._value_set_service_categories (
	service_category_1 string,
	service_category_2 string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/service_category_service_categories.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ed_classification;
CREATE OR REPLACE TABLE ed_classification._value_set_categories (
	classification string,
	classification_name string,
	classification_order string,
	classification_column string
);
LOAD DATA INTO ed_classification._value_set_categories (
	classification string,
	classification_name string,
	classification_order string,
	classification_column string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/ed_classification_categories.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ed_classification;
CREATE OR REPLACE TABLE ed_classification._value_set_icd_10_cm_to_ccs (
	icd_10_cm string,
	description string,
	ccs_diagnosis_category string,
	ccs_description string
);
LOAD DATA INTO ed_classification._value_set_icd_10_cm_to_ccs (
	icd_10_cm string,
	description string,
	ccs_diagnosis_category string,
	ccs_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/icd_10_cm_to_ccs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ed_classification;
CREATE OR REPLACE TABLE ed_classification._value_set_johnston_icd9 (
	icd9 string,
	edcnnpa string,
	edcnpa string,
	epct string,
	noner string,
	injury string,
	psych string,
	alcohol string,
	drug string
);
LOAD DATA INTO ed_classification._value_set_johnston_icd9 (
	icd9 string,
	edcnnpa string,
	edcnpa string,
	epct string,
	noner string,
	injury string,
	psych string,
	alcohol string,
	drug string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/johnston_icd9.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ed_classification;
CREATE OR REPLACE TABLE ed_classification._value_set_johnston_icd10 (
	icd10 string,
	edcnnpa string,
	edcnpa string,
	noner string,
	epct string,
	injury string,
	psych string,
	alcohol string,
	drug string
);
LOAD DATA INTO ed_classification._value_set_johnston_icd10 (
	icd10 string,
	edcnnpa string,
	edcnpa string,
	noner string,
	epct string,
	injury string,
	psych string,
	alcohol string,
	drug string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/johnston_icd10.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_acute_diagnosis_ccs (
	ccs_diagnosis_category string,
	description string
);
LOAD DATA INTO readmissions._value_set_acute_diagnosis_ccs (
	ccs_diagnosis_category string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/acute_diagnosis_ccs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_acute_diagnosis_icd_10_cm (
	icd_10_cm string,
	description string
);
LOAD DATA INTO readmissions._value_set_acute_diagnosis_icd_10_cm (
	icd_10_cm string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/acute_diagnosis_icd_10_cm.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_always_planned_ccs_diagnosis_category (
	ccs_diagnosis_category string,
	description string
);
LOAD DATA INTO readmissions._value_set_always_planned_ccs_diagnosis_category (
	ccs_diagnosis_category string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/always_planned_ccs_diagnosis_category.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_always_planned_ccs_procedure_category (
	ccs_procedure_category string,
	description string
);
LOAD DATA INTO readmissions._value_set_always_planned_ccs_procedure_category (
	ccs_procedure_category string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/always_planned_ccs_procedure_category.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_exclusion_ccs_diagnosis_category (
	ccs_diagnosis_category string,
	description string,
	exclusion_category string
);
LOAD DATA INTO readmissions._value_set_exclusion_ccs_diagnosis_category (
	ccs_diagnosis_category string,
	description string,
	exclusion_category string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/exclusion_ccs_diagnosis_category.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_icd_10_cm_to_ccs (
	icd_10_cm string,
	description string,
	ccs_diagnosis_category string,
	ccs_description string
);
LOAD DATA INTO readmissions._value_set_icd_10_cm_to_ccs (
	icd_10_cm string,
	description string,
	ccs_diagnosis_category string,
	ccs_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/icd_10_cm_to_ccs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_icd_10_pcs_to_ccs (
	icd_10_pcs string,
	description string,
	ccs_procedure_category string,
	ccs_description string
);
LOAD DATA INTO readmissions._value_set_icd_10_pcs_to_ccs (
	icd_10_pcs string,
	description string,
	ccs_procedure_category string,
	ccs_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/icd_10_pcs_to_ccs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_potentially_planned_ccs_procedure_category (
	ccs_procedure_category string,
	description string
);
LOAD DATA INTO readmissions._value_set_potentially_planned_ccs_procedure_category (
	ccs_procedure_category string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/potentially_planned_ccs_procedure_category.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_potentially_planned_icd_10_pcs (
	icd_10_pcs string,
	description string
);
LOAD DATA INTO readmissions._value_set_potentially_planned_icd_10_pcs (
	icd_10_pcs string,
	description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/potentially_planned_icd_10_pcs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_specialty_cohort (
	ccs string,
	description string,
	specialty_cohort string,
	procedure_or_diagnosis string
);
LOAD DATA INTO readmissions._value_set_specialty_cohort (
	ccs string,
	description string,
	specialty_cohort string,
	procedure_or_diagnosis string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/specialty_cohort.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS readmissions;
CREATE OR REPLACE TABLE readmissions._value_set_surgery_gynecology_cohort (
	icd_10_pcs string,
	description string,
	ccs_code_and_description string,
	specialty_cohort string
);
LOAD DATA INTO readmissions._value_set_surgery_gynecology_cohort (
	icd_10_pcs string,
	description string,
	ccs_code_and_description string,
	specialty_cohort string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/surgery_gynecology_cohort.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_adjustment_rates (
	model_version string,
	payment_year integer,
	normalization_factor float,
	ma_coding_pattern_adjustment float
);
LOAD DATA INTO cms_hcc._value_set_adjustment_rates (
	model_version string,
	payment_year integer,
	normalization_factor float,
	ma_coding_pattern_adjustment float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_adjustment_rates.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_cpt_hcpcs (
	payment_year integer,
	hcpcs_cpt_code string,
	included_flag string
);
LOAD DATA INTO cms_hcc._value_set_cpt_hcpcs (
	payment_year integer,
	hcpcs_cpt_code string,
	included_flag string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_cpt_hcpcs.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_demographic_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	plan_segment string,
	gender string,
	age_group string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_demographic_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	plan_segment string,
	gender string,
	age_group string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_demographic_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_disabled_interaction_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	institutional_status string,
	short_name string,
	description string,
	hcc_code string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_disabled_interaction_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	institutional_status string,
	short_name string,
	description string,
	hcc_code string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_disabled_interaction_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_disease_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	hcc_code string,
	description string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_disease_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	hcc_code string,
	description string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_disease_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_disease_hierarchy (
	model_version string,
	hcc_code string,
	description string,
	hccs_to_exclude string
);
LOAD DATA INTO cms_hcc._value_set_disease_hierarchy (
	model_version string,
	hcc_code string,
	description string,
	hccs_to_exclude string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_disease_hierarchy.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_disease_interaction_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	short_name string,
	description string,
	hcc_code_1 string,
	hcc_code_2 string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_disease_interaction_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	short_name string,
	description string,
	hcc_code_1 string,
	hcc_code_2 string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_disease_interaction_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_enrollment_interaction_factors (
	model_version string,
	factor_type string,
	gender string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	institutional_status string,
	description string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_enrollment_interaction_factors (
	model_version string,
	factor_type string,
	gender string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	institutional_status string,
	description string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_enrollment_interaction_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_icd_10_cm_mappings (
	payment_year integer,
	diagnosis_code string,
	cms_hcc_v24 string,
	cms_hcc_v24_flag string,
	cms_hcc_v28 string,
	cms_hcc_v28_flag string
);
LOAD DATA INTO cms_hcc._value_set_icd_10_cm_mappings (
	payment_year integer,
	diagnosis_code string,
	cms_hcc_v24 string,
	cms_hcc_v24_flag string,
	cms_hcc_v28 string,
	cms_hcc_v28_flag string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_icd_10_cm_mappings.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS cms_hcc;
CREATE OR REPLACE TABLE cms_hcc._value_set_payment_hcc_count_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	payment_hcc_count string,
	description string,
	coefficient float
);
LOAD DATA INTO cms_hcc._value_set_payment_hcc_count_factors (
	model_version string,
	factor_type string,
	enrollment_status string,
	medicaid_status string,
	dual_status string,
	orec string,
	institutional_status string,
	payment_hcc_count string,
	description string,
	coefficient float
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_hcc_payment_hcc_count_factors.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS quality_measures;
CREATE OR REPLACE TABLE quality_measures._value_set_concepts (
	concept_name string,
	concept_oid string,
	measure_id string,
	measure_name string
);
LOAD DATA INTO quality_measures._value_set_concepts (
	concept_name string,
	concept_oid string,
	measure_id string,
	measure_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/quality_measures_concepts.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS quality_measures;
CREATE OR REPLACE TABLE quality_measures._value_set_measures (
	id string,
	name string,
	description string,
	version string,
	steward string
);
LOAD DATA INTO quality_measures._value_set_measures (
	id string,
	name string,
	description string,
	version string,
	steward string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/quality_measures_measures.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS quality_measures;
CREATE OR REPLACE TABLE quality_measures._value_set_codes (
	concept_name string,
	concept_oid string,
	code string,
	code_system string
);
LOAD DATA INTO quality_measures._value_set_codes (
	concept_name string,
	concept_oid string,
	code string,
	code_system string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/quality_measures_value_set_codes.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ccsr;
CREATE OR REPLACE TABLE ccsr._value_set_dxccsr_v2023_1_body_systems (
	body_system string,
	ccsr_parent_category string,
	parent_category_description string
);
LOAD DATA INTO ccsr._value_set_dxccsr_v2023_1_body_systems (
	body_system string,
	ccsr_parent_category string,
	parent_category_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/dxccsr_v2023_1_body_systems.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ccsr;
CREATE OR REPLACE TABLE ccsr._value_set_dxccsr_v2023_1_cleaned_map (
	icd_10_cm_code string,
	icd_10_cm_code_description string,
	default_ccsr_category_ip string,
	default_ccsr_category_description_ip string,
	default_ccsr_category_op string,
	default_ccsr_category_description_op string,
	ccsr_category_1 string,
	ccsr_category_1_description string,
	ccsr_category_2 string,
	ccsr_category_2_description string,
	ccsr_category_3 string,
	ccsr_category_3_description string,
	ccsr_category_4 string,
	ccsr_category_4_description string,
	ccsr_category_5 string,
	ccsr_category_5_description string,
	ccsr_category_6 string,
	ccsr_category_6_description string
);
LOAD DATA INTO ccsr._value_set_dxccsr_v2023_1_cleaned_map (
	icd_10_cm_code string,
	icd_10_cm_code_description string,
	default_ccsr_category_ip string,
	default_ccsr_category_description_ip string,
	default_ccsr_category_op string,
	default_ccsr_category_description_op string,
	ccsr_category_1 string,
	ccsr_category_1_description string,
	ccsr_category_2 string,
	ccsr_category_2_description string,
	ccsr_category_3 string,
	ccsr_category_3_description string,
	ccsr_category_4 string,
	ccsr_category_4_description string,
	ccsr_category_5 string,
	ccsr_category_5_description string,
	ccsr_category_6 string,
	ccsr_category_6_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/dxccsr_v2023_1_cleaned_map.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS ccsr;
CREATE OR REPLACE TABLE ccsr._value_set_prccsr_v2023_1_cleaned_map (
	icd_10_pcs string,
	icd_10_pcs_description string,
	prccsr string,
	prccsr_description string,
	clinical_domain string
);
LOAD DATA INTO ccsr._value_set_prccsr_v2023_1_cleaned_map (
	icd_10_pcs string,
	icd_10_pcs_description string,
	prccsr string,
	prccsr_description string,
	clinical_domain string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/prccsr_v2023_1_cleaned_map.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS hcc_suspecting;
CREATE OR REPLACE TABLE hcc_suspecting._value_set_clinical_concepts (
	concept_name string,
	concept_oid string,
	code string,
	code_system string
);
LOAD DATA INTO hcc_suspecting._value_set_clinical_concepts (
	concept_name string,
	concept_oid string,
	code string,
	code_system string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/hcc_suspecting_clinical_concepts.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS hcc_suspecting;
CREATE OR REPLACE TABLE hcc_suspecting._value_set_hcc_descriptions (
	hcc_code string,
	hcc_description string
);
LOAD DATA INTO hcc_suspecting._value_set_hcc_descriptions (
	hcc_code string,
	hcc_description string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/hcc_suspecting_descriptions.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS hcc_suspecting;
CREATE OR REPLACE TABLE hcc_suspecting._value_set_icd_10_cm_mappings (
	diagnosis_code string,
	cms_hcc_esrd_v21 string,
	cms_hcc_esrd_v24 string,
	cms_hcc_v22 string,
	cms_hcc_v24 string,
	cms_hcc_v28 string,
	rx_hcc_v05 string,
	rx_hcc_v08 string
);
LOAD DATA INTO hcc_suspecting._value_set_icd_10_cm_mappings (
	diagnosis_code string,
	cms_hcc_esrd_v21 string,
	cms_hcc_esrd_v24 string,
	cms_hcc_v22 string,
	cms_hcc_v24 string,
	cms_hcc_v28 string,
	rx_hcc_v05 string,
	rx_hcc_v08 string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/hcc_suspecting_icd_10_cm_mappings.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS chronic_conditions;
CREATE OR REPLACE TABLE chronic_conditions._value_set_tuva_chronic_conditions_hierarchy (
	condition_family string,
	condition string,
	icd_10_cm_code string,
	icd_10_cm_description string,
	condition_column_name string
);
LOAD DATA INTO chronic_conditions._value_set_tuva_chronic_conditions_hierarchy (
	condition_family string,
	condition string,
	icd_10_cm_code string,
	icd_10_cm_description string,
	condition_column_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/tuva_chronic_conditions_hierarchy.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS chronic_conditions;
CREATE OR REPLACE TABLE chronic_conditions._value_set_cms_chronic_conditions_hierarchy (
	condition_id string,
	condition string,
	condition_column_name string,
	chronic_condition_type string,
	condition_category string,
	additional_logic string,
	claims_qualification string,
	inclusion_type string,
	code_system string,
	code string
);
LOAD DATA INTO chronic_conditions._value_set_cms_chronic_conditions_hierarchy (
	condition_id string,
	condition string,
	condition_column_name string,
	chronic_condition_type string,
	condition_category string,
	additional_logic string,
	claims_qualification string,
	inclusion_type string,
	code_system string,
	code string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_value_sets/0.10.1/cms_chronic_conditions_hierarchy.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE OR REPLACE TABLE reference_data.ansi_fips_state (
	ansi_fips_state_code string,
	ansi_fips_state_abbreviation string,
	ansi_fips_state_name string
);
LOAD DATA INTO reference_data.ansi_fips_state (
	ansi_fips_state_code string,
	ansi_fips_state_abbreviation string,
	ansi_fips_state_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/ansi_fips_state.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE OR REPLACE TABLE reference_data.calendar (
	full_date date,
	year integer,
	month integer,
	day integer,
	month_name string,
	day_of_week_number integer,
	day_of_week_name string,
	week_of_year integer,
	day_of_year integer,
	year_month string,
	first_day_of_month date,
	last_day_of_month date,
	year_month_int integer
);
LOAD DATA INTO reference_data.calendar (
	full_date date,
	year integer,
	month integer,
	day integer,
	month_name string,
	day_of_week_number integer,
	day_of_week_name string,
	week_of_year integer,
	day_of_year integer,
	year_month string,
	first_day_of_month date,
	last_day_of_month date,
	year_month_int integer
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/calendar.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE OR REPLACE TABLE reference_data.code_type (
	code_type string
);
LOAD DATA INTO reference_data.code_type (
	code_type string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/code_type.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE OR REPLACE TABLE reference_data.fips_county (
	fips_code string,
	county string,
	state string
);
LOAD DATA INTO reference_data.fips_county (
	fips_code string,
	county string,
	state string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/fips_county.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE OR REPLACE TABLE reference_data.ssa_fips_state (
	ssa_fips_state_code string,
	ssa_fips_state_name string
);
LOAD DATA INTO reference_data.ssa_fips_state (
	ssa_fips_state_code string,
	ssa_fips_state_name string
) FROM FILES (
  FORMAT = 'CSV',
  URIS = ['gs://tuva-public-resources/versioned_terminology/0.10.1/ssa_fips_state.csv*'],
  COMPRESSION = 'GZIP',
  NULL_MARKER = '\\N',
  QUOTE = '"',
  ALLOW_QUOTED_NEWLINES = True
);


