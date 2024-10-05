{% docs admission_date %}
Admission date for the claim (inpatient claims only).
{% enddocs %}

{% docs admit_age %}
The age of the patient at the time of admission.
{% enddocs %}

{% docs admit_source_code %}
Indicates where the patient was before the healthcare encounter (inpatient claims only).
{% enddocs %}

{% docs admit_source_description %}
Description of the admit_source_code for the encounter.
{% enddocs %}

{% docs admit_type_code %}
Indicates the type of admission (inpatient claims only).
{% enddocs %}

{% docs admit_type_description %}
Description of the admit_type_code for the encounter.
{% enddocs %}

{% docs allowed_amount %}
The total amount allowed (includes amount paid by the insurer and patient).
{% enddocs %}

{% docs ambulance_flag %}
Indicates whether ambulance services were utilized during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs apr_drg_code %}
APR-DRG for the claim (inpatient claims only).
{% enddocs %}

{% docs apr_drg_description %}
Description of the APR-DRG code.
{% enddocs %}

{% docs billing_id %}
Billing ID for the claim (typically represents organization billing the claim).
{% enddocs %}

{% docs bill_type_code %}
Bill type code for the claim (institutional claims only).
{% enddocs %}

{% docs bill_type_description %}
Bill type description.
{% enddocs %}

{% docs birth_date %}
The birth date of the patient.
{% enddocs %}

{% docs ccs_category %}
The Clinical Classifications Software (CCS) category code for the diagnosis or procedure.
{% enddocs %}

{% docs ccs_category_description %}
Description of the Clinical Classifications Software (CCS) category.
{% enddocs %}

{% docs charge_amount %}
The total amount charged for the services provided, before any adjustments or payments. This is typically in US dollars.
{% enddocs %}

{% docs claim_attribution_number %}
A unique number used for attributing or associating claims with specific entities or processes.
{% enddocs %}

{% docs claim_count %}
The number of claims associated with the encounter or record.
{% enddocs %}

{% docs claim_end_date %}
End date for the claim.
{% enddocs %}

{% docs claim_id %}
Unique identifier for a claim. Each claim represents a distinct healthcare service or set of services provided to a patient.
{% enddocs %}

{% docs claim_line_attribution_number %}
A unique number used for attributing or associating specific claim lines with entities or processes.
{% enddocs %}

{% docs claim_line_end_date %}
End date for the claim line.
{% enddocs %}

{% docs claim_line_id %}
Unique identifier for each line item within a claim.
{% enddocs %}

{% docs claim_line_number %}
Indicates the line number for the particular line of the claim.
{% enddocs %}

{% docs claim_line_start_date %}
Start date for the claim line.
{% enddocs %}

{% docs claim_start_date %}
The date when the healthcare service was provided. Format: YYYY-MM-DD.
{% enddocs %}

{% docs claim_type %}
Indicates whether the claim is professional (CMS-1500), institutional (UB-04), dental, or vision.
{% enddocs %}

{% docs close_flag %}
A flag indicating if the claim has been closed.
{% enddocs %}

{% docs data_source %}
User-configured field that indicates the data source.
{% enddocs %}

{% docs default_ccsr_category_description_ip %}
Description of the default Clinical Classifications Software Refined (CCSR) category for inpatient services.
{% enddocs %}

{% docs default_ccsr_category_description_op %}
Description of the default Clinical Classifications Software Refined (CCSR) category for outpatient services.
{% enddocs %}

{% docs default_ccsr_category_ip %}
The default Clinical Classifications Software Refined (CCSR) category code for inpatient services.
{% enddocs %}

{% docs default_ccsr_category_op %}
The default Clinical Classifications Software Refined (CCSR) category code for outpatient services.
{% enddocs %}

{% docs delivery_flag %}
Indicates whether the encounter involved a delivery (1 for yes, 0 for no).
{% enddocs %}

{% docs delivery_type %}
Type of delivery that occurred during the encounter, if applicable.
{% enddocs %}

{% docs diagnosis_code_1 %}
The primary diagnosis code for the encounter or claim.
{% enddocs %}

{% docs diagnosis_code_type %}
The coding system used for the diagnosis code (e.g., ICD-10-CM, ICD-9-CM).
{% enddocs %}

{% docs discharge_date %}
Discharge date for the claim (inpatient claims only).
{% enddocs %}

{% docs discharge_disposition_code %}
Indicates the type of setting the patient was discharged to (institutional inpatient claims only).
{% enddocs %}

{% docs discharge_disposition_description %}
Description of the discharge_disposition_code for the encounter.
{% enddocs %}

{% docs distinct_claims %}
The number of distinct claims associated with the record.
{% enddocs %}

{% docs distinct_service_category_count %}
The count of distinct service categories associated with the claim.
{% enddocs %}

{% docs dme_flag %}
Indicates whether durable medical equipment (DME) was used during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs dq_problem %}
A flag or description indicating a data quality issue.
{% enddocs %}

{% docs duplicate_row_number %}
A number assigned to duplicate rows for identification purposes.
{% enddocs %}

{% docs ed_flag %}
Indicates whether the encounter involved an emergency department visit (1 for yes, 0 for no).
{% enddocs %}

{% docs encounter_claim_number %}
A unique identifier for the encounter or claim.
{% enddocs %}

{% docs encounter_claim_number_desc %}
A description or additional information about the encounter claim number.
{% enddocs %}

{% docs encounter_end_date %}
Date when the encounter ended.
{% enddocs %}

{% docs encounter_group %}
Categorization of the encounter into groups based on predefined criteria.
{% enddocs %}

{% docs encounter_id %}
Unique identifier for each encounter in the dataset.
{% enddocs %}

{% docs encounter_start_date %}
Date when the encounter started.
{% enddocs %}

{% docs encounter_type %}
Indicates the type of encounter e.g. acute inpatient, emergency department, etc.
{% enddocs %}

{% docs end_date %}
The end date of the service or claim period.
{% enddocs %}

{% docs facility_id %}
Facility ID for the claim (typically represents the facility where services were performed).
{% enddocs %}

{% docs facility_npi %}
Facility NPI for the claim (typically represents the facility where services were performed).
{% enddocs %}

{% docs facility_name %}
Facility name.
{% enddocs %}

{% docs facility_type %}
The type of facility e.g. acute care hospital.
{% enddocs %}

{% docs gender %}
The gender of the patient.
{% enddocs %}

{% docs hcpcs_code %}
The CPT or HCPCS code representing the procedure or service provided. These codes are used to describe medical, surgical, and diagnostic services.
{% enddocs %}

{% docs hcpcs_modifier_1 %}
1st modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_2 %}
2nd modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_3 %}
3rd modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_4 %}
4th modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_5 %}
5th modifier for HCPCS code.
{% enddocs %}

{% docs inferred_claim_end_column_used %}
The column used to infer the claim end date.
{% enddocs %}

{% docs inferred_claim_end_year_month %}
The inferred year and month of the claim end date.
{% enddocs %}

{% docs inferred_claim_start_column_used %}
The column used to infer the claim start date.
{% enddocs %}

{% docs inferred_claim_start_year_month %}
The inferred year and month of the claim start date.
{% enddocs %}

{% docs inst_claim_count %}
Number of institutional claims generated from the encounter.
{% enddocs %}

{% docs lab_flag %}
Indicates whether lab services were utilized during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs length_of_stay %}
Length of the encounter calculated as encounter_end_date - encounter_start_date.
{% enddocs %}

{% docs medical_surgical %}
A flag or code indicating if the service was medical or surgical.
{% enddocs %}

{% docs min_closing_row %}
The minimum row number for closing entries in the dataset.
{% enddocs %}

{% docs modality %}
The mode or method of treatment or service delivery.
{% enddocs %}

{% docs mortality_flag %}
A flag indicating if the patient died during the encounter.
{% enddocs %}

{% docs ms_drg_code %}
MS-DRG for the claim (inpatient claims only).
{% enddocs %}

{% docs ms_drg_description %}
Description of the ms_drg_code.
{% enddocs %}

{% docs newborn_flag %}
Indicates whether the encounter was for a newborn (1 for yes, 0 for no).
{% enddocs %}

{% docs nicu_flag %}
Indicates whether the newborn was admitted to the Neonatal Intensive Care Unit (NICU) during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs observation_flag %}
Indicates whether the encounter was marked as an observation stay (1 for yes, 0 for no).
{% enddocs %}

{% docs old_encounter_id %}
A previous or alternative identifier for the encounter.
{% enddocs %}

{% docs original_service_cat_2 %}
The original second-level service category.
{% enddocs %}

{% docs original_service_cat_3 %}
The original third-level service category.
{% enddocs %}

{% docs paid_amount %}
The total amount paid by the insurer.
{% enddocs %}

{% docs patient_data_source_id %}
Identifier for the source system from which patient data originated.
{% enddocs %}

{% docs patient_id %}
Unique identifier for each patient in the dataset.
{% enddocs %}

{% docs patient_row_num %}
A row number assigned to the patient's records.
{% enddocs %}

{% docs payer %}
Name of the payer (i.e. health insurer) providing coverage.
{% enddocs %}

{% docs pharmacy_flag %}
Indicates whether pharmacy services were utilized during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs place_of_service_code %}
Place of service for the claim (professional claims only).
{% enddocs %}

{% docs place_of_service_description %}
Place of service description.
{% enddocs %}

{% docs plan %}
Name of the plan (i.e. sub contract) providing coverage.
{% enddocs %}

{% docs primary_diagnosis_code %}
Primary diagnosis code for the encounter. If from claims the primary diagnosis code comes from the institutional claim.
{% enddocs %}

{% docs primary_diagnosis_code_type %}
The type of condition code reported in the source system e.g. ICD-10-CM.
{% enddocs %}

{% docs primary_diagnosis_description %}
Description of the primary diagnosis code.
{% enddocs %}

{% docs primary_specialty_description %}
Description of the primary medical specialty of the provider.
{% enddocs %}

{% docs primary_taxonomy_code %}
The primary taxonomy code identifying the provider's specialty, classification, or area of practice.
{% enddocs %}

{% docs priority %}
The priority or urgency level of the service or claim.
{% enddocs %}

{% docs priority_number %}
A number indicating the priority or sequence of the service or claim.
{% enddocs %}

{% docs prof_claim_count %}
Number of professional claims generated from the encounter.
{% enddocs %}

{% docs provider_name %}
The name of the healthcare provider.
{% enddocs %}

{% docs provider_first_name %}
The first name of the healthcare provider.
{% enddocs %}

{% docs provider_last_name %}
The last name of the healthcare provider.
{% enddocs %}

{% docs provider_specialty %}
The medical specialty of the provider.
{% enddocs %}

{% docs race %}
The patient's race.
{% enddocs %}

{% docs relative_rank %}
A ranking or order of importance for the record.
{% enddocs %}

{% docs rend_primary_specialty_description %}
A description of the rendering provider's primary specialty.
{% enddocs %}

{% docs rendering_id %}
Rendering ID for the claim (typically represents the physician or entity providing services).
{% enddocs %}

{% docs revenue_center_code %}
Revenue center code for the claim line (institutional only and typically multiple codes per claim).
{% enddocs %}

{% docs revenue_center_description %}
Revenue center description.
{% enddocs %}

{% docs service_category_1 %}
The broader service category this claim belongs to.
{% enddocs %}

{% docs service_category_2 %}
The more specific service category this claim belongs to.
{% enddocs %}

{% docs service_category_3 %}
The most specific service category this claim belongs to.
{% enddocs %}

{% docs service_type %}
The type of service provided.
{% enddocs %}

{% docs source_model_name %}
The name of the source data model.
{% enddocs %}

{% docs start_date %}
The start date of the service or claim period.
{% enddocs %}

{% docs total_allowed_amount %}
The total amount allowed by the insurance company for all services in the claim.
{% enddocs %}

{% docs total_charge_amount %}
The total amount charged for all services in the claim.
{% enddocs %}

{% docs total_paid_amount %}
The total amount paid for all services in the claim.
{% enddocs %}

{% docs tuva_last_run %}
The last time the data was refreshed. Generated by `dbt_utils.pretty_time` as the local time of the `dbt run` environment. Timezone is configurable via the `tuva_last_run` var.
{% enddocs %}