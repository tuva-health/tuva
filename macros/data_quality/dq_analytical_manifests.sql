{% macro dq_analytical_metric_manifest() %}
    {% set manifest_json %}
[
    {
        "sort_order": 1,
        "model_name": "data_quality__analytical_key_metric__acute_inpatient_visits_per_1000_members",
        "domain": "acute inpatient",
        "metric": "acute inpatient visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "acute inpatient"
    },
    {
        "sort_order": 2,
        "model_name": "data_quality__analytical_key_metric__acute_inpatient_days_per_1000_members",
        "domain": "acute inpatient",
        "metric": "acute inpatient days per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_days_per_1000",
        "encounter_type": "acute inpatient"
    },
    {
        "sort_order": 3,
        "model_name": "data_quality__analytical_key_metric__acute_inpatient_average_length_of_stay",
        "domain": "acute inpatient",
        "metric": "average length of stay",
        "result_type": "decimal",
        "family": "encounter_average_length_of_stay",
        "encounter_type": "acute inpatient"
    },
    {
        "sort_order": 4,
        "model_name": "data_quality__analytical_key_metric__acute_inpatient_mortality_rate",
        "domain": "acute inpatient",
        "metric": "mortality rate",
        "result_type": "decimal",
        "family": "acute_inpatient_mortality_rate"
    },
    {
        "sort_order": 5,
        "model_name": "data_quality__analytical_key_metric__acute_inpatient_average_cost_per_visit",
        "domain": "acute inpatient",
        "metric": "average cost per visit",
        "result_type": "decimal",
        "family": "encounter_average_paid_amount",
        "encounter_type": "acute inpatient"
    },
    {
        "sort_order": 6,
        "model_name": "data_quality__analytical_key_metric__pmpm__total_paid",
        "domain": "pmpm",
        "metric": "Total PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "total_paid"
    },
    {
        "sort_order": 7,
        "model_name": "data_quality__analytical_key_metric__pmpm__medical_paid",
        "domain": "pmpm",
        "metric": "Medical PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "medical_paid"
    },
    {
        "sort_order": 8,
        "model_name": "data_quality__analytical_key_metric__pmpm__pharmacy_paid",
        "domain": "pmpm",
        "metric": "Pharmacy PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "pharmacy_paid"
    },
    {
        "sort_order": 9,
        "model_name": "data_quality__analytical_key_metric__pmpm__acute_inpatient_paid",
        "domain": "pmpm",
        "metric": "Acute Inpatient PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "acute_inpatient_paid"
    },
    {
        "sort_order": 10,
        "model_name": "data_quality__analytical_key_metric__pmpm__ambulance_paid",
        "domain": "pmpm",
        "metric": "Ambulance PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "ambulance_paid"
    },
    {
        "sort_order": 11,
        "model_name": "data_quality__analytical_key_metric__pmpm__ambulatory_surgery_center_paid",
        "domain": "pmpm",
        "metric": "Ambulatory Surgery Center PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "ambulatory_surgery_center_paid"
    },
    {
        "sort_order": 12,
        "model_name": "data_quality__analytical_key_metric__pmpm__dialysis_paid",
        "domain": "pmpm",
        "metric": "Dialysis PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "dialysis_paid"
    },
    {
        "sort_order": 13,
        "model_name": "data_quality__analytical_key_metric__pmpm__durable_medical_equipment_paid",
        "domain": "pmpm",
        "metric": "Durable Medical Equipment PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "durable_medical_equipment_paid"
    },
    {
        "sort_order": 14,
        "model_name": "data_quality__analytical_key_metric__pmpm__emergency_department_paid",
        "domain": "pmpm",
        "metric": "Emergency Department PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "emergency_department_paid"
    },
    {
        "sort_order": 15,
        "model_name": "data_quality__analytical_key_metric__pmpm__home_health_paid",
        "domain": "pmpm",
        "metric": "Home Health PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "home_health_paid"
    },
    {
        "sort_order": 16,
        "model_name": "data_quality__analytical_key_metric__pmpm__inpatient_hospice_paid",
        "domain": "pmpm",
        "metric": "Inpatient Hospice PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "inpatient_hospice_paid"
    },
    {
        "sort_order": 17,
        "model_name": "data_quality__analytical_key_metric__pmpm__inpatient_psychiatric_paid",
        "domain": "pmpm",
        "metric": "Inpatient Psychiatric PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "inpatient_psychiatric_paid"
    },
    {
        "sort_order": 18,
        "model_name": "data_quality__analytical_key_metric__pmpm__inpatient_rehabilitation_paid",
        "domain": "pmpm",
        "metric": "Inpatient Rehabilitation PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "inpatient_rehabilitation_paid"
    },
    {
        "sort_order": 19,
        "model_name": "data_quality__analytical_key_metric__pmpm__lab_paid",
        "domain": "pmpm",
        "metric": "Lab PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "lab_paid"
    },
    {
        "sort_order": 20,
        "model_name": "data_quality__analytical_key_metric__pmpm__observation_paid",
        "domain": "pmpm",
        "metric": "Observation PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "observation_paid"
    },
    {
        "sort_order": 21,
        "model_name": "data_quality__analytical_key_metric__pmpm__office_based_other_paid",
        "domain": "pmpm",
        "metric": "Office-Based Other PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "office_based_other_paid"
    },
    {
        "sort_order": 22,
        "model_name": "data_quality__analytical_key_metric__pmpm__office_based_pt_ot_st_paid",
        "domain": "pmpm",
        "metric": "Office-Based PT/OT/ST PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "office_based_pt_ot_st_paid"
    },
    {
        "sort_order": 23,
        "model_name": "data_quality__analytical_key_metric__pmpm__office_based_radiology_paid",
        "domain": "pmpm",
        "metric": "Office-Based Radiology PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "office_based_radiology_paid"
    },
    {
        "sort_order": 24,
        "model_name": "data_quality__analytical_key_metric__pmpm__office_based_surgery_paid",
        "domain": "pmpm",
        "metric": "Office-Based Surgery PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "office_based_surgery_paid"
    },
    {
        "sort_order": 25,
        "model_name": "data_quality__analytical_key_metric__pmpm__office_based_visit_paid",
        "domain": "pmpm",
        "metric": "Office-Based Visit PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "office_based_visit_paid"
    },
    {
        "sort_order": 26,
        "model_name": "data_quality__analytical_key_metric__pmpm__other_paid",
        "domain": "pmpm",
        "metric": "Other PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "other_paid_2"
    },
    {
        "sort_order": 27,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_hospice_paid",
        "domain": "pmpm",
        "metric": "Outpatient Hospice PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_hospice_paid"
    },
    {
        "sort_order": 28,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_hospital_or_clinic_paid",
        "domain": "pmpm",
        "metric": "Outpatient Hospital or Clinic PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_hospital_or_clinic_paid"
    },
    {
        "sort_order": 29,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_pt_ot_st_paid",
        "domain": "pmpm",
        "metric": "Outpatient PT/OT/ST PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_pt_ot_st_paid"
    },
    {
        "sort_order": 30,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_psychiatric_paid",
        "domain": "pmpm",
        "metric": "Outpatient Psychiatric PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_psychiatric_paid"
    },
    {
        "sort_order": 31,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_radiology_paid",
        "domain": "pmpm",
        "metric": "Outpatient Radiology PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_radiology_paid"
    },
    {
        "sort_order": 32,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_rehabilitation_paid",
        "domain": "pmpm",
        "metric": "Outpatient Rehabilitation PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_rehabilitation_paid"
    },
    {
        "sort_order": 33,
        "model_name": "data_quality__analytical_key_metric__pmpm__outpatient_surgery_paid",
        "domain": "pmpm",
        "metric": "Outpatient Surgery PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "outpatient_surgery_paid"
    },
    {
        "sort_order": 34,
        "model_name": "data_quality__analytical_key_metric__pmpm__pharmacy_paid_service_category_2",
        "domain": "pmpm",
        "metric": "Pharmacy PMPM (paid) | service_category_2",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "pharmacy_paid_2"
    },
    {
        "sort_order": 35,
        "model_name": "data_quality__analytical_key_metric__pmpm__skilled_nursing_paid",
        "domain": "pmpm",
        "metric": "Skilled Nursing PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "skilled_nursing_paid"
    },
    {
        "sort_order": 36,
        "model_name": "data_quality__analytical_key_metric__pmpm__telehealth_visit_paid",
        "domain": "pmpm",
        "metric": "Telehealth Visit PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "telehealth_visit_paid"
    },
    {
        "sort_order": 37,
        "model_name": "data_quality__analytical_key_metric__pmpm__urgent_care_paid",
        "domain": "pmpm",
        "metric": "Urgent Care PMPM (paid)",
        "result_type": "decimal",
        "family": "pmpm",
        "value_column": "urgent_care_paid"
    },
    {
        "sort_order": 38,
        "model_name": "data_quality__analytical_key_metric__total_member_months",
        "domain": "member months",
        "metric": "Total Member Months",
        "result_type": "count",
        "family": "total_member_months"
    },
    {
        "sort_order": 39,
        "model_name": "data_quality__analytical_key_metric__members_with_claims_without_enrollment",
        "domain": "member months",
        "metric": "Members w/ Claims w/o Enrollment",
        "result_type": "count",
        "family": "members_with_claims_without_enrollment"
    },
    {
        "sort_order": 40,
        "model_name": "data_quality__analytical_key_metric__average_member_months",
        "domain": "member months",
        "metric": "Average Member Months",
        "result_type": "decimal",
        "family": "average_member_months"
    },
    {
        "sort_order": 41,
        "model_name": "data_quality__analytical_key_metric__max_member_months",
        "domain": "member months",
        "metric": "Max Member Months",
        "result_type": "count",
        "family": "max_member_months"
    },
    {
        "sort_order": 42,
        "model_name": "data_quality__analytical_key_metric__ed_classification__alcohol_related",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Alcohol Related",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Alcohol Related"
    },
    {
        "sort_order": 43,
        "model_name": "data_quality__analytical_key_metric__ed_classification__emergent_ed_care_needed_not_preventable_avoidable",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Emergent, ED Care Needed, Not Preventable/Avoidable",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Emergent, ED Care Needed, Not Preventable/Avoidable"
    },
    {
        "sort_order": 44,
        "model_name": "data_quality__analytical_key_metric__ed_classification__emergent_ed_care_needed_preventable_avoidable",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Emergent, ED Care Needed, Preventable/Avoidable",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Emergent, ED Care Needed, Preventable/Avoidable"
    },
    {
        "sort_order": 45,
        "model_name": "data_quality__analytical_key_metric__ed_classification__emergent_primary_care_treatable",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Emergent, Primary Care Treatable",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Emergent, Primary Care Treatable"
    },
    {
        "sort_order": 46,
        "model_name": "data_quality__analytical_key_metric__ed_classification__injury",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Injury",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Injury"
    },
    {
        "sort_order": 47,
        "model_name": "data_quality__analytical_key_metric__ed_classification__mental_health_related",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Mental Health Related",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Mental Health Related"
    },
    {
        "sort_order": 48,
        "model_name": "data_quality__analytical_key_metric__ed_classification__non_emergent",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Non-Emergent",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": "Non-Emergent"
    },
    {
        "sort_order": 49,
        "model_name": "data_quality__analytical_key_metric__ed_classification__not_classified",
        "domain": "emergency department",
        "metric": "Percent of ED Visits | Not Classified",
        "result_type": "decimal",
        "family": "ed_classification_percentage",
        "classification": null
    },
    {
        "sort_order": 50,
        "model_name": "data_quality__analytical_key_metric__ed_visits_per_1000_members",
        "domain": "emergency department",
        "metric": "ed visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "emergency department"
    },
    {
        "sort_order": 51,
        "model_name": "data_quality__analytical_key_metric__ed_average_cost_per_visit",
        "domain": "emergency department",
        "metric": "average cost per visit",
        "result_type": "decimal",
        "family": "encounter_average_paid_amount",
        "encounter_type": "emergency department"
    },
    {
        "sort_order": 52,
        "model_name": "data_quality__analytical_key_metric__count_of_patients",
        "domain": "patient demographics",
        "metric": "count of patients",
        "result_type": "count",
        "family": "patient_count"
    },
    {
        "sort_order": 53,
        "model_name": "data_quality__analytical_key_metric__count_of_patients_deceased",
        "domain": "patient demographics",
        "metric": "count of patients | deceased",
        "result_type": "count",
        "family": "patient_count",
        "where_sql": "death_flag = 1 or death_date is not null"
    },
    {
        "sort_order": 54,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_30_39",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 30-39",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "30-39"
    },
    {
        "sort_order": 55,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_40_49",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 40-49",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "40-49"
    },
    {
        "sort_order": 56,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_50_59",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 50-59",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "50-59"
    },
    {
        "sort_order": 57,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_60_69",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 60-69",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "60-69"
    },
    {
        "sort_order": 58,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_70_79",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 70-79",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "70-79"
    },
    {
        "sort_order": 59,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_80_89",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 80-89",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "80-89"
    },
    {
        "sort_order": 60,
        "model_name": "data_quality__analytical_key_metric__patient__age_group_90_plus",
        "domain": "patient demographics",
        "metric": "count distinct patients | age_group = 90+",
        "result_type": "count",
        "family": "patient_age_group_count",
        "age_group": "90+"
    },
    {
        "sort_order": 61,
        "model_name": "data_quality__analytical_key_metric__patient__sex_female",
        "domain": "patient demographics",
        "metric": "count distinct patients | sex = female",
        "result_type": "count",
        "family": "patient_sex_count",
        "sex": "female"
    },
    {
        "sort_order": 62,
        "model_name": "data_quality__analytical_key_metric__patient__sex_male",
        "domain": "patient demographics",
        "metric": "count distinct patients | sex = male",
        "result_type": "count",
        "family": "patient_sex_count",
        "sex": "male"
    },
    {
        "sort_order": 63,
        "model_name": "data_quality__analytical_key_metric__number_of_acute_inpatient_visits",
        "domain": "readmissions",
        "metric": "Number of Acute Inpatient Visits",
        "result_type": "count",
        "family": "acute_inpatient_count"
    },
    {
        "sort_order": 64,
        "model_name": "data_quality__analytical_key_metric__number_of_index_admissions",
        "domain": "readmissions",
        "metric": "Number of Index Admissions",
        "result_type": "count",
        "family": "readmissions_summary_count",
        "flag_expression": "summary.index_admission_flag = 1"
    },
    {
        "sort_order": 65,
        "model_name": "data_quality__analytical_key_metric__number_of_30_day_readmissions",
        "domain": "readmissions",
        "metric": "Number of 30-Day Readmissions",
        "result_type": "count",
        "family": "readmissions_summary_count",
        "flag_expression": "summary.readmit_30_flag = 1"
    },
    {
        "sort_order": 66,
        "model_name": "data_quality__analytical_key_metric__number_of_30_day_unplanned_readmissions",
        "domain": "readmissions",
        "metric": "Number of 30-Day Unplanned Readmissions",
        "result_type": "count",
        "family": "readmissions_summary_count",
        "flag_expression": "summary.unplanned_readmit_30_flag = 1"
    },
    {
        "sort_order": 67,
        "model_name": "data_quality__analytical_key_metric__rate_of_index_admissions",
        "domain": "readmissions",
        "metric": "Rate of Index Admissions",
        "result_type": "decimal",
        "family": "rate_of_index_admissions"
    },
    {
        "sort_order": 68,
        "model_name": "data_quality__analytical_key_metric__rate_of_30_day_all_cause_readmissions",
        "domain": "readmissions",
        "metric": "Rate of 30-Day All-Cause Readmissions",
        "result_type": "decimal",
        "family": "readmissions_rate",
        "numerator_expression": "summary.readmit_30_flag = 1"
    },
    {
        "sort_order": 69,
        "model_name": "data_quality__analytical_key_metric__rate_of_30_day_unplanned_readmissions",
        "domain": "readmissions",
        "metric": "Rate of 30-Day Unplanned Readmissions",
        "result_type": "decimal",
        "family": "readmissions_rate",
        "numerator_expression": "summary.unplanned_readmit_30_flag = 1"
    },
    {
        "sort_order": 70,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__anxiety_disorders",
        "domain": "chronic conditions",
        "metric": "prevalence | Anxiety Disorders",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Anxiety Disorders"
    },
    {
        "sort_order": 71,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__atherosclerosis",
        "domain": "chronic conditions",
        "metric": "prevalence | Atherosclerosis",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Atherosclerosis"
    },
    {
        "sort_order": 72,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__major_depressive_disorder",
        "domain": "chronic conditions",
        "metric": "prevalence | Major Depressive Disorder",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Major Depressive Disorder"
    },
    {
        "sort_order": 73,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__dyslipidemias",
        "domain": "chronic conditions",
        "metric": "prevalence | Dyslipidemias",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Dyslipidemias"
    },
    {
        "sort_order": 74,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__coronary_artery_disease",
        "domain": "chronic conditions",
        "metric": "prevalence | Coronary Artery Disease",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": null
    },
    {
        "sort_order": 75,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__type_2_diabetes_mellitus",
        "domain": "chronic conditions",
        "metric": "prevalence | Type 2 Diabetes Mellitus",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Type 2 Diabetes Mellitus"
    },
    {
        "sort_order": 76,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__chronic_kidney_disease",
        "domain": "chronic conditions",
        "metric": "prevalence | Chronic Kidney Disease",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Chronic Kidney Disease"
    },
    {
        "sort_order": 77,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__hypertension",
        "domain": "chronic conditions",
        "metric": "prevalence | Hypertension",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Hypertension"
    },
    {
        "sort_order": 78,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__obesity",
        "domain": "chronic conditions",
        "metric": "prevalence | Obesity",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Obesity"
    },
    {
        "sort_order": 79,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__osteoarthritis",
        "domain": "chronic conditions",
        "metric": "prevalence | Osteoarthritis",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Osteoarthritis"
    },
    {
        "sort_order": 80,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__dementia",
        "domain": "chronic conditions",
        "metric": "prevalence | Dementia",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": "Dementia"
    },
    {
        "sort_order": 81,
        "model_name": "data_quality__analytical_key_metric__chronic_condition__parkinsons",
        "domain": "chronic conditions",
        "metric": "prevalence | Parkinson's",
        "result_type": "decimal",
        "family": "chronic_condition_prevalence",
        "source_condition": null
    },
    {
        "sort_order": 82,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_acute_inpatient",
        "domain": "encounters",
        "metric": "inpatient | acute inpatient visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "acute inpatient"
    },
    {
        "sort_order": 83,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_hospice",
        "domain": "encounters",
        "metric": "inpatient | inpatient hospice visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient hospice"
    },
    {
        "sort_order": 84,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_long_term_acute_care",
        "domain": "encounters",
        "metric": "inpatient | inpatient long term acute care visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient long term acute care"
    },
    {
        "sort_order": 85,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_psych",
        "domain": "encounters",
        "metric": "inpatient | inpatient psych visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient psych"
    },
    {
        "sort_order": 86,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_rehabilitation",
        "domain": "encounters",
        "metric": "inpatient | inpatient rehabilitation visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient rehabilitation"
    },
    {
        "sort_order": 87,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_skilled_nursing",
        "domain": "encounters",
        "metric": "inpatient | inpatient skilled nursing visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient skilled nursing"
    },
    {
        "sort_order": 88,
        "model_name": "data_quality__analytical_key_metric__encounter__inpatient_inpatient_substance_use",
        "domain": "encounters",
        "metric": "inpatient | inpatient substance use visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "inpatient substance use"
    },
    {
        "sort_order": 89,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_ambulatory_surgery_center",
        "domain": "encounters",
        "metric": "outpatient | ambulatory surgery center visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "ambulatory surgery center"
    },
    {
        "sort_order": 90,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_dialysis",
        "domain": "encounters",
        "metric": "outpatient | dialysis visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "dialysis"
    },
    {
        "sort_order": 91,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_emergency_department",
        "domain": "encounters",
        "metric": "outpatient | emergency department visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "emergency department"
    },
    {
        "sort_order": 92,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_home_health",
        "domain": "encounters",
        "metric": "outpatient | home health visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "home health"
    },
    {
        "sort_order": 93,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_hospice",
        "domain": "encounters",
        "metric": "outpatient | outpatient hospice visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient hospice"
    },
    {
        "sort_order": 94,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_hospital_or_clinic",
        "domain": "encounters",
        "metric": "outpatient | outpatient hospital or clinic visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient hospital or clinic"
    },
    {
        "sort_order": 95,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_injections",
        "domain": "encounters",
        "metric": "outpatient | outpatient injections visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient injections"
    },
    {
        "sort_order": 96,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_psych",
        "domain": "encounters",
        "metric": "outpatient | outpatient psych visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient psych"
    },
    {
        "sort_order": 97,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_pt_ot_st",
        "domain": "encounters",
        "metric": "outpatient | outpatient pt/ot/st visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient pt/ot/st"
    },
    {
        "sort_order": 98,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_radiology",
        "domain": "encounters",
        "metric": "outpatient | outpatient radiology visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient radiology"
    },
    {
        "sort_order": 99,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_rehabilitation",
        "domain": "encounters",
        "metric": "outpatient | outpatient rehabilitation visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient rehabilitation"
    },
    {
        "sort_order": 100,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_substance_use",
        "domain": "encounters",
        "metric": "outpatient | outpatient substance use visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient substance use"
    },
    {
        "sort_order": 101,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_outpatient_surgery",
        "domain": "encounters",
        "metric": "outpatient | outpatient surgery visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "outpatient surgery"
    },
    {
        "sort_order": 102,
        "model_name": "data_quality__analytical_key_metric__encounter__outpatient_urgent_care",
        "domain": "encounters",
        "metric": "outpatient | urgent care visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "urgent care"
    },
    {
        "sort_order": 103,
        "model_name": "data_quality__analytical_key_metric__encounter__office_based_office_visit",
        "domain": "encounters",
        "metric": "office based | office visit visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "office visit"
    },
    {
        "sort_order": 104,
        "model_name": "data_quality__analytical_key_metric__encounter__other_ambulance_orphaned",
        "domain": "encounters",
        "metric": "other | ambulance - orphaned visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "ambulance - orphaned"
    },
    {
        "sort_order": 105,
        "model_name": "data_quality__analytical_key_metric__encounter__other_dme_orphaned",
        "domain": "encounters",
        "metric": "other | dme - orphaned visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "dme - orphaned"
    },
    {
        "sort_order": 106,
        "model_name": "data_quality__analytical_key_metric__encounter__other_lab_orphaned",
        "domain": "encounters",
        "metric": "other | lab - orphaned visits per 1,000 members",
        "result_type": "decimal",
        "family": "encounter_visits_per_1000",
        "encounter_type": "lab - orphaned"
    },
    {
        "sort_order": 107,
        "model_name": "data_quality__analytical_key_metric__medication__glp_1_ozempic_semaglutide",
        "domain": "medications pmpm",
        "metric": "glp-1 | ozempic (semaglutide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "ozempic",
        "ingredient_name": "semaglutide"
    },
    {
        "sort_order": 108,
        "model_name": "data_quality__analytical_key_metric__medication__glp_1_wegovy_semaglutide",
        "domain": "medications pmpm",
        "metric": "glp-1 | wegovy (semaglutide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "wegovy",
        "ingredient_name": "semaglutide"
    },
    {
        "sort_order": 109,
        "model_name": "data_quality__analytical_key_metric__medication__glp_1_mounjaro_tirzepatide",
        "domain": "medications pmpm",
        "metric": "glp-1 | mounjaro (tirzepatide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "mounjaro",
        "ingredient_name": "tirzepatide"
    },
    {
        "sort_order": 110,
        "model_name": "data_quality__analytical_key_metric__medication__glp_1_zepbound_tirzepatide",
        "domain": "medications pmpm",
        "metric": "glp-1 | zepbound (tirzepatide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "zepbound",
        "ingredient_name": "tirzepatide"
    },
    {
        "sort_order": 111,
        "model_name": "data_quality__analytical_key_metric__medication__autoimmune_humira_adalimumab",
        "domain": "medications pmpm",
        "metric": "autoimmune | humira (adalimumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "humira",
        "ingredient_name": "adalimumab"
    },
    {
        "sort_order": 112,
        "model_name": "data_quality__analytical_key_metric__medication__autoimmune_stelara_ustekinumab",
        "domain": "medications pmpm",
        "metric": "autoimmune | stelara (ustekinumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "stelara",
        "ingredient_name": "ustekinumab"
    },
    {
        "sort_order": 113,
        "model_name": "data_quality__analytical_key_metric__medication__autoimmune_skyrizi_risankizumab",
        "domain": "medications pmpm",
        "metric": "autoimmune | skyrizi (risankizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "skyrizi",
        "ingredient_name": "risankizumab"
    },
    {
        "sort_order": 114,
        "model_name": "data_quality__analytical_key_metric__medication__autoimmune_enbrel_etanercept",
        "domain": "medications pmpm",
        "metric": "autoimmune | enbrel (etanercept)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "enbrel",
        "ingredient_name": "etanercept"
    },
    {
        "sort_order": 115,
        "model_name": "data_quality__analytical_key_metric__medication__oncology_keytruda_pembrolizumab",
        "domain": "medications pmpm",
        "metric": "oncology | keytruda (pembrolizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "keytruda",
        "ingredient_name": "pembrolizumab"
    },
    {
        "sort_order": 116,
        "model_name": "data_quality__analytical_key_metric__medication__oncology_opdivo_nivolumab",
        "domain": "medications pmpm",
        "metric": "oncology | opdivo (nivolumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "opdivo",
        "ingredient_name": "nivolumab"
    },
    {
        "sort_order": 117,
        "model_name": "data_quality__analytical_key_metric__medication__oncology_revlimid_lenalidomide",
        "domain": "medications pmpm",
        "metric": "oncology | revlimid (lenalidomide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "revlimid",
        "ingredient_name": "lenalidomide"
    },
    {
        "sort_order": 118,
        "model_name": "data_quality__analytical_key_metric__medication__oncology_imbruvica_ibrutinib",
        "domain": "medications pmpm",
        "metric": "oncology | imbruvica (ibrutinib)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "imbruvica",
        "ingredient_name": "ibrutinib"
    },
    {
        "sort_order": 119,
        "model_name": "data_quality__analytical_key_metric__medication__gene_therapy_zolgensma_onasemnogene_abeparvovec",
        "domain": "medications pmpm",
        "metric": "gene therapy | zolgensma (onasemnogene abeparvovec)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "zolgensma",
        "ingredient_name": "onasemnogene abeparvovec"
    },
    {
        "sort_order": 120,
        "model_name": "data_quality__analytical_key_metric__medication__gene_therapy_luxturna_voretigene_neparvovec",
        "domain": "medications pmpm",
        "metric": "gene therapy | luxturna (voretigene neparvovec)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "luxturna",
        "ingredient_name": "voretigene neparvovec"
    },
    {
        "sort_order": 121,
        "model_name": "data_quality__analytical_key_metric__medication__gene_therapy_kymriah_tisagenlecleucel",
        "domain": "medications pmpm",
        "metric": "gene therapy | kymriah (tisagenlecleucel)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "kymriah",
        "ingredient_name": "tisagenlecleucel"
    },
    {
        "sort_order": 122,
        "model_name": "data_quality__analytical_key_metric__medication__multiple_sclerosis_ocrevus_ocrelizumab",
        "domain": "medications pmpm",
        "metric": "multiple sclerosis | ocrevus (ocrelizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "ocrevus",
        "ingredient_name": "ocrelizumab"
    },
    {
        "sort_order": 123,
        "model_name": "data_quality__analytical_key_metric__medication__multiple_sclerosis_kesimpta_ofatumumab",
        "domain": "medications pmpm",
        "metric": "multiple sclerosis | kesimpta (ofatumumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "kesimpta",
        "ingredient_name": "ofatumumab"
    },
    {
        "sort_order": 124,
        "model_name": "data_quality__analytical_key_metric__medication__multiple_sclerosis_tecfidera_dimethyl_fumarate",
        "domain": "medications pmpm",
        "metric": "multiple sclerosis | tecfidera (dimethyl fumarate)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "tecfidera",
        "ingredient_name": "dimethyl fumarate"
    },
    {
        "sort_order": 125,
        "model_name": "data_quality__analytical_key_metric__medication__rare_disease_soliris_eculizumab",
        "domain": "medications pmpm",
        "metric": "rare disease | soliris (eculizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "soliris",
        "ingredient_name": "eculizumab"
    },
    {
        "sort_order": 126,
        "model_name": "data_quality__analytical_key_metric__medication__rare_disease_ultomiris_ravulizumab",
        "domain": "medications pmpm",
        "metric": "rare disease | ultomiris (ravulizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "ultomiris",
        "ingredient_name": "ravulizumab"
    },
    {
        "sort_order": 127,
        "model_name": "data_quality__analytical_key_metric__medication__rare_disease_vimizim_elosulfase_alfa",
        "domain": "medications pmpm",
        "metric": "rare disease | vimizim (elosulfase alfa)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "vimizim",
        "ingredient_name": "elosulfase alfa"
    },
    {
        "sort_order": 128,
        "model_name": "data_quality__analytical_key_metric__medication__hemophilia_hemlibra_emicizumab",
        "domain": "medications pmpm",
        "metric": "hemophilia | hemlibra (emicizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "hemlibra",
        "ingredient_name": "emicizumab"
    },
    {
        "sort_order": 129,
        "model_name": "data_quality__analytical_key_metric__medication__hemophilia_advate_antihemophilic_factor_viii",
        "domain": "medications pmpm",
        "metric": "hemophilia | advate (antihemophilic factor VIII)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "advate",
        "ingredient_name": "antihemophilic factor viii"
    },
    {
        "sort_order": 130,
        "model_name": "data_quality__analytical_key_metric__medication__hemophilia_benefix_coagulation_factor_ix",
        "domain": "medications pmpm",
        "metric": "hemophilia | benefix (coagulation factor IX)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "benefix",
        "ingredient_name": "coagulation factor ix"
    },
    {
        "sort_order": 131,
        "model_name": "data_quality__analytical_key_metric__medication__alzheimers_leqembi_lecanemab",
        "domain": "medications pmpm",
        "metric": "alzheimers | leqembi (lecanemab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "leqembi",
        "ingredient_name": "lecanemab"
    },
    {
        "sort_order": 132,
        "model_name": "data_quality__analytical_key_metric__medication__alzheimers_aduhelm_aducanumab",
        "domain": "medications pmpm",
        "metric": "alzheimers | aduhelm (aducanumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "aduhelm",
        "ingredient_name": "aducanumab"
    },
    {
        "sort_order": 133,
        "model_name": "data_quality__analytical_key_metric__medication__respiratory_dupixent_dupilumab",
        "domain": "medications pmpm",
        "metric": "respiratory | dupixent (dupilumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "dupixent",
        "ingredient_name": "dupilumab"
    },
    {
        "sort_order": 134,
        "model_name": "data_quality__analytical_key_metric__medication__respiratory_nucala_mepolizumab",
        "domain": "medications pmpm",
        "metric": "respiratory | nucala (mepolizumab)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "nucala",
        "ingredient_name": "mepolizumab"
    },
    {
        "sort_order": 135,
        "model_name": "data_quality__analytical_key_metric__medication__hiv_biktarvy_bictegravir_emtricitabine_tenofovir_alafenamide",
        "domain": "medications pmpm",
        "metric": "hiv | biktarvy (bictegravir/emtricitabine/tenofovir alafenamide)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "biktarvy",
        "ingredient_name": "bictegravir/emtricitabine/tenofovir alafenamide"
    },
    {
        "sort_order": 136,
        "model_name": "data_quality__analytical_key_metric__medication__hiv_cabenuva_cabotegravir_rilpivirine",
        "domain": "medications pmpm",
        "metric": "hiv | cabenuva (cabotegravir/rilpivirine)",
        "result_type": "decimal",
        "family": "medication_pmpm",
        "brand_name": "cabenuva",
        "ingredient_name": "cabotegravir/rilpivirine"
    }
]
    {% endset %}

    {{ return(fromjson(manifest_json)) }}
{% endmacro %}
