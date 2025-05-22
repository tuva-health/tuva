{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

with cte as 
(
    SELECT DISTINCT
         a.person_id
         , cal.year as year_nbr
         , replace(REPLACE(REPLACE(REPLACE(LOWER(c.condition_column_name), '''', ''), '.', ''), '-', ''),' ','_') AS cleaned_concept_name
    FROM {{ ref('core__condition') }} a
    INNER JOIN {{ ref('chronic_conditions__cms_chronic_conditions_hierarchy') }} c
        ON a.normalized_code = c.code
    INNER JOIN {{ ref('reference_data__calendar') }} cal
        ON a.recorded_date = cal.full_date
)

,member_month as (
    select mm.person_id
    ,left(year_month,4) as year_nbr
    ,count(year_month) as member_month_count
    from {{ ref('core__member_months') }} mm
    group by mm.person_id
    ,left(year_month,4) 
)

,condition_flags as (
select 
 person_id as condition_person_id
,year_nbr as condition_year_nbr
,max(case when cleaned_concept_name = 'acute_myocardial_infarction' then 1 else 0 end) as cms_acute_myocardial_infarction
,max(case when cleaned_concept_name = 'adhd_conduct_disorders_and_hyperkinetic_syndrome' then 1 else 0 end) as cms_adhd_conduct_disorders_and_hyperkinetic_syndrome
,max(case when cleaned_concept_name = 'alcohol_use_disorders' then 1 else 0 end) as cms_alcohol_use_disorders
,max(case when cleaned_concept_name = 'anemia' then 1 else 0 end) as cms_anemia
,max(case when cleaned_concept_name = 'asthma' then 1 else 0 end) as cms_asthma
,max(case when cleaned_concept_name = 'atrial_fibrillation_and_flutter' then 1 else 0 end) as cms_atrial_fibrillation_and_flutter
,max(case when cleaned_concept_name = 'autism_spectrum_disorders' then 1 else 0 end) as cms_autism_spectrum_disorders
,max(case when cleaned_concept_name = 'benign_prostatic_hyperplasia' then 1 else 0 end) as cms_benign_prostatic_hyperplasia
,max(case when cleaned_concept_name = 'bipolar_disorder' then 1 else 0 end) as cms_bipolar_disorder
,max(case when cleaned_concept_name = 'cancer_breast' then 1 else 0 end) as cms_cancer_breast
,max(case when cleaned_concept_name = 'cancer_urologic_kidney_renal_pelvis_and_ureter' then 1 else 0 end) as cms_cancer_urologic_kidney_renal_pelvis_and_ureter
,max(case when cleaned_concept_name = 'cataract' then 1 else 0 end) as cms_cataract
,max(case when cleaned_concept_name = 'cerebral_palsy' then 1 else 0 end) as cms_cerebral_palsy
,max(case when cleaned_concept_name = 'chronic_kidney_disease' then 1 else 0 end) as cms_chronic_kidney_disease
,max(case when cleaned_concept_name = 'chronic_obstructive_pulmonary_disease' then 1 else 0 end) as cms_chronic_obstructive_pulmonary_disease
,max(case when cleaned_concept_name = 'depression_bipolar_or_other_depressive_mood_disorders' then 1 else 0 end) as cms_depression_bipolar_or_other_depressive_mood_disorders
,max(case when cleaned_concept_name = 'depressive_disorders' then 1 else 0 end) as cms_depressive_disorders
,max(case when cleaned_concept_name = 'diabetes' then 1 else 0 end) as cms_diabetes
,max(case when cleaned_concept_name = 'drug_use_disorders' then 1 else 0 end) as cms_drug_use_disorders
,max(case when cleaned_concept_name = 'epilepsy' then 1 else 0 end) as cms_epilepsy
,max(case when cleaned_concept_name = 'fibromyalgia_and_chronic_pain_and_fatigue' then 1 else 0 end) as cms_fibromyalgia_and_chronic_pain_and_fatigue
,max(case when cleaned_concept_name = 'glaucoma' then 1 else 0 end) as cms_glaucoma
,max(case when cleaned_concept_name = 'heart_failure_and_non_ischemic_heart_disease' then 1 else 0 end) as cms_heart_failure_and_non_ischemic_heart_disease
,max(case when cleaned_concept_name = 'hepatitis_a' then 1 else 0 end) as cms_hepatitis_a
,max(case when cleaned_concept_name = 'hepatitis_b_acute_or_unspecified' then 1 else 0 end) as cms_hepatitis_b_acute_or_unspecified
,max(case when cleaned_concept_name = 'hepatitis_c_acute' then 1 else 0 end) as cms_hepatitis_c_acute
,max(case when cleaned_concept_name = 'hepatitis_c_chronic' then 1 else 0 end) as cms_hepatitis_c_chronic
,max(case when cleaned_concept_name = 'hepatitis_c_unspecified' then 1 else 0 end) as cms_hepatitis_c_unspecified
,max(case when cleaned_concept_name = 'hepatitis_e' then 1 else 0 end) as cms_hepatitis_e
,max(case when cleaned_concept_name = 'human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids' then 1 else 0 end) as cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids
,max(case when cleaned_concept_name = 'hypertension' then 1 else 0 end) as cms_hypertension
,max(case when cleaned_concept_name = 'ischemic_heart_disease' then 1 else 0 end) as cms_ischemic_heart_disease
,max(case when cleaned_concept_name = 'migraine_and_chronic_headache' then 1 else 0 end) as cms_migraine_and_chronic_headache
,max(case when cleaned_concept_name = 'muscular_dystrophy' then 1 else 0 end) as cms_muscular_dystrophy
,max(case when cleaned_concept_name = 'opioid_use_disorder_oud' then 1 else 0 end) as cms_opioid_use_disorder_oud
,max(case when cleaned_concept_name = 'learning_disabilities' then 1 else 0 end) as cms_learning_disabilities
,max(case when cleaned_concept_name = 'leukemias_and_lymphomas' then 1 else 0 end) as cms_leukemias_and_lymphomas
,max(case when cleaned_concept_name = 'parkinsons_disease_and_secondary_parkinsonism' then 1 else 0 end) as cms_parkinsons_disease_and_secondary_parkinsonism
,max(case when cleaned_concept_name = 'peripheral_vascular_disease_pvd' then 1 else 0 end) as cms_peripheral_vascular_disease_pvd
,max(case when cleaned_concept_name = 'personality_disorders' then 1 else 0 end) as cms_personality_disorders
,max(case when cleaned_concept_name = 'pneumonia_all_cause' then 1 else 0 end) as cms_pneumonia_all_cause
,max(case when cleaned_concept_name = 'sensory_blindness_and_visual_impairment' then 1 else 0 end) as cms_sensory_blindness_and_visual_impairment
,max(case when cleaned_concept_name = 'spinal_cord_injury' then 1 else 0 end) as cms_spinal_cord_injury
,max(case when cleaned_concept_name = 'alzheimers_disease' then 1 else 0 end) as cms_alzheimers_disease
,max(case when cleaned_concept_name = 'anxiety_disorders' then 1 else 0 end) as cms_anxiety_disorders
,max(case when cleaned_concept_name = 'cancer_colorectal' then 1 else 0 end) as cms_cancer_colorectal
,max(case when cleaned_concept_name = 'cancer_endometrial' then 1 else 0 end) as cms_cancer_endometrial
,max(case when cleaned_concept_name = 'cancer_prostate' then 1 else 0 end) as cms_cancer_prostate
,max(case when cleaned_concept_name = 'cystic_fibrosis_and_other_metabolic_developmental_disorders' then 1 else 0 end) as cms_cystic_fibrosis_and_other_metabolic_developmental_disorders
,max(case when cleaned_concept_name = 'hepatitis_b_chronic' then 1 else 0 end) as cms_hepatitis_b_chronic
,max(case when cleaned_concept_name = 'hepatitis_d' then 1 else 0 end) as cms_hepatitis_d
,max(case when cleaned_concept_name = 'hip_pelvic_fracture' then 1 else 0 end) as cms_hip_pelvic_fracture
,max(case when cleaned_concept_name = 'hyperlipidemia' then 1 else 0 end) as cms_hyperlipidemia
,max(case when cleaned_concept_name = 'intellectual_disabilities_and_related_conditions' then 1 else 0 end) as cms_intellectual_disabilities_and_related_conditions
,max(case when cleaned_concept_name = 'liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis' then 1 else 0 end) as cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis
,max(case when cleaned_concept_name = 'non_alzheimers_dementia' then 1 else 0 end) as cms_non_alzheimers_dementia
,max(case when cleaned_concept_name = 'multiple_sclerosis_and_transverse_myelitis' then 1 else 0 end) as cms_multiple_sclerosis_and_transverse_myelitis
,max(case when cleaned_concept_name = 'osteoporosis_with_or_without_pathological_fracture' then 1 else 0 end) as cms_osteoporosis_with_or_without_pathological_fracture
,max(case when cleaned_concept_name = 'post_traumatic_stress_disorder_ptsd' then 1 else 0 end) as cms_post_traumatic_stress_disorder_ptsd
,max(case when cleaned_concept_name = 'rheumatoid_arthritis_osteoarthritis' then 1 else 0 end) as cms_rheumatoid_arthritis_osteoarthritis
,max(case when cleaned_concept_name = 'sensory_deafness_and_hearing_impairment' then 1 else 0 end) as cms_sensory_deafness_and_hearing_impairment
,max(case when cleaned_concept_name = 'sickle_cell_disease' then 1 else 0 end) as cms_sickle_cell_disease
,max(case when cleaned_concept_name = 'spina_bifida_and_other_congenital_anomalies_of_the_nervous_system' then 1 else 0 end) as cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system
,max(case when cleaned_concept_name = 'stroke_transient_ischemic_attack' then 1 else 0 end) as cms_stroke_transient_ischemic_attack
,max(case when cleaned_concept_name = 'tobacco_use' then 1 else 0 end) as cms_tobacco_use
,max(case when cleaned_concept_name = 'viral_hepatitis_general' then 1 else 0 end) as cms_viral_hepatitis_general
,max(case when cleaned_concept_name = 'schizophrenia' then 1 else 0 end) as cms_schizophrenia
,max(case when cleaned_concept_name = 'cancer_lung' then 1 else 0 end) as cms_cancer_lung
,max(case when cleaned_concept_name = 'hypothyroidism' then 1 else 0 end) as cms_hypothyroidism
,max(case when cleaned_concept_name = 'mobility_impairments' then 1 else 0 end) as cms_mobility_impairments
,max(case when cleaned_concept_name = 'obesity' then 1 else 0 end) as cms_obesity
,max(case when cleaned_concept_name = 'other_developmental_delays' then 1 else 0 end) as cms_other_developmental_delays
,max(case when cleaned_concept_name = 'pressure_and_chronic_ulcers' then 1 else 0 end) as cms_pressure_and_chronic_ulcers
,max(case when cleaned_concept_name = 'schizophrenia_and_other_psychotic_disorders' then 1 else 0 end) as cms_schizophrenia_and_other_psychotic_disorders
,max(case when cleaned_concept_name = 'traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage' then 1 else 0 end) as cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage

from cte 
group by 
person_id
,year_nbr 

)

select 
mm.person_id
,mm.year_nbr
,coalesce(cms_acute_myocardial_infarction,0) as cms_acute_myocardial_infarction
,coalesce(cms_adhd_conduct_disorders_and_hyperkinetic_syndrome,0) as cms_adhd_conduct_disorders_and_hyperkinetic_syndrome
,coalesce(cms_alcohol_use_disorders,0) as cms_alcohol_use_disorders
,coalesce(cms_anemia,0) as cms_anemia
,coalesce(cms_asthma,0) as cms_asthma
,coalesce(cms_atrial_fibrillation_and_flutter,0) as cms_atrial_fibrillation_and_flutter
,coalesce(cms_autism_spectrum_disorders,0) as cms_autism_spectrum_disorders
,coalesce(cms_benign_prostatic_hyperplasia,0) as cms_benign_prostatic_hyperplasia
,coalesce(cms_bipolar_disorder,0) as cms_bipolar_disorder
,coalesce(cms_cancer_breast,0) as cms_cancer_breast
,coalesce(cms_cancer_urologic_kidney_renal_pelvis_and_ureter,0) as cms_cancer_urologic_kidney_renal_pelvis_and_ureter
,coalesce(cms_cataract,0) as cms_cataract
,coalesce(cms_cerebral_palsy,0) as cms_cerebral_palsy
,coalesce(cms_chronic_kidney_disease,0) as cms_chronic_kidney_disease
,coalesce(cms_chronic_obstructive_pulmonary_disease,0) as cms_chronic_obstructive_pulmonary_disease
,coalesce(cms_depression_bipolar_or_other_depressive_mood_disorders,0) as cms_depression_bipolar_or_other_depressive_mood_disorders
,coalesce(cms_depressive_disorders,0) as cms_depressive_disorders
,coalesce(cms_diabetes,0) as cms_diabetes
,coalesce(cms_drug_use_disorders,0) as cms_drug_use_disorders
,coalesce(cms_epilepsy,0) as cms_epilepsy
,coalesce(cms_fibromyalgia_and_chronic_pain_and_fatigue,0) as cms_fibromyalgia_and_chronic_pain_and_fatigue
,coalesce(cms_glaucoma,0) as cms_glaucoma
,coalesce(cms_heart_failure_and_non_ischemic_heart_disease,0) as cms_heart_failure_and_non_ischemic_heart_disease
,coalesce(cms_hepatitis_a,0) as cms_hepatitis_a
,coalesce(cms_hepatitis_b_acute_or_unspecified,0) as cms_hepatitis_b_acute_or_unspecified
,coalesce(cms_hepatitis_c_acute,0) as cms_hepatitis_c_acute
,coalesce(cms_hepatitis_c_chronic,0) as cms_hepatitis_c_chronic
,coalesce(cms_hepatitis_c_unspecified,0) as cms_hepatitis_c_unspecified
,coalesce(cms_hepatitis_e,0) as cms_hepatitis_e
,coalesce(cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids,0) as cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids
,coalesce(cms_hypertension,0) as cms_hypertension
,coalesce(cms_ischemic_heart_disease,0) as cms_ischemic_heart_disease
,coalesce(cms_migraine_and_chronic_headache,0) as cms_migraine_and_chronic_headache
,coalesce(cms_muscular_dystrophy,0) as cms_muscular_dystrophy
,coalesce(cms_opioid_use_disorder_oud,0) as cms_opioid_use_disorder_oud
,coalesce(cms_learning_disabilities,0) as cms_learning_disabilities
,coalesce(cms_leukemias_and_lymphomas,0) as cms_leukemias_and_lymphomas
,coalesce(cms_parkinsons_disease_and_secondary_parkinsonism,0) as cms_parkinsons_disease_and_secondary_parkinsonism
,coalesce(cms_peripheral_vascular_disease_pvd,0) as cms_peripheral_vascular_disease_pvd
,coalesce(cms_personality_disorders,0) as cms_personality_disorders
,coalesce(cms_pneumonia_all_cause,0) as cms_pneumonia_all_cause
,coalesce(cms_sensory_blindness_and_visual_impairment,0) as cms_sensory_blindness_and_visual_impairment
,coalesce(cms_spinal_cord_injury,0) as cms_spinal_cord_injury
,coalesce(cms_alzheimers_disease,0) as cms_alzheimers_disease
,coalesce(cms_anxiety_disorders,0) as cms_anxiety_disorders
,coalesce(cms_cancer_colorectal,0) as cms_cancer_colorectal
,coalesce(cms_cancer_endometrial,0) as cms_cancer_endometrial
,coalesce(cms_cancer_prostate,0) as cms_cancer_prostate
,coalesce(cms_cystic_fibrosis_and_other_metabolic_developmental_disorders,0) as cms_cystic_fibrosis_and_other_metabolic_developmental_disorders
,coalesce(cms_hepatitis_b_chronic,0) as cms_hepatitis_b_chronic
,coalesce(cms_hepatitis_d,0) as cms_hepatitis_d
,coalesce(cms_hip_pelvic_fracture,0) as cms_hip_pelvic_fracture
,coalesce(cms_hyperlipidemia,0) as cms_hyperlipidemia
,coalesce(cms_intellectual_disabilities_and_related_conditions,0) as cms_intellectual_disabilities_and_related_conditions
,coalesce(cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis,0) as cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis
,coalesce(cms_non_alzheimers_dementia,0) as cms_non_alzheimers_dementia
,coalesce(cms_multiple_sclerosis_and_transverse_myelitis,0) as cms_multiple_sclerosis_and_transverse_myelitis
,coalesce(cms_osteoporosis_with_or_without_pathological_fracture,0) as cms_osteoporosis_with_or_without_pathological_fracture
,coalesce(cms_post_traumatic_stress_disorder_ptsd,0) as cms_post_traumatic_stress_disorder_ptsd
,coalesce(cms_rheumatoid_arthritis_osteoarthritis,0) as cms_rheumatoid_arthritis_osteoarthritis
,coalesce(cms_sensory_deafness_and_hearing_impairment,0) as cms_sensory_deafness_and_hearing_impairment
,coalesce(cms_sickle_cell_disease,0) as cms_sickle_cell_disease
,coalesce(cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system,0) as cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system
,coalesce(cms_stroke_transient_ischemic_attack,0) as cms_stroke_transient_ischemic_attack
,coalesce(cms_tobacco_use,0) as cms_tobacco_use
,coalesce(cms_viral_hepatitis_general,0) as cms_viral_hepatitis_general
,coalesce(cms_schizophrenia,0) as cms_schizophrenia
,coalesce(cms_cancer_lung,0) as cms_cancer_lung
,coalesce(cms_hypothyroidism,0) as cms_hypothyroidism
,coalesce(cms_mobility_impairments,0) as cms_mobility_impairments
,coalesce(cms_obesity,0) as cms_obesity
,coalesce(cms_other_developmental_delays,0) as cms_other_developmental_delays
,coalesce(cms_pressure_and_chronic_ulcers,0) as cms_pressure_and_chronic_ulcers
,coalesce(cms_schizophrenia_and_other_psychotic_disorders,0) as cms_schizophrenia_and_other_psychotic_disorders
,coalesce(cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage,0) as cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage


from member_month mm 
left join condition_flags f on mm.person_id = f.condition_person_id 
and
mm.year_nbr = f.condition_year_nbr
