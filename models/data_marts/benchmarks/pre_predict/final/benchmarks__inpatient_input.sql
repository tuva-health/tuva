{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

with first_last as (
  select person_id
  , payer
  , data_source
  , {{ quote_column('plan') }}
  , cast(left(year_month, 4) as int) as year_nbr
  , min(year_month) as first_month
  , max(year_month) as last_month
  from {{ ref('benchmarks__stg_core__member_months') }}
  group by
  person_id
  , data_source
  , payer
  , cast(left(year_month, 4) as int)
  , {{ quote_column('plan') }}
)

, enrollment_fields as (
select distinct
 encounter_id
, payer
, {{ quote_column('plan') }}
, person_id
, data_source
from {{ ref('benchmarks__stg_core__medical_claim') }}
)

, enrollment_row as (
select encounter_id
, payer
, {{ quote_column('plan') }}
, person_id
, data_source
, row_number() over (partition by encounter_id
order by payer
                                                      , {{ quote_column('plan') }}
                                                      , person_id
                                                      , data_source
                                                      ) as row_num
from enrollment_fields
)

, final as (
select
    e.encounter_id
  , e.data_source
  , coalesce(p.sex, 'unknown') as sex
  , c.year as year_nbr
  , e.length_of_stay
  , e.discharge_disposition_code
  , case
      when rs.index_admission_flag = 1
       and rs.unplanned_readmit_30_flag = 1 then 1
      else 0
    end as readmission_numerator
  , coalesce(rs.index_admission_flag, 0) as readmission_denominator
  , coalesce(st_ab.ansi_fips_state_name, st_full.ansi_fips_state_name, 'null_state') as state
  , case
      when r.description is not null then r.description
      else 'null_race'
    end as race
  , case when e.drg_code_type = 'ms-drg' then e.drg_code else 'null_drg' end as ms_drg_code
  , coalesce(ccsr.default_ccsr_category_description_ip, 'unknown') as ccsr_cat
  , {{ datediff('p.birth_date', 'e.encounter_start_date', 'year') }} as age_at_admit_with_null
  , case
      when e.discharge_disposition_code = '01' then 'home'
      when e.discharge_disposition_code = '06' then 'home health'
      when e.discharge_disposition_code = '03' then 'snf'
      when e.discharge_disposition_code = '20' then 'expired'
      when e.discharge_disposition_code = '62' then 'ipt rehab'
      when e.discharge_disposition_code in ('02', '63', '13', '12', '05', '04', '61', '65', '64')
        then 'transfer/other facility'
      when e.discharge_disposition_code in ('50', '51') then 'hospice'
      else 'other'
    end as discharge_location

  , fl.first_month
  , fl.last_month
  /* tuva chronic conditions */
  , coalesce(pc.hip_fracture, 0) as cond_hip_fracture
  , coalesce(pc.type_1_diabetes_mellitus, 0) as cond_type_1_diabetes_mellitus
  , coalesce(pc.no_chronic_conditions, 0) as cond_no_chronic_conditions
  , coalesce(pc.invasive_pneumococcal_disease, 0) as cond_invasive_pneumococcal_disease
  , coalesce(pc.acute_lymphoblastic_leukemia, 0) as cond_acute_lymphoblastic_leukemia
  , coalesce(pc.pulmonary_embolism_thrombotic_or_unspecified, 0) as cond_pulmonary_embolism_thrombotic_or_unspecified
  , coalesce(pc.alcohol_use_disorder, 0) as cond_alcohol_use_disorder
  , coalesce(pc.haemophilus_influenzae_invasive_disease, 0) as cond_haemophilus_influenzae_invasive_disease
  , coalesce(pc.alzheimer_disease, 0) as cond_alzheimer_disease
  , coalesce(pc.lung_cancer_primary_or_unspecified, 0) as cond_lung_cancer_primary_or_unspecified
  , coalesce(pc.anxiety_disorders, 0) as cond_anxiety_disorders
  , coalesce(pc.osteoporosis, 0) as cond_osteoporosis
  , coalesce(pc.asthma, 0) as cond_asthma
  , coalesce(pc.st_louis_encephalitis_virus_disease, 0) as cond_st_louis_encephalitis_virus_disease
  , coalesce(pc.atherosclerosis, 0) as cond_atherosclerosis
  , coalesce(pc.western_equine_encephalitis_virus_disease, 0) as cond_western_equine_encephalitis_virus_disease
  , coalesce(pc.atrial_fibrillation, 0) as cond_atrial_fibrillation
  , coalesce(pc.abdominal_hernia, 0) as cond_abdominal_hernia
  , coalesce(pc.hepatitis_c_infection_acute, 0) as cond_hepatitis_c_infection_acute
  , coalesce(pc.attention_deficithyperactivity_disorder, 0) as cond_attention_deficithyperactivity_disorder
  , coalesce(pc.leptospirosis, 0) as cond_leptospirosis
  , coalesce(pc.benign_prostatic_hyperplasia, 0) as cond_benign_prostatic_hyperplasia
  , coalesce(pc.multiple_myeloma, 0) as cond_multiple_myeloma
  , coalesce(pc.bipolar_affective_disorder, 0) as cond_bipolar_affective_disorder
  , coalesce(pc.opioid_use_disorder, 0) as cond_opioid_use_disorder
  , coalesce(pc.botulism, 0) as cond_botulism
  , coalesce(pc.parvovirus_infection, 0) as cond_parvovirus_infection
  , coalesce(pc.botulism_foodborne, 0) as cond_botulism_foodborne
  , coalesce(pc.rheumatoid_arthritis, 0) as cond_rheumatoid_arthritis
  , coalesce(pc.botulism_wound, 0) as cond_botulism_wound
  , coalesce(pc.stroke, 0) as cond_stroke
  , coalesce(pc.breast_cancer, 0) as cond_breast_cancer
  , coalesce(pc.ulcerative_colitis, 0) as cond_ulcerative_colitis
  , coalesce(pc.cardiac_dysrhythmias, 0) as cond_cardiac_dysrhythmias
  , coalesce(pc.glaucoma, 0) as cond_glaucoma
  , coalesce(pc.cataract, 0) as cond_cataract
  , coalesce(pc.heart_failure, 0) as cond_heart_failure
  , coalesce(pc.chronic_kidney_disease, 0) as cond_chronic_kidney_disease
  , coalesce(pc.herpes_simplex_infection, 0) as cond_herpes_simplex_infection
  , coalesce(pc.chronic_obstructive_pulmonary_disease, 0) as cond_chronic_obstructive_pulmonary_disease
  , coalesce(pc.hypertension, 0) as cond_hypertension
  , coalesce(pc.clostridioides_difficile_enterocolitis, 0) as cond_clostridioides_difficile_enterocolitis
  , coalesce(pc.legionellosis, 0) as cond_legionellosis
  , coalesce(pc.colorectal_cancer, 0) as cond_colorectal_cancer
  , coalesce(pc.listeriosis, 0) as cond_listeriosis
  , coalesce(pc.covid19, 0) as cond_covid19
  , coalesce(pc.major_depressive_disorder, 0) as cond_major_depressive_disorder
  , coalesce(pc.cryptosporidiosis, 0) as cond_cryptosporidiosis
  , coalesce(pc.myocardial_infarction, 0) as cond_myocardial_infarction
  , coalesce(pc.cytomegalovirus_infection, 0) as cond_cytomegalovirus_infection
  , coalesce(pc.obesity, 0) as cond_obesity
  , coalesce(pc.deep_vein_thrombosis_of_extremities_or_central_veins, 0) as cond_deep_vein_thrombosis_of_extremities_or_central_veins
  , coalesce(pc.osteoarthritis, 0) as cond_osteoarthritis
  , coalesce(pc.dementia, 0) as cond_dementia
  , coalesce(pc.parkinsons_disease, 0) as cond_parkinsons_disease
  , coalesce(pc.dexamethasone_systemic, 0) as cond_dexamethasone_systemic
  , coalesce(pc.posttraumatic_stress_disorder, 0) as cond_posttraumatic_stress_disorder
  , coalesce(pc.diabetes_mellitus, 0) as cond_diabetes_mellitus
  , coalesce(pc.respiratory_syncytial_virus_infection, 0) as cond_respiratory_syncytial_virus_infection
  , coalesce(pc.diphtheria, 0) as cond_diphtheria
  , coalesce(pc.schizophrenia, 0) as cond_schizophrenia
  , coalesce(pc.diverticulitis_of_large_intestine, 0) as cond_diverticulitis_of_large_intestine
  , coalesce(pc.stem_cell_transplantation, 0) as cond_stem_cell_transplantation
  , coalesce(pc.dyslipidemias, 0) as cond_dyslipidemias
  , coalesce(pc.tobacco_use, 0) as cond_tobacco_use
  , coalesce(pc.endocarditis, 0) as cond_endocarditis
  , coalesce(pc.type_2_diabetes_mellitus, 0) as cond_type_2_diabetes_mellitus
  , coalesce(pc.epilepsy_and_seizure_disorders, 0) as cond_epilepsy_and_seizure_disorders
  , coalesce(pc.west_nile_virus_disease, 0) as cond_west_nile_virus_disease
  , coalesce(pc.erectile_dysfunction, 0) as cond_erectile_dysfunction
  , coalesce(pc.abdominal_aortic_aneurysm, 0) as cond_abdominal_aortic_aneurysm
  , coalesce(pc.gastroesophageal_reflux, 0) as cond_gastroesophageal_reflux

/* cms conditions */
  , coalesce(pcms.cms_acute_myocardial_infarction, 0) as cms_acute_myocardial_infarction
  , coalesce(pcms.cms_adhd_conduct_disorders_and_hyperkinetic_syndrome, 0) as cms_adhd_conduct_disorders_and_hyperkinetic_syndrome
  , coalesce(pcms.cms_alcohol_use_disorders, 0) as cms_alcohol_use_disorders
  , coalesce(pcms.cms_anemia, 0) as cms_anemia
  , coalesce(pcms.cms_asthma, 0) as cms_asthma
  , coalesce(pcms.cms_atrial_fibrillation_and_flutter, 0) as cms_atrial_fibrillation_and_flutter
  , coalesce(pcms.cms_autism_spectrum_disorders, 0) as cms_autism_spectrum_disorders
  , coalesce(pcms.cms_benign_prostatic_hyperplasia, 0) as cms_benign_prostatic_hyperplasia
  , coalesce(pcms.cms_bipolar_disorder, 0) as cms_bipolar_disorder
  , coalesce(pcms.cms_cancer_breast, 0) as cms_cancer_breast
  , coalesce(pcms.cms_cancer_urologic_kidney_renal_pelvis_and_ureter, 0) as cms_cancer_urologic_kidney_renal_pelvis_and_ureter
  , coalesce(pcms.cms_cataract, 0) as cms_cataract
  , coalesce(pcms.cms_cerebral_palsy, 0) as cms_cerebral_palsy
  , coalesce(pcms.cms_chronic_kidney_disease, 0) as cms_chronic_kidney_disease
  , coalesce(pcms.cms_chronic_obstructive_pulmonary_disease, 0) as cms_chronic_obstructive_pulmonary_disease
  , coalesce(pcms.cms_depression_bipolar_or_other_depressive_mood_disorders, 0) as cms_depression_bipolar_or_other_depressive_mood_disorders
  , coalesce(pcms.cms_depressive_disorders, 0) as cms_depressive_disorders
  , coalesce(pcms.cms_diabetes, 0) as cms_diabetes
  , coalesce(pcms.cms_drug_use_disorders, 0) as cms_drug_use_disorders
  , coalesce(pcms.cms_epilepsy, 0) as cms_epilepsy
  , coalesce(pcms.cms_fibromyalgia_and_chronic_pain_and_fatigue, 0) as cms_fibromyalgia_and_chronic_pain_and_fatigue
  , coalesce(pcms.cms_glaucoma, 0) as cms_glaucoma
  , coalesce(pcms.cms_heart_failure_and_non_ischemic_heart_disease, 0) as cms_heart_failure_and_non_ischemic_heart_disease
  , coalesce(pcms.cms_hepatitis_a, 0) as cms_hepatitis_a
  , coalesce(pcms.cms_hepatitis_b_acute_or_unspecified, 0) as cms_hepatitis_b_acute_or_unspecified
  , coalesce(pcms.cms_hepatitis_c_acute, 0) as cms_hepatitis_c_acute
  , coalesce(pcms.cms_hepatitis_c_chronic, 0) as cms_hepatitis_c_chronic
  , coalesce(pcms.cms_hepatitis_c_unspecified, 0) as cms_hepatitis_c_unspecified
  , coalesce(pcms.cms_hepatitis_e, 0) as cms_hepatitis_e
  , coalesce(pcms.cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids, 0) as cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids
  , coalesce(pcms.cms_hypertension, 0) as cms_hypertension
  , coalesce(pcms.cms_ischemic_heart_disease, 0) as cms_ischemic_heart_disease
  , coalesce(pcms.cms_migraine_and_chronic_headache, 0) as cms_migraine_and_chronic_headache
  , coalesce(pcms.cms_muscular_dystrophy, 0) as cms_muscular_dystrophy
  , coalesce(pcms.cms_opioid_use_disorder_oud, 0) as cms_opioid_use_disorder_oud
  , coalesce(pcms.cms_learning_disabilities, 0) as cms_learning_disabilities
  , coalesce(pcms.cms_leukemias_and_lymphomas, 0) as cms_leukemias_and_lymphomas
  , coalesce(pcms.cms_parkinsons_disease_and_secondary_parkinsonism, 0) as cms_parkinsons_disease_and_secondary_parkinsonism
  , coalesce(pcms.cms_peripheral_vascular_disease_pvd, 0) as cms_peripheral_vascular_disease_pvd
  , coalesce(pcms.cms_personality_disorders, 0) as cms_personality_disorders
  , coalesce(pcms.cms_pneumonia_all_cause, 0) as cms_pneumonia_all_cause
  , coalesce(pcms.cms_sensory_blindness_and_visual_impairment, 0) as cms_sensory_blindness_and_visual_impairment
  , coalesce(pcms.cms_spinal_cord_injury, 0) as cms_spinal_cord_injury
  , coalesce(pcms.cms_alzheimers_disease, 0) as cms_alzheimers_disease
  , coalesce(pcms.cms_anxiety_disorders, 0) as cms_anxiety_disorders
  , coalesce(pcms.cms_cancer_colorectal, 0) as cms_cancer_colorectal
  , coalesce(pcms.cms_cancer_endometrial, 0) as cms_cancer_endometrial
  , coalesce(pcms.cms_cancer_prostate, 0) as cms_cancer_prostate
  , coalesce(pcms.cms_cystic_fibrosis_and_other_metabolic_developmental_disorders, 0) as cms_cystic_fibrosis_and_other_metabolic_developmental_disorders
  , coalesce(pcms.cms_hepatitis_b_chronic, 0) as cms_hepatitis_b_chronic
  , coalesce(pcms.cms_hepatitis_d, 0) as cms_hepatitis_d
  , coalesce(pcms.cms_hip_pelvic_fracture, 0) as cms_hip_pelvic_fracture
  , coalesce(pcms.cms_hyperlipidemia, 0) as cms_hyperlipidemia
  , coalesce(pcms.cms_intellectual_disabilities_and_related_conditions, 0) as cms_intellectual_disabilities_and_related_conditions
  , coalesce(pcms.cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis, 0) as cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis
  , coalesce(pcms.cms_non_alzheimers_dementia, 0) as cms_non_alzheimers_dementia
  , coalesce(pcms.cms_multiple_sclerosis_and_transverse_myelitis, 0) as cms_multiple_sclerosis_and_transverse_myelitis
  , coalesce(pcms.cms_osteoporosis_with_or_without_pathological_fracture, 0) as cms_osteoporosis_with_or_without_pathological_fracture
  , coalesce(pcms.cms_post_traumatic_stress_disorder_ptsd, 0) as cms_post_traumatic_stress_disorder_ptsd
  , coalesce(pcms.cms_rheumatoid_arthritis_osteoarthritis, 0) as cms_rheumatoid_arthritis_osteoarthritis
  , coalesce(pcms.cms_sensory_deafness_and_hearing_impairment, 0) as cms_sensory_deafness_and_hearing_impairment
  , coalesce(pcms.cms_sickle_cell_disease, 0) as cms_sickle_cell_disease
  , coalesce(pcms.cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system, 0) as cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system
  , coalesce(pcms.cms_stroke_transient_ischemic_attack, 0) as cms_stroke_transient_ischemic_attack
  , coalesce(pcms.cms_tobacco_use, 0) as cms_tobacco_use
  , coalesce(pcms.cms_viral_hepatitis_general, 0) as cms_viral_hepatitis_general
  , coalesce(pcms.cms_schizophrenia, 0) as cms_schizophrenia
  , coalesce(pcms.cms_cancer_lung, 0) as cms_cancer_lung
  , coalesce(pcms.cms_hypothyroidism, 0) as cms_hypothyroidism
  , coalesce(pcms.cms_mobility_impairments, 0) as cms_mobility_impairments
  , coalesce(pcms.cms_obesity, 0) as cms_obesity
  , coalesce(pcms.cms_other_developmental_delays, 0) as cms_other_developmental_delays
  , coalesce(pcms.cms_pressure_and_chronic_ulcers, 0) as cms_pressure_and_chronic_ulcers
  , coalesce(pcms.cms_schizophrenia_and_other_psychotic_disorders, 0) as cms_schizophrenia_and_other_psychotic_disorders
  , coalesce(pcms.cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage, 0) as cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage


  /* hccs*/
  , coalesce(phcc.hcc_1, 0) as hcc_1
  , coalesce(phcc.hcc_2, 0) as hcc_2
  , coalesce(phcc.hcc_6, 0) as hcc_6
  , coalesce(phcc.hcc_8, 0) as hcc_8
  , coalesce(phcc.hcc_9, 0) as hcc_9
  , coalesce(phcc.hcc_10, 0) as hcc_10
  , coalesce(phcc.hcc_11, 0) as hcc_11
  , coalesce(phcc.hcc_12, 0) as hcc_12
  , coalesce(phcc.hcc_17, 0) as hcc_17
  , coalesce(phcc.hcc_18, 0) as hcc_18
  , coalesce(phcc.hcc_19, 0) as hcc_19
  , coalesce(phcc.hcc_21, 0) as hcc_21
  , coalesce(phcc.hcc_22, 0) as hcc_22
  , coalesce(phcc.hcc_23, 0) as hcc_23
  , coalesce(phcc.hcc_27, 0) as hcc_27
  , coalesce(phcc.hcc_28, 0) as hcc_28
  , coalesce(phcc.hcc_29, 0) as hcc_29
  , coalesce(phcc.hcc_33, 0) as hcc_33
  , coalesce(phcc.hcc_34, 0) as hcc_34
  , coalesce(phcc.hcc_35, 0) as hcc_35
  , coalesce(phcc.hcc_39, 0) as hcc_39
  , coalesce(phcc.hcc_40, 0) as hcc_40
  , coalesce(phcc.hcc_46, 0) as hcc_46
  , coalesce(phcc.hcc_47, 0) as hcc_47
  , coalesce(phcc.hcc_48, 0) as hcc_48
  , coalesce(phcc.hcc_51, 0) as hcc_51
  , coalesce(phcc.hcc_52, 0) as hcc_52
  , coalesce(phcc.hcc_54, 0) as hcc_54
  , coalesce(phcc.hcc_55, 0) as hcc_55
  , coalesce(phcc.hcc_56, 0) as hcc_56
  , coalesce(phcc.hcc_57, 0) as hcc_57
  , coalesce(phcc.hcc_58, 0) as hcc_58
  , coalesce(phcc.hcc_59, 0) as hcc_59
  , coalesce(phcc.hcc_60, 0) as hcc_60
  , coalesce(phcc.hcc_70, 0) as hcc_70
  , coalesce(phcc.hcc_71, 0) as hcc_71
  , coalesce(phcc.hcc_72, 0) as hcc_72
  , coalesce(phcc.hcc_73, 0) as hcc_73
  , coalesce(phcc.hcc_74, 0) as hcc_74
  , coalesce(phcc.hcc_75, 0) as hcc_75
  , coalesce(phcc.hcc_76, 0) as hcc_76
  , coalesce(phcc.hcc_77, 0) as hcc_77
  , coalesce(phcc.hcc_78, 0) as hcc_78
  , coalesce(phcc.hcc_79, 0) as hcc_79
  , coalesce(phcc.hcc_80, 0) as hcc_80
  , coalesce(phcc.hcc_82, 0) as hcc_82
  , coalesce(phcc.hcc_83, 0) as hcc_83
  , coalesce(phcc.hcc_84, 0) as hcc_84
  , coalesce(phcc.hcc_85, 0) as hcc_85
  , coalesce(phcc.hcc_86, 0) as hcc_86
  , coalesce(phcc.hcc_87, 0) as hcc_87
  , coalesce(phcc.hcc_88, 0) as hcc_88
  , coalesce(phcc.hcc_96, 0) as hcc_96
  , coalesce(phcc.hcc_99, 0) as hcc_99
  , coalesce(phcc.hcc_100, 0) as hcc_100
  , coalesce(phcc.hcc_103, 0) as hcc_103
  , coalesce(phcc.hcc_104, 0) as hcc_104
  , coalesce(phcc.hcc_106, 0) as hcc_106
  , coalesce(phcc.hcc_107, 0) as hcc_107
  , coalesce(phcc.hcc_108, 0) as hcc_108
  , coalesce(phcc.hcc_110, 0) as hcc_110
  , coalesce(phcc.hcc_111, 0) as hcc_111
  , coalesce(phcc.hcc_112, 0) as hcc_112
  , coalesce(phcc.hcc_114, 0) as hcc_114
  , coalesce(phcc.hcc_115, 0) as hcc_115
  , coalesce(phcc.hcc_122, 0) as hcc_122
  , coalesce(phcc.hcc_124, 0) as hcc_124
  , coalesce(phcc.hcc_134, 0) as hcc_134
  , coalesce(phcc.hcc_135, 0) as hcc_135
  , coalesce(phcc.hcc_136, 0) as hcc_136
  , coalesce(phcc.hcc_137, 0) as hcc_137
  , coalesce(phcc.hcc_138, 0) as hcc_138
  , coalesce(phcc.hcc_157, 0) as hcc_157
  , coalesce(phcc.hcc_158, 0) as hcc_158
  , coalesce(phcc.hcc_159, 0) as hcc_159
  , coalesce(phcc.hcc_161, 0) as hcc_161
  , coalesce(phcc.hcc_162, 0) as hcc_162
  , coalesce(phcc.hcc_166, 0) as hcc_166
  , coalesce(phcc.hcc_167, 0) as hcc_167
  , coalesce(phcc.hcc_169, 0) as hcc_169
  , coalesce(phcc.hcc_170, 0) as hcc_170
  , coalesce(phcc.hcc_173, 0) as hcc_173
  , coalesce(phcc.hcc_176, 0) as hcc_176
  , coalesce(phcc.hcc_186, 0) as hcc_186
  , coalesce(phcc.hcc_188, 0) as hcc_188
  , coalesce(phcc.hcc_189, 0) as hcc_189

from {{ ref('benchmarks__stg_core__encounter') }} as e
inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as c on e.encounter_start_date = c.full_date
inner join {{ ref('benchmarks__stg_core__patient') }} as p on e.person_id = p.person_id
left outer join {{ ref('benchmarks__pivot_condition') }} as pc on e.person_id = pc.person_id
  and
  c.year = pc.year_nbr
left outer join {{ ref('benchmarks__pivot_cms_condition') }} as pcms on e.person_id = pcms.person_id
  and
  c.year = pcms.year_nbr
left outer join {{ ref('benchmarks__pivot_hcc') }} as phcc on e.person_id = phcc.person_id
  and
  c.year = phcc.year_nbr
inner join enrollment_row as er on e.encounter_id = er.encounter_id
  and
  er.row_num = 1
inner join first_last as fl on e.person_id = fl.person_id
  and
  er.{{ quote_column('plan') }} = fl.{{ quote_column('plan') }}
  and
  er.payer = fl.payer
  and
  er.data_source = fl.data_source
  and
  fl.year_nbr = c.year
left outer join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} as ccsr on e.primary_diagnosis_code = ccsr.icd_10_cm_code
left outer join {{ ref('reference_data__ansi_fips_state') }} as st_ab on p.state = st_ab.ansi_fips_state_abbreviation
left outer join {{ ref('reference_data__ansi_fips_state') }} as st_full on p.state = st_full.ansi_fips_state_name
left outer join {{ ref('terminology__race') }} as r on p.race = r.description
left outer join {{ ref('readmissions__readmission_summary') }} as rs on e.encounter_id = rs.encounter_id
where e.encounter_type = 'acute inpatient'
)

, handle_missing_age as (
  select data_source
  , avg(age_at_admit_with_null) as avg_age_at_admit
  from final
  group by data_source
)

select final.*
, coalesce(age_at_admit_with_null, h.avg_age_at_admit) as age_at_admit
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from final
inner join handle_missing_age as h on final.data_source = h.data_source
