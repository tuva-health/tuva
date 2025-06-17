{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

with final as (
select
    e.encounter_id
  , e.data_source
  , e.person_id
  , coalesce(p.sex,'unknown') as sex
  , c.year as year_nbr
  , e.length_of_stay
  , e.discharge_disposition_code
  , case 
      when rs.index_admission_flag = 1 
       and rs.unplanned_readmit_30_flag = 1 then 1 
      else 0 
    end as readmission_numerator
  , coalesce(rs.index_admission_flag,0) as readmission_denominator
  , coalesce(st_ab.ansi_fips_state_name,st_full.ansi_fips_state_name,'null_state') as state
  , case 
      when r.description is not null then r.description 
      else 'null_race'
    end as race
  , case when e.drg_code_type = 'ms-drg' then e.drg_code else 'null_drg' end as ms_drg_code
  , coalesce(ccsr.default_ccsr_category_description_ip, 'unknown') as ccsr_cat
  , datediff(year, p.birth_date, e.encounter_start_date) as age_at_admit_with_null
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
  /* tuva chronic conditions */
  , pc.hip_fracture as cond_hip_fracture
  , pc.type_1_diabetes_mellitus as cond_type_1_diabetes_mellitus
  , pc.no_chronic_conditions as cond_no_chronic_conditions
  , pc.invasive_pneumococcal_disease as cond_invasive_pneumococcal_disease
  , pc.acute_lymphoblastic_leukemia as cond_acute_lymphoblastic_leukemia
  , pc.pulmonary_embolism_thrombotic_or_unspecified as cond_pulmonary_embolism_thrombotic_or_unspecified
  , pc.alcohol_use_disorder as cond_alcohol_use_disorder
  , pc.haemophilus_influenzae_invasive_disease as cond_haemophilus_influenzae_invasive_disease
  , pc.alzheimer_disease as cond_alzheimer_disease
  , pc.lung_cancer_primary_or_unspecified as cond_lung_cancer_primary_or_unspecified
  , pc.anxiety_disorders as cond_anxiety_disorders
  , pc.osteoporosis as cond_osteoporosis
  , pc.asthma as cond_asthma
  , pc.st_louis_encephalitis_virus_disease as cond_st_louis_encephalitis_virus_disease
  , pc.atherosclerosis as cond_atherosclerosis
  , pc.western_equine_encephalitis_virus_disease as cond_western_equine_encephalitis_virus_disease
  , pc.atrial_fibrillation as cond_atrial_fibrillation
  , pc.abdominal_hernia as cond_abdominal_hernia
  , pc.hepatitis_c_infection_acute as cond_hepatitis_c_infection_acute
  , pc.attention_deficithyperactivity_disorder as cond_attention_deficithyperactivity_disorder
  , pc.leptospirosis as cond_leptospirosis
  , pc.benign_prostatic_hyperplasia as cond_benign_prostatic_hyperplasia
  , pc.multiple_myeloma as cond_multiple_myeloma
  , pc.bipolar_affective_disorder as cond_bipolar_affective_disorder
  , pc.opioid_use_disorder as cond_opioid_use_disorder
  , pc.botulism as cond_botulism
  , pc.parvovirus_infection as cond_parvovirus_infection
  , pc.botulism_foodborne as cond_botulism_foodborne
  , pc.rheumatoid_arthritis as cond_rheumatoid_arthritis
  , pc.botulism_wound as cond_botulism_wound
  , pc.stroke as cond_stroke
  , pc.breast_cancer as cond_breast_cancer
  , pc.ulcerative_colitis as cond_ulcerative_colitis
  , pc.cardiac_dysrhythmias as cond_cardiac_dysrhythmias
  , pc.glaucoma as cond_glaucoma
  , pc.cataract as cond_cataract
  , pc.heart_failure as cond_heart_failure
  , pc.chronic_kidney_disease as cond_chronic_kidney_disease
  , pc.herpes_simplex_infection as cond_herpes_simplex_infection
  , pc.chronic_obstructive_pulmonary_disease as cond_chronic_obstructive_pulmonary_disease
  , pc.hypertension as cond_hypertension
  , pc.clostridioides_difficile_enterocolitis as cond_clostridioides_difficile_enterocolitis
  , pc.legionellosis as cond_legionellosis
  , pc.colorectal_cancer as cond_colorectal_cancer
  , pc.listeriosis as cond_listeriosis
  , pc.covid19 as cond_covid19
  , pc.major_depressive_disorder as cond_major_depressive_disorder
  , pc.cryptosporidiosis as cond_cryptosporidiosis
  , pc.myocardial_infarction as cond_myocardial_infarction
  , pc.cytomegalovirus_infection as cond_cytomegalovirus_infection
  , pc.obesity as cond_obesity
  , pc.deep_vein_thrombosis_of_extremities_or_central_veins as cond_deep_vein_thrombosis_of_extremities_or_central_veins
  , pc.osteoarthritis as cond_osteoarthritis
  , pc.dementia as cond_dementia
  , pc.parkinsons_disease as cond_parkinsons_disease
  , pc.dexamethasone_systemic as cond_dexamethasone_systemic
  , pc.posttraumatic_stress_disorder as cond_posttraumatic_stress_disorder
  , pc.diabetes_mellitus as cond_diabetes_mellitus
  , pc.respiratory_syncytial_virus_infection as cond_respiratory_syncytial_virus_infection
  , pc.diphtheria as cond_diphtheria
  , pc.schizophrenia as cond_schizophrenia
  , pc.diverticulitis_of_large_intestine as cond_diverticulitis_of_large_intestine
  , pc.stem_cell_transplantation as cond_stem_cell_transplantation
  , pc.dyslipidemias as cond_dyslipidemias
  , pc.tobacco_use as cond_tobacco_use
  , pc.endocarditis as cond_endocarditis
  , pc.type_2_diabetes_mellitus as cond_type_2_diabetes_mellitus
  , pc.epilepsy_and_seizure_disorders as cond_epilepsy_and_seizure_disorders
  , pc.west_nile_virus_disease as cond_west_nile_virus_disease
  , pc.erectile_dysfunction as cond_erectile_dysfunction
  , pc.abdominal_aortic_aneurysm as cond_abdominal_aortic_aneurysm
  , pc.gastroesophageal_reflux as cond_gastroesophageal_reflux

/* cms conditions */
  , pcms.cms_acute_myocardial_infarction
  , pcms.cms_adhd_conduct_disorders_and_hyperkinetic_syndrome
  , pcms.cms_alcohol_use_disorders
  , pcms.cms_anemia
  , pcms.cms_asthma
  , pcms.cms_atrial_fibrillation_and_flutter
  , pcms.cms_autism_spectrum_disorders
  , pcms.cms_benign_prostatic_hyperplasia
  , pcms.cms_bipolar_disorder
  , pcms.cms_cancer_breast
  , pcms.cms_cancer_urologic_kidney_renal_pelvis_and_ureter
  , pcms.cms_cataract
  , pcms.cms_cerebral_palsy
  , pcms.cms_chronic_kidney_disease
  , pcms.cms_chronic_obstructive_pulmonary_disease
  , pcms.cms_depression_bipolar_or_other_depressive_mood_disorders
  , pcms.cms_depressive_disorders
  , pcms.cms_diabetes
  , pcms.cms_drug_use_disorders
  , pcms.cms_epilepsy
  , pcms.cms_fibromyalgia_and_chronic_pain_and_fatigue
  , pcms.cms_glaucoma
  , pcms.cms_heart_failure_and_non_ischemic_heart_disease
  , pcms.cms_hepatitis_a
  , pcms.cms_hepatitis_b_acute_or_unspecified
  , pcms.cms_hepatitis_c_acute
  , pcms.cms_hepatitis_c_chronic
  , pcms.cms_hepatitis_c_unspecified
  , pcms.cms_hepatitis_e
  , pcms.cms_human_immunodeficiency_virus_and_or_acquired_immunodeficiency_syndrome_hiv_aids
  , pcms.cms_hypertension
  , pcms.cms_ischemic_heart_disease
  , pcms.cms_migraine_and_chronic_headache
  , pcms.cms_muscular_dystrophy
  , pcms.cms_opioid_use_disorder_oud
  , pcms.cms_learning_disabilities
  , pcms.cms_leukemias_and_lymphomas
  , pcms.cms_parkinsons_disease_and_secondary_parkinsonism
  , pcms.cms_peripheral_vascular_disease_pvd
  , pcms.cms_personality_disorders
  , pcms.cms_pneumonia_all_cause
  , pcms.cms_sensory_blindness_and_visual_impairment
  , pcms.cms_spinal_cord_injury
  , pcms.cms_alzheimers_disease
  , pcms.cms_anxiety_disorders
  , pcms.cms_cancer_colorectal
  , pcms.cms_cancer_endometrial
  , pcms.cms_cancer_prostate
  , pcms.cms_cystic_fibrosis_and_other_metabolic_developmental_disorders
  , pcms.cms_hepatitis_b_chronic
  , pcms.cms_hepatitis_d
  , pcms.cms_hip_pelvic_fracture
  , pcms.cms_hyperlipidemia
  , pcms.cms_intellectual_disabilities_and_related_conditions
  , pcms.cms_liver_disease_cirrhosis_and_other_liver_conditions_except_viral_hepatitis
  , pcms.cms_non_alzheimers_dementia
  , pcms.cms_multiple_sclerosis_and_transverse_myelitis
  , pcms.cms_osteoporosis_with_or_without_pathological_fracture
  , pcms.cms_post_traumatic_stress_disorder_ptsd
  , pcms.cms_rheumatoid_arthritis_osteoarthritis
  , pcms.cms_sensory_deafness_and_hearing_impairment
  , pcms.cms_sickle_cell_disease
  , pcms.cms_spina_bifida_and_other_congenital_anomalies_of_the_nervous_system
  , pcms.cms_stroke_transient_ischemic_attack
  , pcms.cms_tobacco_use
  , pcms.cms_viral_hepatitis_general
  , pcms.cms_schizophrenia
  , pcms.cms_cancer_lung
  , pcms.cms_hypothyroidism
  , pcms.cms_mobility_impairments
  , pcms.cms_obesity
  , pcms.cms_other_developmental_delays
  , pcms.cms_pressure_and_chronic_ulcers
  , pcms.cms_schizophrenia_and_other_psychotic_disorders
  , pcms.cms_traumatic_brain_injury_and_nonpsychotic_mental_disorders_due_to_brain_damage


  /* hccs*/
  , phcc.hcc_1
  , phcc.hcc_2
  , phcc.hcc_6
  , phcc.hcc_8
  , phcc.hcc_9
  , phcc.hcc_10
  , phcc.hcc_11
  , phcc.hcc_12
  , phcc.hcc_17
  , phcc.hcc_18
  , phcc.hcc_19
  , phcc.hcc_21
  , phcc.hcc_22
  , phcc.hcc_23
  , phcc.hcc_27
  , phcc.hcc_28
  , phcc.hcc_29
  , phcc.hcc_33
  , phcc.hcc_34
  , phcc.hcc_35
  , phcc.hcc_39
  , phcc.hcc_40
  , phcc.hcc_46
  , phcc.hcc_47
  , phcc.hcc_48
  , phcc.hcc_51
  , phcc.hcc_52
  , phcc.hcc_54
  , phcc.hcc_55
  , phcc.hcc_56
  , phcc.hcc_57
  , phcc.hcc_58
  , phcc.hcc_59
  , phcc.hcc_60
  , phcc.hcc_70
  , phcc.hcc_71
  , phcc.hcc_72
  , phcc.hcc_73
  , phcc.hcc_74
  , phcc.hcc_75
  , phcc.hcc_76
  , phcc.hcc_77
  , phcc.hcc_78
  , phcc.hcc_79
  , phcc.hcc_80
  , phcc.hcc_82
  , phcc.hcc_83
  , phcc.hcc_84
  , phcc.hcc_85
  , phcc.hcc_86
  , phcc.hcc_87
  , phcc.hcc_88
  , phcc.hcc_96
  , phcc.hcc_99
  , phcc.hcc_100
  , phcc.hcc_103
  , phcc.hcc_104
  , phcc.hcc_106
  , phcc.hcc_107
  , phcc.hcc_108
  , phcc.hcc_110
  , phcc.hcc_111
  , phcc.hcc_112
  , phcc.hcc_114
  , phcc.hcc_115
  , phcc.hcc_122
  , phcc.hcc_124
  , phcc.hcc_134
  , phcc.hcc_135
  , phcc.hcc_136
  , phcc.hcc_137
  , phcc.hcc_138
  , phcc.hcc_157
  , phcc.hcc_158
  , phcc.hcc_159
  , phcc.hcc_161
  , phcc.hcc_162
  , phcc.hcc_166
  , phcc.hcc_167
  , phcc.hcc_169
  , phcc.hcc_170
  , phcc.hcc_173
  , phcc.hcc_176
  , phcc.hcc_186
  , phcc.hcc_188
  , phcc.hcc_189
   
from {{ ref('core__encounter')}} e
inner join {{ ref('reference_data__calendar')}} c on e.encounter_start_date = c.full_date
inner join {{ ref('core__patient')}} p on e.person_id = p.person_id
inner join {{ ref('benchmarks__pivot_condition') }} pc on e.person_id = pc.person_id 
  and
  c.year = pc.year_nbr
inner join {{ ref('benchmarks__pivot_cms_condition') }} pcms on e.person_id = pcms.person_id 
  and
  c.year = pcms.year_nbr
inner join {{ ref('benchmarks__pivot_hcc') }} phcc on e.person_id = phcc.person_id 
  and
  c.year = phcc.year_nbr  

left join {{ ref('ccsr__dxccsr_v2023_1_cleaned_map') }} ccsr on e.primary_diagnosis_code = ccsr.icd_10_cm_code
left join {{ ref('reference_data__ansi_fips_state')}} st_ab on p.state=st_ab.ansi_fips_state_abbreviation
left join {{ ref('reference_data__ansi_fips_state')}} st_full on p.state=st_full.ansi_fips_state_name
left join {{ ref('terminology__race')}} r on p.race = r.description
left join {{ ref('readmissions__readmission_summary')}} rs on e.encounter_id = rs.encounter_id
where e.encounter_type = 'acute inpatient'
)

,handle_missing_age as (
  select data_source
  ,avg(age_at_admit_with_null) as avg_age_at_admit
  from final
  group by data_source
)

, first_last as (
  select person_id
  ,payer
  ,{{ quote_column('plan') }}
  ,cast(left(year_month,4) as int) as year_nbr
  ,min(year_month) as first_month
  ,max(year_month) as last_month
  from {{ ref('core__member_months') }}
  group by 
  person_id
  ,payer
  ,cast(left(year_month,4) as int)
  ,{{ quote_column('plan') }}
)

select final.*
,coalesce(age_at_admit_with_null,h.avg_age_at_admit) as age_at_admit
,fl.first_month
,fl.last_month
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final
inner join handle_missing_age h on final.data_source = h.data_source
inner join first_last fl on final.person_id = fl.person_id 
  and
  final.{{ quote_column('plan') }} = fl.{{ quote_column('plan') }}
  and
  fl.year_nbr = final.year_nbr  