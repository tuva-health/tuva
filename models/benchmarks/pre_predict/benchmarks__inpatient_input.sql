{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

with cte as (
  select distinct ssa_fips_state_name as state_nm
    from {{ ref('reference_data__ssa_fips_state')}} s
  where cast(s.ssa_fips_state_code as int) < 53
)

select
    e.encounter_id
  , e.length_of_stay
  , e.discharge_disposition_code
  , case 
      when rs.index_admission_flag = 1 
       and rs.unplanned_readmit_30_flag = 1 then 1 
      else 0 
    end as readmission_numerator
  , coalesce(rs.index_admission_flag,0) as readmission_denominator
  , case 
      when s.state_nm is not null then s.state_nm 
      else 'other' 
    end as state
  , case 
      when r.description is not null then r.description 
      else 'unknown' 
    end as race
  , case when drg_code_type = 'ms-drg' then drg_code else null end as ms_drg_code
  , coalesce(ccsr.default_ccsr_category_description_ip, 'unknown') as ccsr_cat
  , datediff(year, p.birth_date, e.encounter_start_date) as age_at_admit
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
from core.encounter e
inner join core.patient p on e.person_id = p.person_id
inner join {{ ref('benchmarks__pivot_condition') }} pc on e.person_id = pc.person_id 
  and
  year(e.encounter_start_date) = pc.year_nbr
left join ccsr._value_set_dxccsr_v2023_1_cleaned_map ccsr on e.primary_diagnosis_code = ccsr.icd_10_cm_code
left join cte s on p.state = s.state_nm
left join {{ ref('terminology__race')}} r on p.race = r.description
left join {{ ref('readmissions__readmission_summary')}} rs on e.encounter_id = rs.encounter_id
where e.encounter_type = 'acute inpatient'