with subset as (
  select distinct person_id
  from {{ ref('core__member_months') }}
)

, encounters as (
    select
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year as year_nbr
      , sum(mc.paid_amount) as paid_amount
      , count(distinct e.encounter_id) as encounter_count
      , count(distinct case when e.encounter_type = 'outpatient injections' then e.encounter_id else null end) as outpatient_injections_count
      , count(distinct case when e.encounter_type = 'emergency department' then e.encounter_id else null end) as emergency_department_count
      , count(distinct case when e.encounter_type = 'outpatient radiology' then e.encounter_id else null end) as outpatient_radiology_count
      , count(distinct case when e.encounter_type = 'outpatient pt/ot/st' then e.encounter_id else null end) as outpatient_pt_ot_st_count
      , count(distinct case when e.encounter_type = 'outpatient hospice' then e.encounter_id else null end) as outpatient_hospice_count
      , count(distinct case when e.encounter_type = 'urgent care' then e.encounter_id else null end) as urgent_care_count
      , count(distinct case when e.encounter_type = 'outpatient hospital or clinic' then e.encounter_id else null end) as outpatient_hospital_or_clinic_count
      , count(distinct case when e.encounter_type = 'home health' then e.encounter_id else null end) as home_health_count
      , count(distinct case when e.encounter_type = 'dialysis' then e.encounter_id else null end) as dialysis_count
      , count(distinct case when e.encounter_type = 'outpatient rehabilitation' then e.encounter_id else null end) as outpatient_rehabilitation_count
      , count(distinct case when e.encounter_type = 'outpatient surgery' then e.encounter_id else null end) as outpatient_surgery_count
      , count(distinct case when e.encounter_type = 'ambulatory surgery center' then e.encounter_id else null end) as ambulatory_surgery_center_count
      , count(distinct case when e.encounter_type = 'outpatient psych' then e.encounter_id else null end) as outpatient_psych_count
      , count(distinct case when e.encounter_type = 'dme - orphaned' then e.encounter_id else null end) as dme_orphaned_count
      , count(distinct case when e.encounter_type = 'orphaned claim' then e.encounter_id else null end) as orphaned_claim_count
      , count(distinct case when e.encounter_type = 'ambulance - orphaned' then e.encounter_id else null end) as ambulance_orphaned_count
      , count(distinct case when e.encounter_type = 'lab - orphaned' then e.encounter_id else null end) as lab_orphaned_count
      , count(distinct case when e.encounter_type = 'office visit radiology' then e.encounter_id else null end) as office_visit_radiology_count
      , count(distinct case when e.encounter_type = 'office visit' then e.encounter_id else null end) as office_visit_count
      , count(distinct case when e.encounter_type = 'office visit surgery' then e.encounter_id else null end) as office_visit_surgery_count
      , count(distinct case when e.encounter_type = 'office visit - other' then e.encounter_id else null end) as office_visit_other_count
      , count(distinct case when e.encounter_type = 'telehealth' then e.encounter_id else null end) as telehealth_count
      , count(distinct case when e.encounter_type = 'office visit pt/ot/st' then e.encounter_id else null end) as office_visit_pt_ot_st_count
      , count(distinct case when e.encounter_type = 'office visit injections' then e.encounter_id else null end) as office_visit_injections_count
      , count(distinct case when e.encounter_type = 'acute inpatient' then e.encounter_id else null end) as acute_inpatient_count
      , count(distinct case when e.encounter_type = 'inpatient hospice' then e.encounter_id else null end) as inpatient_hospice_count
      , count(distinct case when e.encounter_type = 'inpatient psych' then e.encounter_id else null end) as inpatient_psych_count
      , count(distinct case when e.encounter_type = 'inpatient rehabilitation' then e.encounter_id else null end) as inpatient_rehabilitation_count
      , count(distinct case when e.encounter_type = 'inpatient skilled nursing' then e.encounter_id else null end) as inpatient_skilled_nursing_count


  , count(distinct case when e.encounter_group = 'inpatient' then e.encounter_id else null end) as inpatient_count
  , count(distinct case when e.encounter_group = 'office based' then e.encounter_id else null end) as office_based_count
  , count(distinct case when e.encounter_group = 'other' then e.encounter_id else null end) as other_count
  , count(distinct case when e.encounter_group = 'outpatient' then e.encounter_id else null end) as outpatient_count


      , sum( case when e.encounter_type = 'outpatient injections' then mc.paid_amount else 0 end) as outpatient_injections_paid
      , sum( case when e.encounter_type = 'emergency department' then mc.paid_amount else 0 end) as emergency_department_paid
      , sum( case when e.encounter_type = 'outpatient radiology' then mc.paid_amount else 0 end) as outpatient_radiology_paid
      , sum( case when e.encounter_type = 'outpatient pt/ot/st' then mc.paid_amount else 0 end) as outpatient_pt_ot_st_paid
      , sum( case when e.encounter_type = 'outpatient hospice' then mc.paid_amount else 0 end) as outpatient_hospice_paid
      , sum( case when e.encounter_type = 'urgent care' then mc.paid_amount else 0 end) as urgent_care_paid
      , sum( case when e.encounter_type = 'outpatient hospital or clinic' then mc.paid_amount else 0 end) as outpatient_hospital_or_clinic_paid
      , sum( case when e.encounter_type = 'home health' then mc.paid_amount else 0 end) as home_health_paid
      , sum( case when e.encounter_type = 'dialysis' then mc.paid_amount else 0 end) as dialysis_paid
      , sum( case when e.encounter_type = 'outpatient rehabilitation' then mc.paid_amount else 0 end) as outpatient_rehabilitation_paid
      , sum( case when e.encounter_type = 'outpatient surgery' then mc.paid_amount else 0 end) as outpatient_surgery_paid
      , sum( case when e.encounter_type = 'ambulatory surgery center' then mc.paid_amount else 0 end) as ambulatory_surgery_center_paid
      , sum( case when e.encounter_type = 'outpatient psych' then mc.paid_amount else 0 end) as outpatient_psych_paid
      , sum( case when e.encounter_type = 'dme - orphaned' then mc.paid_amount else 0 end) as dme_orphaned_paid
      , sum( case when e.encounter_type = 'orphaned claim' then mc.paid_amount else 0 end) as orphaned_claim_paid
      , sum( case when e.encounter_type = 'ambulance - orphaned' then mc.paid_amount else 0 end) as ambulance_orphaned_paid
      , sum( case when e.encounter_type = 'lab - orphaned' then mc.paid_amount else 0 end) as lab_orphaned_paid
      , sum( case when e.encounter_type = 'office visit radiology' then mc.paid_amount else 0 end) as office_visit_radiology_paid
      , sum( case when e.encounter_type = 'office visit' then mc.paid_amount else 0 end) as office_visit_paid
      , sum( case when e.encounter_type = 'office visit surgery' then mc.paid_amount else 0 end) as office_visit_surgery_paid
      , sum( case when e.encounter_type = 'office visit - other' then mc.paid_amount else 0 end) as office_visit_other_paid
      , sum( case when e.encounter_type = 'telehealth' then mc.paid_amount else 0 end) as telehealth_paid
      , sum( case when e.encounter_type = 'office visit pt/ot/st' then mc.paid_amount else 0 end) as office_visit_pt_ot_st_paid
      , sum( case when e.encounter_type = 'office visit injections' then mc.paid_amount else 0 end) as office_visit_injections_paid
      , sum( case when e.encounter_type = 'acute inpatient' then mc.paid_amount else 0 end) as acute_inpatient_paid
      , sum( case when e.encounter_type = 'inpatient hospice' then mc.paid_amount else 0 end) as inpatient_hospice_paid
      , sum( case when e.encounter_type = 'inpatient psych' then mc.paid_amount else 0 end) as inpatient_psych_paid
      , sum( case when e.encounter_type = 'inpatient rehabilitation' then mc.paid_amount else 0 end) as inpatient_rehabilitation_paid
      , sum( case when e.encounter_type = 'inpatient skilled nursing' then mc.paid_amount else 0 end) as inpatient_skilled_nursing_paid


  , sum( case when e.encounter_group = 'inpatient' then mc.paid_amount else 0 end) as inpatient_paid
  , sum( case when e.encounter_group = 'office based' then mc.paid_amount else 0 end) as office_based_paid
  , sum( case when e.encounter_group = 'other' then mc.paid_amount else 0 end) as other_paid
  , sum( case when e.encounter_group = 'outpatient' then mc.paid_amount else 0 end) as outpatient_paid



    from {{ ref('core__medical_claim') }} as mc
    inner join subset on mc.person_id = subset.person_id
    inner join {{ ref('core__encounter') }} as e 
      on e.encounter_id = mc.encounter_id
    inner join {{ ref('reference_data__calendar') }} as cal 
      on e.encounter_start_date = cal.full_date
    inner join {{ ref('core__member_months') }} as mm 
      on mc.person_id = mm.person_id
      and mc.data_source = mm.data_source
      and mc.payer = mm.payer
      and mc.{{ quote_column('plan') }} = mm.{{ quote_column('plan') }}
      and cal.year_month_int = mm.year_month
    group by
        e.person_id
      , e.data_source
      , mc.payer
      , mc.{{ quote_column('plan') }}
      , cal.year 
)

,member_month as (
  select person_id
  ,payer
  ,{{ quote_column('plan') }}
  ,data_source
  ,left(year_month,4) as year_nbr
  ,count(year_month) as member_month_count
  from {{ ref('core__member_months') }} as mm
  group by 
  person_id
  ,payer
  ,{{ quote_column('plan') }}
  ,data_source
  ,left(year_month,4) 
)

,state_cte as (
  select distinct ssa_fips_state_name as state_nm
    from {{ ref('reference_data__ssa_fips_state')}} s
  where cast(s.ssa_fips_state_code as int) < 53
)

select
  row_number() over (order by mm.person_id, mm.year_nbr)  as benchmark_key
  , cast(mm.year_nbr as int) as year_nbr
  , mm.person_id
  , mm.payer
  , mm.{{ quote_column('plan') }}
  , mm.data_source
  , mm.member_month_count
  , DATEDIFF(
    YEAR,
    p.birth_date,
    CAST(CONCAT(mm.year_nbr, '-01-01') AS DATE)
  ) AS age_at_year_start
  , case when st.state_nm is not null then st.state_nm else null end as state --values that don't match are null for xgboost
  , case when r.description is not null then r.description else null end as race --values that don't match are null for xgboost
  , case when e.paid_amount < 0 then 0 else coalesce(e.paid_amount, 0) end as paid_amount

, case when e.outpatient_paid < 0 then 0 else coalesce(e.outpatient_paid, 0) end as outpatient_paid_amount
, case when e.other_paid < 0 then 0 else coalesce(e.other_paid, 0) end as other_paid_amount
, case when e.office_based_paid < 0 then 0 else coalesce(e.office_based_paid, 0) end as office_based_paid_amount
, case when e.inpatient_paid < 0 then 0 else coalesce(e.inpatient_paid, 0) end as inpatient_paid_amount
 
, case when e.outpatient_injections_paid < 0 then 0 else coalesce(e.outpatient_injections_paid, 0) end as outpatient_injections_paid_amount
, case when e.emergency_department_paid < 0 then 0 else coalesce(e.emergency_department_paid, 0) end as emergency_department_paid_amount
, case when e.outpatient_radiology_paid < 0 then 0 else coalesce(e.outpatient_radiology_paid, 0) end as outpatient_radiology_paid_amount
, case when e.outpatient_pt_ot_st_paid < 0 then 0 else coalesce(e.outpatient_pt_ot_st_paid, 0) end as outpatient_pt_ot_st_paid_amount
, case when e.outpatient_hospice_paid < 0 then 0 else coalesce(e.outpatient_hospice_paid, 0) end as outpatient_hospice_paid_amount
, case when e.urgent_care_paid < 0 then 0 else coalesce(e.urgent_care_paid, 0) end as urgent_care_paid_amount
, case when e.outpatient_hospital_or_clinic_paid < 0 then 0 else coalesce(e.outpatient_hospital_or_clinic_paid, 0) end as outpatient_hospital_or_clinic_paid_amount
, case when e.home_health_paid < 0 then 0 else coalesce(e.home_health_paid, 0) end as home_health_paid_amount
, case when e.dialysis_paid < 0 then 0 else coalesce(e.dialysis_paid, 0) end as dialysis_paid_amount
, case when e.outpatient_rehabilitation_paid < 0 then 0 else coalesce(e.outpatient_rehabilitation_paid, 0) end as outpatient_rehabilitation_paid_amount
, case when e.outpatient_surgery_paid < 0 then 0 else coalesce(e.outpatient_surgery_paid, 0) end as outpatient_surgery_paid_amount
, case when e.ambulatory_surgery_center_paid < 0 then 0 else coalesce(e.ambulatory_surgery_center_paid, 0) end as ambulatory_surgery_center_paid_amount
, case when e.outpatient_psych_paid < 0 then 0 else coalesce(e.outpatient_psych_paid, 0) end as outpatient_psych_paid_amount
, case when e.dme_orphaned_paid < 0 then 0 else coalesce(e.dme_orphaned_paid, 0) end as dme_orphaned_paid_amount
, case when e.orphaned_claim_paid < 0 then 0 else coalesce(e.orphaned_claim_paid, 0) end as orphaned_claim_paid_amount
, case when e.ambulance_orphaned_paid < 0 then 0 else coalesce(e.ambulance_orphaned_paid, 0) end as ambulance_orphaned_paid_amount
, case when e.lab_orphaned_paid < 0 then 0 else coalesce(e.lab_orphaned_paid, 0) end as lab_orphaned_paid_amount
, case when e.office_visit_radiology_paid < 0 then 0 else coalesce(e.office_visit_radiology_paid, 0) end as office_visit_radiology_paid_amount
, case when e.office_visit_paid < 0 then 0 else coalesce(e.office_visit_paid, 0) end as office_visit_paid_amount
, case when e.office_visit_surgery_paid < 0 then 0 else coalesce(e.office_visit_surgery_paid, 0) end as office_visit_surgery_paid_amount
, case when e.office_visit_other_paid < 0 then 0 else coalesce(e.office_visit_other_paid, 0) end as office_visit_other_paid_amount
, case when e.telehealth_paid < 0 then 0 else coalesce(e.telehealth_paid, 0) end as telehealth_paid_amount
, case when e.office_visit_pt_ot_st_paid < 0 then 0 else coalesce(e.office_visit_pt_ot_st_paid, 0) end as office_visit_pt_ot_st_paid_amount
, case when e.office_visit_injections_paid < 0 then 0 else coalesce(e.office_visit_injections_paid, 0) end as office_visit_injections_paid_amount
, case when e.acute_inpatient_paid < 0 then 0 else coalesce(e.acute_inpatient_paid, 0) end as acute_inpatient_paid_amount
, case when e.inpatient_hospice_paid < 0 then 0 else coalesce(e.inpatient_hospice_paid, 0) end as inpatient_hospice_paid_amount
, case when e.inpatient_psych_paid < 0 then 0 else coalesce(e.inpatient_psych_paid, 0) end as inpatient_psych_paid_amount
, case when e.inpatient_rehabilitation_paid < 0 then 0 else coalesce(e.inpatient_rehabilitation_paid, 0) end as inpatient_rehabilitation_paid_amount
, case when e.inpatient_skilled_nursing_paid < 0 then 0 else coalesce(e.inpatient_skilled_nursing_paid, 0) end as inpatient_skilled_nursing_paid_amount

, coalesce(e.outpatient_count, 0) as outpatient_count
, coalesce(e.other_count, 0) as other_count
, coalesce(e.office_based_count, 0) as office_based_count
, coalesce(e.inpatient_count, 0) as inpatient_count

, coalesce(e.outpatient_injections_count, 0) as outpatient_injections_count
, coalesce(e.emergency_department_count, 0) as emergency_department_count
, coalesce(e.outpatient_radiology_count, 0) as outpatient_radiology_count
, coalesce(e.outpatient_pt_ot_st_count, 0) as outpatient_pt_ot_st_count
, coalesce(e.outpatient_hospice_count, 0) as outpatient_hospice_count
, coalesce(e.urgent_care_count, 0) as urgent_care_count
, coalesce(e.outpatient_hospital_or_clinic_count, 0) as outpatient_hospital_or_clinic_count
, coalesce(e.home_health_count, 0) as home_health_count
, coalesce(e.dialysis_count, 0) as dialysis_count
, coalesce(e.outpatient_rehabilitation_count, 0) as outpatient_rehabilitation_count
, coalesce(e.outpatient_surgery_count, 0) as outpatient_surgery_count
, coalesce(e.ambulatory_surgery_center_count, 0) as ambulatory_surgery_center_count
, coalesce(e.outpatient_psych_count, 0) as outpatient_psych_count
, coalesce(e.dme_orphaned_count, 0) as dme_orphaned_count
, coalesce(e.orphaned_claim_count, 0) as orphaned_claim_count
, coalesce(e.ambulance_orphaned_count, 0) as ambulance_orphaned_count
, coalesce(e.lab_orphaned_count, 0) as lab_orphaned_count
, coalesce(e.office_visit_radiology_count, 0) as office_visit_radiology_count
, coalesce(e.office_visit_count, 0) as office_visit_count
, coalesce(e.office_visit_surgery_count, 0) as office_visit_surgery_count
, coalesce(e.office_visit_other_count, 0) as office_visit_other_count
, coalesce(e.telehealth_count, 0) as telehealth_count
, coalesce(e.office_visit_pt_ot_st_count, 0) as office_visit_pt_ot_st_count
, coalesce(e.office_visit_injections_count, 0) as office_visit_injections_count
, coalesce(e.acute_inpatient_count, 0) as acute_inpatient_count
, coalesce(e.inpatient_hospice_count, 0) as inpatient_hospice_count
, coalesce(e.inpatient_psych_count, 0) as inpatient_psych_count
, coalesce(e.inpatient_rehabilitation_count, 0) as inpatient_rehabilitation_count
, coalesce(e.inpatient_skilled_nursing_count, 0) as inpatient_skilled_nursing_count



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
 
from member_month as mm
inner join subset on mm.person_id = subset.person_id
inner join {{ ref('core__patient') }} as p 
  on mm.person_id = p.person_id
left join state_cte st on p.state = st.state_nm
inner join {{ ref('benchmarks__pivot_condition') }} pc on mm.person_id = pc.person_id 
  and
  pc.year_nbr = mm.year_nbr
inner join {{ ref('benchmarks__pivot_cms_condition') }} pcms on mm.person_id = pcms.person_id 
  and
  pcms.year_nbr = mm.year_nbr
inner join {{ ref('benchmarks__pivot_hcc') }} phcc on mm.person_id = phcc.person_id 
  and
  phcc.year_nbr = mm.year_nbr
left join encounters as e 
  on mm.person_id = e.person_id
  and mm.data_source = e.data_source
  and mm.{{ quote_column('plan') }} = e.{{ quote_column('plan') }}
  and mm.payer = e.payer
  and mm.year_nbr = e.year_nbr
left join {{ ref('terminology__race')}} r on p.race = r.description
