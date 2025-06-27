{{
    config(
        enabled = var('benchmarks_train', False) | as_bool
    )
}}

/* returns person year grain. Did the patient have the condition coded in each year? Not if they ever had the condition */
with cte as (
    select distinct
        a.person_id
        , cal.year as year_nbr
    {% if target.type == 'bigquery' %}
        -- BigQuery syntax: Use "'" to represent a single quote character in a string
        , replace(replace(replace(replace(lower(c.concept_name), "'", ''), '.', ''), '-', ''), ' ', '_') as cleaned_concept_name
    {% else %}
        -- Standard SQL syntax: Use '''' to represent a single quote
        , replace(replace(replace(replace(lower(c.concept_name), '''', ''), '.', ''), '-', ''), ' ', '_') as cleaned_concept_name --noqa
    {% endif %}
    from {{ ref('benchmarks__stg_core__condition') }} as a
    inner join {{ ref('clinical_concept_library__value_set_member_relevant_fields') }} as c
        on a.normalized_code = c.code
    inner join {{ ref('benchmarks__stg_reference_data__calendar') }} as cal
        on a.recorded_date = cal.full_date
)


, member_month as (
    select mm.person_id
    , cast(left(year_month, 4) as {{ dbt.type_int() }}) as year_nbr
    , count(year_month) as member_month_count
    from {{ ref('benchmarks__stg_core__member_months') }} as mm
    group by mm.person_id
    , cast(left(year_month, 4) as {{ dbt.type_int() }})
)

/* top 25 by pmpm and top 50 prevelance in CMS data */
, condition_flags as (
select
 person_id as condition_person_id
, year_nbr as condition_year_nbr
, max(case when cleaned_concept_name = 'hip_fracture' then 1 else 0 end) as hip_fracture
, max(case when cleaned_concept_name = 'type_1_diabetes_mellitus' then 1 else 0 end) as type_1_diabetes_mellitus
, max(case when cleaned_concept_name = 'no_chronic_conditions' then 1 else 0 end) as no_chronic_conditions
, max(case when cleaned_concept_name = 'invasive_pneumococcal_disease' then 1 else 0 end) as invasive_pneumococcal_disease
, max(case when cleaned_concept_name = 'acute_lymphoblastic_leukemia' then 1 else 0 end) as acute_lymphoblastic_leukemia
, max(case when cleaned_concept_name = 'pulmonary_embolism_thrombotic_or_unspecified' then 1 else 0 end) as pulmonary_embolism_thrombotic_or_unspecified
, max(case when cleaned_concept_name = 'alcohol_use_disorder' then 1 else 0 end) as alcohol_use_disorder
, max(case when cleaned_concept_name = 'haemophilus_influenzae_invasive_disease' then 1 else 0 end) as haemophilus_influenzae_invasive_disease
, max(case when cleaned_concept_name = 'alzheimer_disease' then 1 else 0 end) as alzheimer_disease
, max(case when cleaned_concept_name = 'lung_cancer_primary_or_unspecified' then 1 else 0 end) as lung_cancer_primary_or_unspecified
, max(case when cleaned_concept_name = 'anxiety_disorders' then 1 else 0 end) as anxiety_disorders
, max(case when cleaned_concept_name = 'osteoporosis' then 1 else 0 end) as osteoporosis
, max(case when cleaned_concept_name = 'asthma' then 1 else 0 end) as asthma
, max(case when cleaned_concept_name = 'st_louis_encephalitis_virus_disease' then 1 else 0 end) as st_louis_encephalitis_virus_disease
, max(case when cleaned_concept_name = 'atherosclerosis' then 1 else 0 end) as atherosclerosis
, max(case when cleaned_concept_name = 'western_equine_encephalitis_virus_disease' then 1 else 0 end) as western_equine_encephalitis_virus_disease
, max(case when cleaned_concept_name = 'atrial_fibrillation' then 1 else 0 end) as atrial_fibrillation
, max(case when cleaned_concept_name = 'abdominal_hernia' then 1 else 0 end) as abdominal_hernia
, max(case when cleaned_concept_name = 'hepatitis_c_infection_acute' then 1 else 0 end) as hepatitis_c_infection_acute
, max(case when cleaned_concept_name = 'attention_deficithyperactivity_disorder' then 1 else 0 end) as attention_deficithyperactivity_disorder
, max(case when cleaned_concept_name = 'leptospirosis' then 1 else 0 end) as leptospirosis
, max(case when cleaned_concept_name = 'benign_prostatic_hyperplasia' then 1 else 0 end) as benign_prostatic_hyperplasia
, max(case when cleaned_concept_name = 'multiple_myeloma' then 1 else 0 end) as multiple_myeloma
, max(case when cleaned_concept_name = 'bipolar_affective_disorder' then 1 else 0 end) as bipolar_affective_disorder
, max(case when cleaned_concept_name = 'opioid_use_disorder' then 1 else 0 end) as opioid_use_disorder
, max(case when cleaned_concept_name = 'botulism' then 1 else 0 end) as botulism
, max(case when cleaned_concept_name = 'parvovirus_infection' then 1 else 0 end) as parvovirus_infection
, max(case when cleaned_concept_name = 'botulism_foodborne' then 1 else 0 end) as botulism_foodborne
, max(case when cleaned_concept_name = 'rheumatoid_arthritis' then 1 else 0 end) as rheumatoid_arthritis
, max(case when cleaned_concept_name = 'botulism_wound' then 1 else 0 end) as botulism_wound
, max(case when cleaned_concept_name = 'stroke' then 1 else 0 end) as stroke
, max(case when cleaned_concept_name = 'breast_cancer' then 1 else 0 end) as breast_cancer
, max(case when cleaned_concept_name = 'ulcerative_colitis' then 1 else 0 end) as ulcerative_colitis
, max(case when cleaned_concept_name = 'cardiac_dysrhythmias' then 1 else 0 end) as cardiac_dysrhythmias
, max(case when cleaned_concept_name = 'glaucoma' then 1 else 0 end) as glaucoma
, max(case when cleaned_concept_name = 'cataract' then 1 else 0 end) as cataract
, max(case when cleaned_concept_name = 'heart_failure' then 1 else 0 end) as heart_failure
, max(case when cleaned_concept_name = 'chronic_kidney_disease' then 1 else 0 end) as chronic_kidney_disease
, max(case when cleaned_concept_name = 'herpes_simplex_infection' then 1 else 0 end) as herpes_simplex_infection
, max(case when cleaned_concept_name = 'chronic_obstructive_pulmonary_disease' then 1 else 0 end) as chronic_obstructive_pulmonary_disease
, max(case when cleaned_concept_name = 'hypertension' then 1 else 0 end) as hypertension
, max(case when cleaned_concept_name = 'clostridioides_difficile_enterocolitis' then 1 else 0 end) as clostridioides_difficile_enterocolitis
, max(case when cleaned_concept_name = 'legionellosis' then 1 else 0 end) as legionellosis
, max(case when cleaned_concept_name = 'colorectal_cancer' then 1 else 0 end) as colorectal_cancer
, max(case when cleaned_concept_name = 'listeriosis' then 1 else 0 end) as listeriosis
, max(case when cleaned_concept_name = 'covid19' then 1 else 0 end) as covid19
, max(case when cleaned_concept_name = 'major_depressive_disorder' then 1 else 0 end) as major_depressive_disorder
, max(case when cleaned_concept_name = 'cryptosporidiosis' then 1 else 0 end) as cryptosporidiosis
, max(case when cleaned_concept_name = 'myocardial_infarction' then 1 else 0 end) as myocardial_infarction
, max(case when cleaned_concept_name = 'cytomegalovirus_infection' then 1 else 0 end) as cytomegalovirus_infection
, max(case when cleaned_concept_name = 'obesity' then 1 else 0 end) as obesity
, max(case when cleaned_concept_name = 'deep_vein_thrombosis_of_extremities_or_central_veins' then 1 else 0 end) as deep_vein_thrombosis_of_extremities_or_central_veins
, max(case when cleaned_concept_name = 'osteoarthritis' then 1 else 0 end) as osteoarthritis
, max(case when cleaned_concept_name = 'dementia' then 1 else 0 end) as dementia
, max(case when cleaned_concept_name = 'parkinsons_disease' then 1 else 0 end) as parkinsons_disease
, max(case when cleaned_concept_name = 'dexamethasone_systemic' then 1 else 0 end) as dexamethasone_systemic
, max(case when cleaned_concept_name = 'posttraumatic_stress_disorder' then 1 else 0 end) as posttraumatic_stress_disorder
, max(case when cleaned_concept_name = 'diabetes_mellitus' then 1 else 0 end) as diabetes_mellitus
, max(case when cleaned_concept_name = 'respiratory_syncytial_virus_infection' then 1 else 0 end) as respiratory_syncytial_virus_infection
, max(case when cleaned_concept_name = 'diphtheria' then 1 else 0 end) as diphtheria
, max(case when cleaned_concept_name = 'schizophrenia' then 1 else 0 end) as schizophrenia
, max(case when cleaned_concept_name = 'diverticulitis_of_large_intestine' then 1 else 0 end) as diverticulitis_of_large_intestine
, max(case when cleaned_concept_name = 'stem_cell_transplantation' then 1 else 0 end) as stem_cell_transplantation
, max(case when cleaned_concept_name = 'dyslipidemias' then 1 else 0 end) as dyslipidemias
, max(case when cleaned_concept_name = 'tobacco_use' then 1 else 0 end) as tobacco_use
, max(case when cleaned_concept_name = 'endocarditis' then 1 else 0 end) as endocarditis
, max(case when cleaned_concept_name = 'type_2_diabetes_mellitus' then 1 else 0 end) as type_2_diabetes_mellitus
, max(case when cleaned_concept_name = 'epilepsy_and_seizure_disorders' then 1 else 0 end) as epilepsy_and_seizure_disorders
, max(case when cleaned_concept_name = 'west_nile_virus_disease' then 1 else 0 end) as west_nile_virus_disease
, max(case when cleaned_concept_name = 'erectile_dysfunction' then 1 else 0 end) as erectile_dysfunction
, max(case when cleaned_concept_name = 'abdominal_aortic_aneurysm' then 1 else 0 end) as abdominal_aortic_aneurysm
, max(case when cleaned_concept_name = 'gastroesophageal_reflux' then 1 else 0 end) as gastroesophageal_reflux
from cte
 group by person_id
, year_nbr
)

select
mm.person_id
, mm.year_nbr
, coalesce(hip_fracture, 0) as hip_fracture
, coalesce(type_1_diabetes_mellitus, 0) as type_1_diabetes_mellitus
, coalesce(no_chronic_conditions, 0) as no_chronic_conditions
, coalesce(invasive_pneumococcal_disease, 0) as invasive_pneumococcal_disease
, coalesce(acute_lymphoblastic_leukemia, 0) as acute_lymphoblastic_leukemia
, coalesce(pulmonary_embolism_thrombotic_or_unspecified, 0) as pulmonary_embolism_thrombotic_or_unspecified
, coalesce(alcohol_use_disorder, 0) as alcohol_use_disorder
, coalesce(haemophilus_influenzae_invasive_disease, 0) as haemophilus_influenzae_invasive_disease
, coalesce(alzheimer_disease, 0) as alzheimer_disease
, coalesce(lung_cancer_primary_or_unspecified, 0) as lung_cancer_primary_or_unspecified
, coalesce(anxiety_disorders, 0) as anxiety_disorders
, coalesce(osteoporosis, 0) as osteoporosis
, coalesce(asthma, 0) as asthma
, coalesce(st_louis_encephalitis_virus_disease, 0) as st_louis_encephalitis_virus_disease
, coalesce(atherosclerosis, 0) as atherosclerosis
, coalesce(western_equine_encephalitis_virus_disease, 0) as western_equine_encephalitis_virus_disease
, coalesce(atrial_fibrillation, 0) as atrial_fibrillation
, coalesce(abdominal_hernia, 0) as abdominal_hernia
, coalesce(hepatitis_c_infection_acute, 0) as hepatitis_c_infection_acute
, coalesce(attention_deficithyperactivity_disorder, 0) as attention_deficithyperactivity_disorder
, coalesce(leptospirosis, 0) as leptospirosis
, coalesce(benign_prostatic_hyperplasia, 0) as benign_prostatic_hyperplasia
, coalesce(multiple_myeloma, 0) as multiple_myeloma
, coalesce(bipolar_affective_disorder, 0) as bipolar_affective_disorder
, coalesce(opioid_use_disorder, 0) as opioid_use_disorder
, coalesce(botulism, 0) as botulism
, coalesce(parvovirus_infection, 0) as parvovirus_infection
, coalesce(botulism_foodborne, 0) as botulism_foodborne
, coalesce(rheumatoid_arthritis, 0) as rheumatoid_arthritis
, coalesce(botulism_wound, 0) as botulism_wound
, coalesce(stroke, 0) as stroke
, coalesce(breast_cancer, 0) as breast_cancer
, coalesce(ulcerative_colitis, 0) as ulcerative_colitis
, coalesce(cardiac_dysrhythmias, 0) as cardiac_dysrhythmias
, coalesce(glaucoma, 0) as glaucoma
, coalesce(cataract, 0) as cataract
, coalesce(heart_failure, 0) as heart_failure
, coalesce(chronic_kidney_disease, 0) as chronic_kidney_disease
, coalesce(herpes_simplex_infection, 0) as herpes_simplex_infection
, coalesce(chronic_obstructive_pulmonary_disease, 0) as chronic_obstructive_pulmonary_disease
, coalesce(hypertension, 0) as hypertension
, coalesce(clostridioides_difficile_enterocolitis, 0) as clostridioides_difficile_enterocolitis
, coalesce(legionellosis, 0) as legionellosis
, coalesce(colorectal_cancer, 0) as colorectal_cancer
, coalesce(listeriosis, 0) as listeriosis
, coalesce(covid19, 0) as covid19
, coalesce(major_depressive_disorder, 0) as major_depressive_disorder
, coalesce(cryptosporidiosis, 0) as cryptosporidiosis
, coalesce(myocardial_infarction, 0) as myocardial_infarction
, coalesce(cytomegalovirus_infection, 0) as cytomegalovirus_infection
, coalesce(obesity, 0) as obesity
, coalesce(deep_vein_thrombosis_of_extremities_or_central_veins, 0) as deep_vein_thrombosis_of_extremities_or_central_veins
, coalesce(osteoarthritis, 0) as osteoarthritis
, coalesce(dementia, 0) as dementia
, coalesce(parkinsons_disease, 0) as parkinsons_disease
, coalesce(dexamethasone_systemic, 0) as dexamethasone_systemic
, coalesce(posttraumatic_stress_disorder, 0) as posttraumatic_stress_disorder
, coalesce(diabetes_mellitus, 0) as diabetes_mellitus
, coalesce(respiratory_syncytial_virus_infection, 0) as respiratory_syncytial_virus_infection
, coalesce(diphtheria, 0) as diphtheria
, coalesce(schizophrenia, 0) as schizophrenia
, coalesce(diverticulitis_of_large_intestine, 0) as diverticulitis_of_large_intestine
, coalesce(stem_cell_transplantation, 0) as stem_cell_transplantation
, coalesce(dyslipidemias, 0) as dyslipidemias
, coalesce(tobacco_use, 0) as tobacco_use
, coalesce(endocarditis, 0) as endocarditis
, coalesce(type_2_diabetes_mellitus, 0) as type_2_diabetes_mellitus
, coalesce(epilepsy_and_seizure_disorders, 0) as epilepsy_and_seizure_disorders
, coalesce(west_nile_virus_disease, 0) as west_nile_virus_disease
, coalesce(erectile_dysfunction, 0) as erectile_dysfunction
, coalesce(abdominal_aortic_aneurysm, 0) as abdominal_aortic_aneurysm
, coalesce(gastroesophageal_reflux, 0) as gastroesophageal_reflux

from member_month as mm
left outer join condition_flags as f on mm.person_id = f.condition_person_id
and
mm.year_nbr = f.condition_year_nbr
