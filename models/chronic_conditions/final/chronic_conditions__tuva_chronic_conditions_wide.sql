{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with patients as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
)

, obesity as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Obesity'
)


, osteoarthritis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Osteoarthritis'
)

, copd as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Chronic Obstructive Pulmonary Disease'
)

, anxiety_disorders as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Anxiety Disorders'
)

, ckd as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Chronic Kidney Disease'
)

, t2d as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Type 2 Diabetes Mellitus'
)

, cll as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Chronic Lymphocytic Leukemia'
)

, dysplipidemias as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Dyslipidemias'
)

, hypertension as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Hypertension'
)

, atherosclerosis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Atherosclerosis'
)

, dementia as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Dementia'
)

, rheumatoid_arthritis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Rheumatoid Arthritis'
)

, celiac as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Celiac Disease'
)

, hip_fracture as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Hip Fracture'
)

, immunodeficiencies_and_white_blood_cell_disorders as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Specified Immunodeficiencies and White Blood Cell Disorders  (HCC v28 concept #115)'
)

, asthma as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Asthma'
)

, t1d as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Type 1 Diabetes Mellitus'
)

, ulcerative_colitis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Ulcerative colitis'
)

, chrohns as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Crohns Disease'
)

, holicobacter as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Helicobacter pylori Infection'
)

, bipolar as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Bipolar Affective Disorder'
)

, heart_failure as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Heart Failure'
)

, tabacco as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Tobacco Use'
)

, lyme as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Lyme Disease'
)

, breast_cancer as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Breast Cancer'
)

, osteoporosis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Osteoporosis'
)

, pulmonary_embolism as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Pulmonary Embolism, Thrombotic or Unspecified'
)

, schizophrenia as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Schizophrenia'
)

, atrial_fibrillation as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Atrial Fibrillation'
)

, colorectal_cancer as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Colorectal Cancer'
)

, depression as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Major Depressive Disorder'
)

, deep_vein_thrombosis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Deep Vein Thrombosis of Extremities or Central Veins'
)

, alzheimer as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Alzheimer Disease'
)

, stroke as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Stroke'
)

, myocardial_infraction as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Myocardial Infarction'
)

, opiod_use_disorder as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Opioid Use Disorder'
)

, lung_cancer as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Lung cancer, primary or unspecified'
)

, herpes as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Herpes Simplex Infection'
)

, rickettsiosis as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Rickettsiosis'
)

, ms as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Multiple Sclerosis'
)

, alchohol as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Alcohol Use Disorder'
)

, adhd as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Attention Deficit-Hyperactivity Disorder'
)

, hiv as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'HIV/AIDS  (HCC v28 concept #1)'
)

, ptsd as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Post-Traumatic Stress Disorder'
)

, lupus as (
select distinct person_id
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
where condition = 'Systemic Lupus Erythematosus'
)




select
  person_id
  , case when person_id in (select * from obesity) then 1
       else 0
  end as obesity
  , case when person_id in (select * from osteoarthritis) then 1
       else 0
  end as osteoarthritis
  , case when person_id in (select * from copd) then 1
       else 0
  end as copd
  , case when person_id in (select * from anxiety_disorders) then 1
       else 0
  end as anxiety_disorders
  , case when person_id in (select * from ckd) then 1
       else 0
  end as ckd
  , case when person_id in (select * from t2d) then 1
       else 0
  end as t2d
  , case when person_id in (select * from cll) then 1
       else 0
  end as cll
  , case when person_id in (select * from dysplipidemias) then 1
       else 0
  end as dysplipidemias
  , case when person_id in (select * from hypertension) then 1
       else 0
  end as hypertension
  , case when person_id in (select * from atherosclerosis) then 1
       else 0
  end as atherosclerosis
  , case when person_id in (select * from dementia) then 1
       else 0
  end as dementia
  , case when person_id in (select * from rheumatoid_arthritis) then 1
       else 0
  end as rheumatoid_arthritis
  , case when person_id in (select * from celiac) then 1
       else 0
  end as celiac
  , case when person_id in (select * from hip_fracture) then 1
       else 0
  end as hip_fracture
  , case when person_id in (select * from immunodeficiencies_and_white_blood_cell_disorders) then 1
       else 0
  end as immunodeficiencies_and_white_blood_cell_disorders
  , case when person_id in (select * from asthma) then 1
       else 0
  end as asthma
  , case when person_id in (select * from t1d) then 1
       else 0
  end as t1d
  , case when person_id in (select * from ulcerative_colitis) then 1
       else 0
  end as ulcerative_colitis
  , case when person_id in (select * from chrohns) then 1
       else 0
  end as chrohns
  , case when person_id in (select * from holicobacter) then 1
       else 0
  end as holicobacter
  , case when person_id in (select * from bipolar) then 1
       else 0
  end as bipolar
  , case when person_id in (select * from heart_failure) then 1
       else 0
  end as heart_failure
  , case when person_id in (select * from tabacco) then 1
       else 0
  end as tabacco
  , case when person_id in (select * from lyme) then 1
       else 0
  end as lyme
  , case when person_id in (select * from breast_cancer) then 1
       else 0
  end as breast_cancer
  , case when person_id in (select * from osteoporosis) then 1
       else 0
  end as osteoporosis
  , case when person_id in (select * from pulmonary_embolism) then 1
       else 0
  end as pulmonary_embolism
  , case when person_id in (select * from schizophrenia) then 1
       else 0
  end as schizophrenia
  , case when person_id in (select * from atrial_fibrillation) then 1
       else 0
  end as atrial_fibrillation
  , case when person_id in (select * from colorectal_cancer) then 1
       else 0
  end as colorectal_cancer
  , case when person_id in (select * from depression) then 1
       else 0
  end as depression
  , case when person_id in (select * from deep_vein_thrombosis) then 1
       else 0
  end as deep_vein_thrombosis
  , case when person_id in (select * from alzheimer) then 1
       else 0
  end as alzheimer
  , case when person_id in (select * from stroke) then 1
       else 0
  end as stroke
  , case when person_id in (select * from myocardial_infraction) then 1
       else 0
  end as myocardial_infraction
  , case when person_id in (select * from opiod_use_disorder) then 1
       else 0
  end as opiod_use_disorder
  , case when person_id in (select * from lung_cancer) then 1
       else 0
  end as lung_cancer
  , case when person_id in (select * from herpes) then 1
       else 0
  end as herpes
  , case when person_id in (select * from rickettsiosis) then 1
       else 0
  end as rickettsiosis
  , case when person_id in (select * from ms) then 1
       else 0
  end as ms
  , case when person_id in (select * from alchohol) then 1
       else 0
  end as alchohol
  , case when person_id in (select * from adhd) then 1
       else 0
  end as adhd
  , case when person_id in (select * from hiv) then 1
       else 0
  end as hiv
  , case when person_id in (select * from ptsd) then 1
       else 0
  end as ptsd
  , case when person_id in (select * from lupus) then 1
       else 0
  end as lupus
  , '{{ var('tuva_last_run') }}' as tuva_last_run

from patients
