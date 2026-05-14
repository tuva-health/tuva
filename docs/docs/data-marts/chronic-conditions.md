---
id: chronic-conditions
title: "Chronic Conditions"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/chronic_conditions)

The Chronic Conditions data mart implements two different chronic condition groupers: one defined by [CMS](https://www2.ccwdata.org/web/guest/condition-categories-chronic) and the other defined by Tuva.  We started defining chronic conditions in Tuva after struggling to use the CMS logic, either because certain chronic conditions were missing (e.g. non-alcoholic fatty liver disease, MASH, etc.) or because existing definitions were unsatisfactory (e.g. type 1 and type 2 diabetes are considered the same condition by CMS) even though the pathology of the two is distinctly different.

You can find the methods for CMS's methodology using the above link.  You can search exact codes used in the Tuva definition in the clinical concept library in our value sets.

## Available Chronic Conditions

The tables below list the distinct chronic condition values from the Tuva Health [value-sets database](https://www.dolthub.com/repositories/tuva-health/value-sets) on DoltHub.

### CMS Chronic Conditions

Source table: [`chronic_conditions__cms_chronic_conditions_hierarchy`](https://www.dolthub.com/repositories/tuva-health/value-sets/data/main/chronic_conditions__cms_chronic_conditions_hierarchy)

| Condition |
| --- |
| ADHD, Conduct Disorders, and Hyperkinetic Syndrome |
| Acute Myocardial Infarction |
| Alcohol Use Disorders |
| Alzheimer’s Disease |
| Anemia |
| Anxiety Disorders |
| Asthma |
| Atrial Fibrillation and Flutter |
| Autism Spectrum Disorders |
| Benign Prostatic Hyperplasia |
| Bipolar Disorder |
| Cancer, Breast |
| Cancer, Colorectal |
| Cancer, Endometrial |
| Cancer, Lung |
| Cancer, Prostate |
| Cancer, Urologic (Kidney, Renal Pelvis, and Ureter) |
| Cataract |
| Cerebral Palsy |
| Chronic Kidney Disease |
| Chronic Obstructive Pulmonary Disease |
| Cystic Fibrosis and Other Metabolic Developmental Disorders |
| Depression, Bipolar, or Other Depressive Mood Disorders |
| Depressive Disorders |
| Diabetes |
| Drug Use Disorders |
| Epilepsy |
| Fibromyalgia and Chronic Pain and Fatigue |
| Glaucoma |
| Heart Failure and Non-Ischemic Heart Disease |
| Hepatitis A |
| Hepatitis B (acute or unspecified) |
| Hepatitis B (chronic) |
| Hepatitis C (acute) |
| Hepatitis C (chronic) |
| Hepatitis C (unspecified) |
| Hepatitis D |
| Hepatitis E |
| Hip/Pelvic Fracture |
| Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS) |
| Hyperlipidemia |
| Hypertension |
| Hypothyroidism |
| Intellectual Disabilities and Related Conditions |
| Ischemic Heart Disease |
| Learning Disabilities |
| Leukemias and Lymphomas |
| Liver Disease, Cirrhosis, and Other Liver Conditions (except Viral Hepatitis) |
| Migraine and Chronic Headache |
| Mobility Impairments |
| Multiple Sclerosis and Transverse Myelitis |
| Muscular Dystrophy |
| Non-Alzheimer’s Dementia |
| Obesity |
| Opioid Use Disorder (OUD) |
| Osteoporosis With or Without Pathological Fracture |
| Other Developmental Delays |
| Parkinson’s Disease and Secondary Parkinsonism |
| Peripheral Vascular Disease (PVD) |
| Personality Disorders |
| Pneumonia, All-cause |
| Post-Traumatic Stress Disorder (PTSD) |
| Pressure and Chronic Ulcers |
| Rheumatoid Arthritis/Osteoarthritis |
| Schizophrenia |
| Schizophrenia and Other Psychotic Disorders |
| Sensory — Blindness and Visual Impairment |
| Sensory — Deafness and Hearing Impairment |
| Sickle Cell Disease |
| Spina Bifida and Other Congenital Anomalies of the Nervous System |
| Spinal Cord Injury |
| Stroke/Transient Ischemic Attack |
| Tobacco Use |
| Traumatic Brain Injury and Nonpsychotic Mental Disorders due to Brain Damage |
| Viral Hepatitis (general) |

### Tuva Chronic Conditions

Source table: [`chronic_conditions__tuva_chronic_conditions_hierarchy`](https://www.dolthub.com/repositories/tuva-health/value-sets/data/main/chronic_conditions__tuva_chronic_conditions_hierarchy)

| Condition Family | Condition |
| --- | --- |
| Autoimmune Disease | Crohn's Disease |
| Autoimmune Disease | Lupus |
| Autoimmune Disease | Rheumatoid Arthritis |
| Autoimmune Disease | Type 1 Diabetes |
| Autoimmune Disease | Ulcerative Colitis |
| Cancer | Breast Cancer |
| Cancer | Colorectal Cancer |
| Cancer | Lung Cancer |
| Cardiovascular Disease | Acute Myocardial Infarction |
| Cardiovascular Disease | Atherosclerosis |
| Cardiovascular Disease | Atrial Fibrillation |
| Cardiovascular Disease | Heart Failure |
| Cardiovascular Disease | Hypertension |
| Cardiovascular Disease | Stroke / Transient Ischemic Attack |
| Mental Health | Anxiety |
| Mental Health | Attention-Deficit Hyperactivity Disorder (ADHD) |
| Mental Health | Bipolar |
| Mental Health | Depression |
| Mental Health | Obsessive-Compulsive Disorder (OCD) |
| Mental Health | Personality Disorder |
| Mental Health | Post-Traumatic Stress Disorder (PTSD) |
| Mental Health | Schizophrenia |
| Metabolic Disease | Chronic Kidney Disease |
| Metabolic Disease | Hyperlipidemia |
| Metabolic Disease | Metabolic Syndrome |
| Metabolic Disease | Obesity |
| Metabolic Disease | Type 2 Diabetes |
| Neuro-degenerative Disease | Alzheimer’s Disease |
| Neuro-degenerative Disease | Amyotrophic Lateral Sclerosis (ALS) |
| Neuro-degenerative Disease | Dementia |
| Neuro-degenerative Disease | Multiple Sclerosis |
| Neuro-degenerative Disease | Muscular Dystrophy |
| Neuro-degenerative Disease | Parkinson's Disease |
| Pulmonary Disease | Asthma |
| Pulmonary Disease | Chronic Obstructive Pulmonary Disease (COPD) |
| Pulmonary Disease | Cystic Fibrosis |
| Substance Use | Alcohol |
| Substance Use | Cocaine |
| Substance Use | Opioid |
| Substance Use | Tobacco |

## Example SQL

<details>
  <summary>Prevalence of Tuva Chronic Conditions</summary>

In this query we show how often each chronic condition occurs in the patient population.

```sql
select
  condition
, count(distinct person_id) as total_patients
, cast(count(distinct person_id) * 100.0 / (select count(distinct person_id) from core.patient) as numeric(38,2)) as percent_of_patients
from chronic_conditions.tuva_chronic_conditions_long
group by 1
order by 3 desc
```

</details>

<details>
  <summary>Prevalence of CMS Chronic Conditions</summary>

In this query we show how often each chronic condition occurs in the patient population.

```sql
select
  condition_category
, condition
, count(distinct person_id) as total_patients
, cast(count(distinct person_id) * 100.0 / (select count(distinct person_id) from core.patient) as numeric(38,2)) as percent_of_patients
from chronic_conditions.cms_chronic_conditions_long
group by 1,2
order by 4 desc
```

</details>

<details>
  <summary>Distribution of Chronic Conditions</summary>

In this query we show how many patients have 0 chronic conditions, how many patients have 1 chronic condition, how many patients have 2 chronic conditions, etc.

```sql
with patients as (
select person_id
from core.patient
)

, conditions as (
select distinct
  a.person_id
, b.condition
from patients a
left join chronic_conditions.tuva_chronic_conditions_long b
 on a.person_id = b.person_id
)

, condition_count as (
select
  person_id
, count(distinct condition) as condition_count
from conditions
group by 1
)

select 
  condition_count
, count(1)
, cast(100 * count(distinct person_id)/sum(count(distinct person_id)) over() as numeric(38,1)) as percent
from condition_count
group by 1
order by 1
```

</details>
