{% docs __the_tuva_project__ %}
# 🌎 The Tuva Project
## 🧰 What is The Tuva Project?

Healthcare data is messy, and working with it is hard!  Analytics teams spend countless hours ingesting, cleaning and transforming healthcare data in order to get it ready for analytics and machine learning. Healthcare knowledge and code are siloed; countless institutions are wasting hours duplicating the same work, and when mistakes are made or issues are missed, the insights gained from the analytics have little value.

Tuva Health is aiming to change that with the launch of The Tuva Project.  We're making it easy to get value out of your healthcare data by open sourcing the code needed to transform your data, publishing a knowledgebase of healthcare concepts and artifacts to make it easy to understand and learn about healthcare data, and building an online community of healthcare data professionals to share insights and get help.

### 🕮 [Knowledge](https://thetuvaproject.com/docs/intro)
We're working towards building a complete knowledgebase of healthcare data concepts, covering everything from getting started with healthcare data through higher level analytics concepts.  You can [help contribute](https://thetuvaproject.com/docs/how-to-contribute/edit-github) by adding new pages through github.

### 🖥️ [Code](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/)
We're open-sourcing code to help transform your healthcare data.  Built just import this dbt package, just map your data to our input layer and instantly transform your data into helpful core concepts, get data marts for some of the most common analytics applications, get insights about your data quality and how it changes over time, and easily import some of the most useful healthcare terminology sets into your data warehouse.


### 🤝 [Community](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)
Still stuck?  Join our slack community of healthcare data professionals and get answers to your healthcare data questions, communicate directly with the engineers working on The Tuva Project, and get the latest updates.

## ⁉ What is this package and how do I use it?

This is The Tuva Project, a [dbt package](https://docs.getdbt.com/docs/build/packages) that imports all of the packages developed by Tuva Health.  Running The Tuva Project is as simple as [mapping](https://thetuvaproject.com/docs/claims-data-warehouse/setup) your data to our input layer, adding the_tuva_project to your packages.yml, and running `dbt deps` and `dbt build`.  See our [readme](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) for more detailed setup instructions.  If you are new to dbt, check out [their documentation](https://docs.getdbt.com/docs/introduction) for tips on getting started.

This package will import the following packages:
- [data_profiling](https://github.com/tuva-health/data_profiling): Runs data quality tests to check for common problems specific to healthcare claims data.
- [claims_preprocessing](https://github.com/tuva-health/claims_preprocessing): Groups overlapping claims into a single encounter, assigns every claim to 1 of 15 different encounter types and populates core concept tables.
- [cms_chronic_conditions](https://github.com/tuva-health/chronic_conditions): Implements a chronic condition grouper based on ICD-10-CM codes. As a result, it is possible to know whether each patient in your population has any of ~70 different chronic conditions defined for the grouper.
- [tuva_chronic_conditions](https://github.com/tuva-health/tuva_chronic_conditions): implements a chronic condition grouper created by the Tuva Project which creates ~40 homogeneous and mutually exclusive chronic condition groups on your patient.
- [pmpm](https://github.com/tuva-health/pmpm): Calculates spend and utilization metrics for your patient population on a per-member-per-month (pmpm) basis.
- [readmissions](https://github.com/tuva-health/readmissions): Calculates hospital readmission measures.
- [terminology](https://github.com/tuva-health/terminology): Makes the latest version of many useful healthcare terminology datasets available as tables in your data warehouse. This package is different from the others because it does not build healthcare concepts on top of your data.

{% enddocs %}


{% docs __the_tuva_project_input__ %}
# 🧰 The Tuva Project Input Layer

In order to run The Tuva Project, you will need to start by creating the following 3 models in your dbt project with their appropriate structures:
- [eligibility](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.eligibility): A table with eligibility coverage spans for all patients.
- [medical_claim](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.medical_claim): A table at the claim line grain housing claim, condition, and procedure information for professional and institutional claims. 
- [pharmacy_claim](https://tuva-health.github.io/the_tuva_project/#!/model/model.the_tuva_project_input.pharmacy_claim): A table housing pharmacy claim details.

For more detailed instructions and guidance, see our [mapping guide](https://thetuvaproject.com/docs/claims-data-warehouse/setup) 

{% enddocs %}

{% docs __readmissions__ %}
# 📦 Readmissions

Hospital Readmissions are when a patient returns to the hospital within 30 days of a previous encounter for the same or a related condition. A hospital's readmission rate is often used to measure quality of care, and [CMS reduces reimbursements](https://www.cms.gov/Medicare/Quality-Initiatives-Patient-Assessment-Instruments/Value-Based-Programs/HRRP/Hospital-Readmission-Reduction-Program) according to an adjustment factor correlated with the readmission measures.

  


### 📊 What does this package produce?

This package produces a mart which calculates which encounters are readmissions.  It produces the following two main output tables:

| Table                                                                                            | Definition                                                                                                                                                                 |
|--------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [encounter_augmented](#!/model/model.readmissions.readmissions__encounter_augmented#description) | A table at the encounter grain including details about which encounters were flagged for readmissions and other flags indicating why or why not the encounter was flagged  |
| [readmission_summary](#!/model/model.readmissions.readmissions__readmission_summary#description) | A table table showing all encounters that were not disqualified for data quality reasons, with flags indicating if the encounter had readmissions and other relevant information |

  

  

---
### 🔗 Quick Links
- [Learn more about Readmissions](https://thetuvaproject.com/docs/analytics/readmissions) at the Tuva Project
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/readmissions/latest/) or [GitHub](https://github.com/tuva-health/readmissions)


{% enddocs %}


{% docs __data_profiling__ %}
# 📦 Data Profiling

Data Profiling is the process of examining and analyzing your data to gain insights into its quality. This package runs a variety of tests against the input tables you have mapped in the Claims Data Model that will help identify data quality problems or mapping issues that could affect downstream analytics.

  


### 📊 What does this package produce?

Data Profiling produces 3 detail tables that provide row-level data quality insights for your mapped Claims Data Model models, and a summary table that provides a high level overview into all three tables.    

| Table                                                                                                    | Definition                                                                                                                 |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| [claim_summary](#!/model/model.data_profiling.data_profiling__claim_summary#description)                 | A table that summarizes the data quality issues in `medical_claim`, `pharmacy_claim`, and `eligibility`                    |
| [eligibility_detail](#!/model/model.data_profiling.data_profiling__eligibility_detail#description)       | A table with the keys to link to `eligibiltiy` and flags indicating whether or not that row failed data quality checks    |
| [medical_claim_detail](#!/model/model.data_profiling.data_profiling__medical_claim_detail#description)   | A table with the keys to link to `medical_claim` and flags indicating whether or not that row failed data quality checks  |
| [pharmacy_claim_detail](#!/model/model.data_profiling.data_profiling__pharmacy_claim_detail#description) | A table with the keys to link to `pharmacy_claim` and flags indicating whether or not that row failed data quality checks |

  

  

---
### 🔗 Quick Links
- [Learn more about Data Profiling](https://thetuvaproject.com/docs/analytics/data-profiling) at the Tuva Project
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/data_profiling/latest/) or [GitHub](https://github.com/tuva-health/data_profiling)


{% enddocs %}


{% docs __claims_preprocessing__ %}
# 📦 Claims Preprocessing

Claims data in its raw form is not built with analytics in mind.  This package takes the Claims Data Model tables you have created, and produces tables more readily built for analytics.  It also groups claims into encounters, and infers an encounter type based on the data in the claims.

  


### 📊 What does this package produce?

This package produces the core schema, which enhances the Claims Data Model and together serve as the fundamental building blocks other packages in The Tuva Project build upon.       

| Table                                                                                  | Definition                                                                          |
|----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [condition](#!/model/model.claims_preprocessing.claims_preprocessing__condition#description)       | A table containing all conditions diagnosed in the claims data                      |
| [encounter](#!/model/model.claims_preprocessing.claims_preprocessing__encounter#description)       | A table containing all encounters and encounter types identified in the claims data |
| [patient](#!/model/model.claims_preprocessing.claims_preprocessing__patient#description)           | A table containing all patients present in the enrollment data                      |
| [prescription](#!/model/model.claims_preprocessing.claims_preprocessing__prescription#description) | A table containing all medications prescribed to a patient                          |
| [procedure](#!/model/model.claims_preprocessing.claims_preprocessing__procedure#description)       | A table containing all procedures performed on a patient in the claims data         |

  

  

---
### 🔗 Quick Links
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/claims_preprocessing/latest/) or [GitHub](https://github.com/tuva-health/claims_preprocessing)


{% enddocs %}


{% docs __terminology__ %}
# 📦 Terminology

Claims data utilizes numerous code sets to convey critical information, but generally does not include human readable descriptions of those codes.  The Terminology Package provides an easy way of loading those dictionaries as well as common mappings and groupers, so you can get more meaningful insight into your data.

  


### 📊 What does this package produce?

This package loads numerous healthcare terminology sets into your warehouse.  Please navigate the project tree in these docs for more information, or see [github](https://github.com/tuva-health/terminology/tree/main/terminology) for the raw list of files and their contents.

  

  

---
### 🔗 Quick Links
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/terminology/latest/) or [GitHub](https://github.com/tuva-health/terminology)


{% enddocs %}



{% docs __cms_chronic_conditions__ %}
# 📦 CMS Chronic Conditions

The OEDA and CMS have develeoped a [set of information products and analytics](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Chronic-Conditions) examining chronic conditions among Medicare beneficiaries.  Identifying patients with chronic conditions will help provide an understanding of the burden and implications for the healthcare system, help identify high-risk patients, and help inform about resource utilization of patients with chronic diseases.

  


### 📊 What does this package produce?

This package calculates which patients have chronic conditions per the CMS methodological standards, and produces that information in two tables at the patient grain and at the patient-condition grain

| Table                                                                                                                  | Definition                                                                                                                                          |
|------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| [chronic_conditions_pivoted](#!/model/model.cms_chronic_conditions.cms_chronic_conditions__chronic_conditions_pivoted#description) | A table at the patient grain, with a row for each patient and a column for each chronic condition and a flag indicating if they have that condition |
| [chronic_conditions_unioned](#!/model/model.cms_chronic_conditions.cms_chronic_conditions__chronic_conditions_unioned#description) | A table at the patient-condition grain, with a row for every chronic condition identified in the data                                               |
  

  

---
### 🔗 Quick Links
- [Learn more about Chronic Conditions](https://thetuvaproject.com/docs/analytics/chronic-conditions) at the Tuva Project
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/cms_chronic_conditions/latest/) or [GitHub](https://github.com/tuva-health/cms_chronic_conditions)


{% enddocs %}



{% docs __tuva_chronic_conditions__ %}
# 📦 Tuva Chronic Conditions

Identifying patients with chronic conditions will help provide an understanding of the burden and implications for the healthcare system, help identify high-risk patients, and help inform about resource utilization of patients with chronic diseases.

  


### 📊 What does this package produce?

This package calculates which patients have chronic conditions, and produces that information in two tables at the patient grain and at the patient-condition grain, and provides a table for grouping conditions into condition families.

| Table                                                                                                                            | Definition                                                                                                                                          |
|----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| [chronic_condition_groups](#!/model/model.tuva_chronic_conditions.tuva_chronic_conditions__chronic_condition_groups#description) | A table with mappings between all of the chronic conditions and their logical condition families                                                    |
| [chronic_conditions_long](#!/model/model.tuva_chronic_conditions.tuva_chronic_conditions__chronic_conditions_long#description)   | A table at the patient-condition grain, with a row for every chronic condition identified in the data                                               |
| [chronic_conditions_wide](#!/model/model.tuva_chronic_conditions.tuva_chronic_conditions__chronic_conditions_wide#description)   | A table at the patient grain, with a row for each patient and a column for each chronic condition and a flag indicating if they have that condition |
  

  

---
### 🔗 Quick Links
- [Learn more about Chronic Conditions](https://thetuvaproject.com/docs/analytics/chronic-conditions) at the Tuva Project
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/tuva_chronic_conditions/latest/) or [GitHub](https://github.com/tuva-health/tuva_chronic_conditions)


{% enddocs %}



{% docs __pmpm__ %}
# 📦 PMPM

PMPM (cost per member per month) is a common healthcare financial benchmark, which calculates the average cost of healthcare per member enrolled per month. 

  


### 📊 What does this package produce?

This package produces one table, whcih can be used to build PMPM calculations and perform related analytics

| Table                                                                                                                | Definition                                                                                                                           |
|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [pmpm_builder](#!/model/model.pmpm.pmpm__pmpm_builder#description) | A table at the patient-month grain, with a row for month every patient has coverage, and the total and detailed spend for that month |
  

  

---
### 🔗 Quick Links
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)
- Find this repo on [dbt hub](https://hub.getdbt.com/tuva-health/pmpm/latest/) or [GitHub](https://github.com/tuva-health/pmpm)


{% enddocs %}



{% docs __claims_data_model__ %}
# 📦 Claims Data Model

The first step to using The Tuva Project is to map your claims data to the Claims Data Model. The Tuva Project expects your project to have three models containing your [medical_claim](#!/model/model.claims_data_model.medical_claim#description), [pharmacy_claim](#!/model/model.claims_data_model.pharmacy_claim#description), and [eligibiilty](#!/model/model.claims_data_model.eligibility#description) data in the expected format.   All packages in The Tuva Project ecosystem run off of these core tables.

### 💡 Tips for getting started:
- Read through our [Mappping Guide](https://thetuvaproject.com/docs/claims-data-warehouse/setup) for tips on how to map the models.
- Check out the [Tuva Project Readme](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) for more details on how to get started with the tuva project.
- Use the [Data Profiling](#!/overview/readmissions) package to validate your models, and make sure there aren't any issues that would affect downstream packages or analytics.

  


### 📊 What models do I need to build in order for The Tuva Project to run?

Create the following models in your dbt project:

| Table                                                                         | Definition                                                                                                                                                       |
|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [eligibility](#!/model/model.claims_data_model.eligibility#description)       | A model detailing eligibility spans for all patients in your population                                                                                          |
| [medical_claim](#!/model/model.claims_data_model.medical_claim#description)   | A model containing all of the claim-line level information from your claims including claim type, provider info, charge/payment info, conditions, and procedures |
| [pharmacy_claim](#!/model/model.claims_data_model.pharmacy_claim#description) | A model containing all of the medications prescribed and administered in pharmacy claims                                                                         |
  

  

---
### 🔗 Quick Links
- Read the [Mapping Guide](https://thetuvaproject.com/docs/claims-data-warehouse/setup), for more info on how to get started
- Discover more about [The Tuva Project](https://thetuvaproject.com/), or the parent company [Tuva Health](https://tuvahealth.com/)
- Find The Tuva Project on [dbt hub](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/) or [GitHub](https://github.com/tuva-health/the_tuva_project)


{% enddocs %}

