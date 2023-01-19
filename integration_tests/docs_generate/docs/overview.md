{% docs __overview__ %}
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