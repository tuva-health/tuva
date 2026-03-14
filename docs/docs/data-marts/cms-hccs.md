---
id: cms-hccs
title: "CMS-HCCs"
---

## Methods

[Code on Github](https://github.com/tuva-health/tuva/tree/main/models/data_marts/cms_hcc)

The CMS-HCC data mart implements v24 and v28 versions of the CMS-HCC risk model.  The full documentation of these models can be found on CMS's website.

There are many tedious steps to map HCCs and calculate risk scores. Most of the critical information is not easy to use since CMS distributes rate announcements annually in PDFs and mappings in Excel files. Many existing tools, such as the SAS program from CMS, require you to have the patient data preprocessed.

Additionally, the new CMS-HCC model V28 will be phased in over three years, requiring organizations to run both models V24 and V28 to create blended risk scores.

* Payment year 2024 risk scores will be blended using 67% of the risk score calculated from V24 and 33% from V28.
* Payment year 2025 risk scores will be blended using 33% of the risk score calculated from V24 and 67% from V28.
* Beginning in payment year 2026 risk scores will be 100% from V28.

In the diagram below we provide an overview explanation of how the data mart works.

<iframe width="780" height="520" src="https://miro.com/app/live-embed/uXjVNq_Lq74=/?moveToViewport=-555,-812,2164,1037&embedId=161883269913" frameborder="0" scrolling="no" allow="fullscreen; clipboard-read; clipboard-write" allowfullscreen></iframe>

In order to run the CMS-HCC data mart you need to map the following data elements to the [Input Layer](../input-layer).  These are the only data elements required.

**Eligibility:**
- person_id
- gender
- birth_date
- death_date
- enrollment_start_date
- enrollment_end_date
- original_reason_entitlement_code
- dual_status_code
- medicare_status_code 

**Medical claim:**
- claim_id
- claim_line_number
- claim_type
- person_id
- claim_start_date
- claim_end_date
- bill_type_code
- hcpcs_code
- diagnosis_code_type
- diagnosis_code_1* 

**Up to 25 diagnosis codes are allowable, but only 1 is required.*

The data mart includes logic that allows you to choose which payment year you want to use to calculate the risk scores. You can also use the snapshot functionality to capture the risk scores calculated for each payment, or on a month-to-month basis.

- `cms_hcc_payment_year` defaults to the current year
- `snapshots_enabled` is an *optional* variable that can be enabled to allow
  running the mart for multiple years

To run the data mart, simply update the payment year in your dbt_project.yml file or use the `--vars` dbt command, if you want to change the payment year from the current year default.

dbt_project.yml:

```yaml
vars:
    cms_hcc_payment_year: 2020
    snapshots_enabled: true
```

dbt command:

```bash
# Uses defaults or vars from project yml, runs all marts
dbt build

# Runs only the CMS HCC mart using defaults or vars from project yml
dbt build --select tag:cms_hcc

# Overrides vars from project yml, executes snapshots
dbt build --select tag:cms_hcc --vars '{cms_hcc_payment_year: 2020, snapshots_enabled: true}'
```

## Example SQL

<details>
  <summary>Average CMS-HCC Risk Scores</summary>

```sql
select
    count(distinct person_id) as patient_count
    , avg(blended_risk_score) as average_blended_risk_score
    , avg(normalized_risk_score) as average_normalized_risk_score
    , avg(payment_risk_score) as average_payment_risk_score
from cms_hcc.patient_risk_scores
```
</details>

<details>
  <summary>Average CMS-HCC Risk Scores by Patient Location</summary>

```sql
select
      patient.state
    , patient.city
    , patient.zip_code
    , avg(risk.payment_risk_score) as average_payment_risk_score
from cms_hcc.patient_risk_scores as risk
    inner join core.patient as patient
        on risk.person_id = patient.person_id
group by
      patient.state
    , patient.city
    , patient.zip_code;
```
</details>

<details>
  <summary>Rolling Monthly Average CMS-HCC Risk Scores</summary>

```sql
select
       collection_end_date
     , payment_year
     , avg(payment_risk_score) as average_payment_risk_score
from cms_hcc.patient_risk_scores_monthly
group by
      collection_start_date
    , collection_end_date
    , payment_year
order by
      collection_end_date
    , payment_year;
```
</details>

<details>
  <summary>Distribution of CMS-HCC Risk Factors</summary>

```sql
select
      risk_factor_description
    , count(*) as total
    , cast(100 * count(*)/sum(count(*)) over() as numeric(38,1)) as percent
from cms_hcc.patient_risk_factors
group by risk_factor_description
order by 2 desc
```
</details>

<details>
  <summary>Risk Weighted by Member Months</summary>

```sql
select sum(payment_risk_score_weighted_by_months) / sum(member_months) as weighted_risk_total
from cms_hcc.patient_risk_scores;
```
</details>

<details>
  <summary>Stratified CMS-HCC Risk Scores</summary>

```sql
select
      (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score <= 1.00) as low_risk
    , (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score = 1.00) as average_risk
    , (select count(*) from cms_hcc.patient_risk_scores where payment_risk_score > 1.00) as high_risk
    , (select avg(payment_risk_score) from cms_hcc.patient_risk_scores) as total_population_average;
```
</details>

<details>
  <summary>Total HCC Conditions</summary>

```sql
select
      risk_factor_description
    , count(*) patient_count
from cms_hcc.patient_risk_factors
where factor_type = 'Disease'
group by risk_factor_description
order by count(*) desc
```
</details>
