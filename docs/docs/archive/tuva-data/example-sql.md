---
id: example-sql
title: "Example SQL"
---

The following SQL examples are designed to run against the Tuva data model (core and data marts).  

## Patient Demographics

<details>
    <summary>Number of Unique Patients</summary>

    ```sql
    select count(distinct patient_id)
    from core.patient
    ```

</details>

<details>
    <summary>Distribution of Patients by Age Group</summary>

    ```sql
    with patient_age as (
    select
        patient_id
    ,   floor(datediff(day, birth_date, current_date)/365) as age
    from core.patient
    )

    , age_groups as (
    select
        patient_id
    ,   age
    ,   case 
            when age <= 0 and age < 2 then '0-2'
            when age <= 2 and age < 18 then '2-18'
            when age <= 18 and age < 30 then '18-30'
            when age <= 30 and age < 40 then '30-40'
            when age <= 40 and age < 50 then '40-50'
            when age <= 50 and age < 60 then '50-60'
            when age <= 60 and age < 70 then '60-70'
            when age <= 70 and age < 80 then '70-80'
            when age <= 80 and age < 90 then '80-90'
            when age > 90 then '> 90'
            else 'Missing Age' 
        end as age_group
    from patient_age
    )
    
    select
        age_group
    ,   count(distinct patient_id) as patients
    ,   cast(100 * count(distinct patient_id)/sum(count(distinct patient_id)) over() as numeric(38,1)) as percent
    from age_groups
    group by age_group
    order by 1
    ;
    ```
    <!-- ![Patients Age Group](/img/example-sql/patients-age-group.jpg) -->

</details>

<details>
    <summary>Distribution of Patients by Gender</summary>

    ```sql
    select
        sex
    ,    count(1)
    from core.patient
    group by 1
    ;
    ```

</details>

<details>
    <summary>Distribution of Patients by Race</summary>

    ```sql
    select
        race
    ,    count(1)
    from core.patient
    group by 1
    ;
    ```

</details>

## Basic Claims Analytics

<details>
    <summary>Number of Distinct Claims</summary>

    ```sql
    select
        claim_type
    ,   count(distinct claim_id)
    from core.medical_claim
    group by 1
    
    union
    
    select 
        'pharmacy' as claim_type
    ,   count(distinct claim_id)
    from core.pharmacy_claim
    ;
    ```

</details>

<details>
    <summary>Distribution of Claims and Payments by Claim Type</summary>

    ```sql
    select
        claim_type
    ,   count(distinct claim_id) as distinct_claims
    ,   sum(paid_amount) as total_payments
    from core.medical_claim
    group by 1
    ;
    ```
    <!-- ![Claims by Claim Type](/img/example-sql/claim-count.jpg) -->

</details>

<details>
    <summary>Distribution of Claims and Payments by Service Category</summary>

    ```sql
    select
        service_category_1
    ,   service_category_2
    ,   count(distinct claim_id) as distinct_claims
    ,   sum(paid_amount) as total_payments
    from core.medical_claim
    group by 1,2
    order by 1,2
    ;
    ```
    <!-- ![Claims by Service Category](/img/example-sql/claims-by-service-category.jpg) -->

</details>

## Basic Encounter Analytics

<details>
    <summary>Volume and Average Cost of Encounters by Type</summary>

    ```sql
    select 
      encounter_type
    , count(distinct encounter_id) as encounters
    , avg(paid_amount) as avg_cost
    from core.encounter
    group by 1
    ```

</details>

<details>
    <summary>Monthly Trends of Encounters by Type</summary>

    ```sql
    select 
      date_part(year, encounter_start_date) || lpad(date_part(month, encounter_start_date),2,0) as year_month
    , count(distinct encounter_id) as encounters
    from core.encounter
    group by 1
    order by 1
    ```
</details>

## Chronic Conditions

<details>
    <summary>Top 10 most prevalent chronic conditions</summary>

    ```sql
    select
        condition
    ,   count(distinct patient_id) as total_patients
    ,   cast(count(distinct patient_id) * 100.0 / (select count(distinct patient_id) from core.patient) as numeric(38,2)) as percent_of_patients
    From chronic_conditions.tuva_chronic_conditions_long
    group by 1
    order by 2 desc
    limit 10
    ```
    The following is example output from this query from the Tuva Claims Demo dataset.  
    
    ![Tuva Condition Prevalence](/img/tuva_condition_prevalence.jpg)

</details>

<details>
    <summary>New patients diagnosed with type 2 diabetes by month</summary>

    ```sql
    with first_month_diabetes as (
    select
      patient_id
    , 'Type 2 Diabetes' as condition
    , min(first_diagnosis_date) as start_date
    from chronic_conditions.tuva_chronic_conditions_long
    where condition in ('Type 2 Diabetes')
    group by patient_id
    )
    
    select 
      condition
    , year(start_date) as year
    , month(start_date) as month
    , count(*) as count
    From first_month_diabetes
    group by 1,2,3
    order by 2 desc, 3 desc
    
    ```
    The following is example output from this query from the Tuva Claims Demo dataset.  
    
    ![The Tuva Project](/img/chronic_conditions/TCC-new_diabetes_by_month.png)
</details>

## Risk Scores

<details>
    <summary>Top 10 most prevalent risk factors</summary>

    ```sql
    select
          risk_factor_description
        , count(*)
    from cms_hcc.patient_risk_factors
    group by risk_factor_description
    order by count(*) desc
    limit 10;
    ```

</details>

<details>
    <summary>Top 10 most prevalent HCCs</summary>

    ```sql
    select
          hcc_code
        , count(*)
    from cms_hcc._int_hcc_hierarchy
    group by hcc_code
    order by count(*) desc
    limit 10;
    ```

</details>

<details>
    <summary>Average risk score</summary>

    ```sql
    select
          avg(raw_risk_score) as average_raw_risk_score
        , avg(normalized_risk_score) as average_normalized_risk_score
        , avg(payment_risk_score) as average_payment_risk_score
    from cms_hcc.patient_risk_scores;
    ```

</details>

## Readmissions

<details>
    <summary>Basic readmission statistics</summary>

    ```sql
    -- Simple readmission statistics
    select 
        1 as id
    ,   'Index Admissions' as measure
    ,   count(1) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1
    
    union all
    
    select 
        2 as id
    ,   'Unplanned 30-day Readmissions' as measure
    ,   count(1) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
        
    union all
    
    select 
        3 as id
    ,   'Avg Days to Readmission' as measure
    ,   avg(days_to_readmit) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
    
    union all
    
    select 
        4 as id
    ,   'Readmission Avg Length of Stay' as measure
    ,   avg(readmission_length_of_stay) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
        
    union all
    
    select 
        5 as id
    ,   'Readmission Mortalities' as measure
    ,   sum(died_flag) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
        
    union all
    
    select 
        6 as id
    ,   'Readmission Avg Paid Amount' as measure
    ,   cast(avg(paid_amount) as numeric(38,0)) as value
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
    order by 1
    ```
    
    The following output is obtained by running the above query on the Tuva Claims Demo dataset.
    
    ![The Tuva Project](/img/readmissions/basic_stats.jpg)
</details>


<details>
    <summary>Trending hospital-wide readmissions</summary>

    ```sql
    -- readmission rate by month
    with index_admissions as (
    select
        date_part(year, discharge_date) || '-' || lpad(date_part(month, discharge_date),2,0) as year_month
    ,   count(1) as index_admissions
    from readmissions.readmission_summary
    where index_admission_flag = 1
    group by 1
    )
    
    , readmissions as (
    select 
        date_part(year, discharge_date) || '-' || lpad(date_part(month, discharge_date),2,0) as year_month
    ,   count(1) as readmissions
    from readmissions.readmission_summary
    where index_admission_flag = 1 
        and unplanned_readmit_30_flag = 1
    group by 1
    )
    
    select
        a.year_month
    ,   a.index_admissions
    ,   coalesce(b.readmissions,0) as readmissions
    ,   cast(coalesce(b.readmissions,0) / a.index_admissions as numeric(38,2)) as readmission_rate
    from index_admissions a
    left join readmissions b
        on a.year_month = b.year_month
    order by 1
    ```
    The following output is generated by running the above query on the Tuva Claims Demo dataset.  The results are sparse for this dataset (there are only 5 total readmissions) but you can get a sense of the structure of the table and how you might use it against your data.
    
    ![The Tuva Project](/img/readmissions/readmission_rate_monthly.jpg)

</details>

<details>
    <summary>Readmissions data quality issues</summary>
    There are several types of data quality issues that can prevent a hospitalization from qualifying as an index admission or from being part of a readmission measure.  Data quality checks for these issues are built into the Tuva Project's readmission mart.  The query below reports the total number of inpatient encounters and the number of encounters that fail any particular data quality check.
    
    ```sql
    -- readmission data quality issues
    with dq_stats as (
    select 
        cast(count(1) as int) as total_encounters
    ,   cast(sum(disqualified_encounter_flag) as int) as disqualified_encounters
    ,   cast(sum(missing_admit_date_flag) as int) as missing_admit_date
    ,   cast(sum(missing_discharge_date_flag) as int) as missing_discharge_date
    ,   cast(sum(admit_after_discharge_flag) as int) as admit_after_discharge_date
    ,   cast(sum(missing_discharge_disposition_code_flag) as int) as missing_discharge_disposition
    ,   cast(sum(invalid_discharge_disposition_code_flag) as int) as invalid_discharge_disposition
    ,   cast(sum(missing_primary_diagnosis_flag) as int) as missing_primary_diagnosis
    ,   cast(sum(multiple_primary_diagnoses_flag) as int) as multiple_primary_diagnoses
    ,   cast(sum(invalid_primary_diagnosis_code_flag) as int) as invalid_primary_diagnosis
    ,   cast(sum(no_diagnosis_ccs_flag) as int) as no_diagnosis_ccs
    ,   cast(sum(overlaps_with_another_encounter_flag) as int) as overlapping_encounter
    ,   cast(sum(missing_ms_drg_flag) as int) as missing_ms_drg
    ,   cast(sum(invalid_ms_drg_flag) as int) as invalid_ms_drg
    from readmissions.encounter_augmented
    )
    select 
        measure
    ,   number_of_encounters
    from dq_stats
    unpivot(number_of_encounters for measure in (total_encounters,
                                         disqualified_encounters,
                                         missing_admit_date,
                                         missing_discharge_date,
                                         admit_after_discharge_date,
                                         missing_discharge_disposition,
                                         invalid_discharge_disposition,
                                         missing_primary_diagnosis,
                                         multiple_primary_diagnoses,
                                         invalid_primary_diagnosis,
                                         no_diagnosis_ccs,
                                         overlapping_encounter,
                                         missing_ms_drg,
                                         invalid_ms_drg                                     
                                        ))
    
    ```
    
    The following is example output from this query from the Tuva Claims Demo dataset.  You can see there are a total of 223 inpatient encounters in the dataset, 79 of which are excluded from readmission analytics due to data quality issues.  You can then see the specific reasons for the exclusion (i.e. missing primary diagnosis and overlapping encounter).
    
    ![The Tuva Project](/img/readmissions/data_quality_issues.jpg)
</details>
