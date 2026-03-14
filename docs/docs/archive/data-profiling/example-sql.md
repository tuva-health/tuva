---
id: example-sql
title: "Example SQL"
---

<details>
    <summary>Summarize all data quality issues by Input Layer table and test_category</summary>

    In the example output above, you can see summary results of data quality for each table (column 1)  and each test category.  For example, on row 8 you can see that 8,235 claims from medical_claim had invalid values.  On row 10 you can see 66,196 claims had no data quality issues of any sort (i.e. their test category label was 'good').  
    
    ```sql
    select *
    from data_profiling.summary
    order by 1,3
    ```
    
    ![Data Profiling Summary](/img/data_profiling_summary.jpg)

</details>

<details>
    <summary>Drill into specific data profiling test results</summary>

    In this query you can drill into the specific data quality test results.  This table is populated for every test that had at least 1 failure.  For example, on row 11 you can see ms_drg has invalid values on 100% of claims.
    
    ```sql
    select *
    from data_profiling.test_result
    order by 1,3,4
    ```
    
    ![Data Profiling Test Results](/img/data_profiling_test_results.jpg)

</details>

<details>
    <summary>Drill into specific records in the data with data quality issues</summary>

    With this query you can drill into the specific records (e.g. claims) that have data quality issues.  For example, the following query will return all the claim_ids that had an invalid ms_drg_code value.  You can easily join from this table back to your actual claims data in medical_claim to see what values are the issue.
    
    ```sql
    select *
    from data_profiling.test_detail
    where test_name = 'ms_drg_code invalid'
    ```

</details>
