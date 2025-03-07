WITH latest_test_invocation AS (
    SELECT
        invocation_id,
        generated_at
    FROM (
        SELECT
            invocation_id,
            generated_at,
            ROW_NUMBER() OVER (PARTITION BY command ORDER BY generated_at DESC) AS row_num
        FROM {{ ref('dbt_invocations') }}
        WHERE command = 'test'
    ) ranked_invocations
    WHERE row_num = 1
),

-- Get the test result rows limited to 3 per test result
limited_test_result_rows AS (
    SELECT
        elementary_test_results_id,
        result_row
    FROM (
        SELECT
            elementary_test_results_id,
            result_row,
            ROW_NUMBER() OVER (PARTITION BY elementary_test_results_id ORDER BY result_row) AS row_num
        FROM {{ ref('test_result_rows') }}
    ) ranked_rows
    WHERE row_num <= 3
),

-- Aggregate the limited rows into a string per test result
aggregated_result_rows AS (
    SELECT
        elementary_test_results_id,
        listagg(result_row, ', ') AS aggregated_result_rows
    FROM limited_test_result_rows
    GROUP BY elementary_test_results_id
)

SELECT
    dt.unique_id,
    dt.database_name,
    dt.schema_name,
    etr.table_name,
    dt.name as test_name,
    dt.short_name as test_short_name,
    dt.test_column_name,
    dt.severity,
    dt.warn_if,
    dt.error_if,
    dt.test_params,
    dt.test_original_name,
    dt.tags as test_tags,
    dt.description as test_description,
    dt.package_name as test_package_name,
    dt.type as test_type,
    dt.generated_at,
    dt.metadata_hash,
    dt.quality_dimension,
    etr.detected_at,
    etr.created_at,
    etr.column_name,
    etr.test_sub_type,
    etr.test_results_description,
    etr.test_results_query,
    etr.status,
    etr.failures,
    etr.failed_row_count,
    arr.aggregated_result_rows AS result_rows,

     -- Extract severity level from tags
    CASE
        WHEN dt.tags LIKE '%"tuva_dqi_sev_1"%' THEN 1
        WHEN dt.tags LIKE '%"tuva_dqi_sev_2"%' THEN 2
        WHEN dt.tags LIKE '%"tuva_dqi_sev_3"%' THEN 3
        WHEN dt.tags LIKE '%"tuva_dqi_sev_4"%' THEN 4
        WHEN dt.tags LIKE '%"tuva_dqi_sev_5"%' THEN 5
        ELSE NULL
    END AS severity_level,

    -- Create flag columns for different categories (0 or 1)
    CASE WHEN dt.tags LIKE '%"dqi_service_categories"%' THEN 1 ELSE 0 END AS flag_service_categories,
    CASE WHEN dt.tags LIKE '%"dqi_ccsr"%' THEN 1 ELSE 0 END AS flag_ccsr,
    CASE WHEN dt.tags LIKE '%"dqi_cms_chronic_conditions"%' THEN 1 ELSE 0 END AS flag_cms_chronic_conditions,
    CASE WHEN dt.tags LIKE '%"dqi_tuva_chronic_conditions"%' THEN 1 ELSE 0 END AS flag_tuva_chronic_conditions,
    CASE WHEN dt.tags LIKE '%"dqi_cms_hccs"%' THEN 1 ELSE 0 END AS flag_cms_hccs,
    CASE WHEN dt.tags LIKE '%"dqi_ed_classification"%' THEN 1 ELSE 0 END AS flag_ed_classification,
    CASE WHEN dt.tags LIKE '%"dqi_financial_pmpm"%' THEN 1 ELSE 0 END AS flag_financial_pmpm,
    CASE WHEN dt.tags LIKE '%"dqi_quality_measures"%' THEN 1 ELSE 0 END AS flag_quality_measures,
    CASE WHEN dt.tags LIKE '%"dqi_readmission"%' THEN 1 ELSE 0 END AS flag_readmission

FROM {{ ref('dbt_tests') }} AS dt
LEFT JOIN {{ ref('elementary_test_results') }} AS etr
    ON dt.unique_id = etr.test_unique_id
INNER JOIN latest_test_invocation lti
    ON etr.invocation_id = lti.invocation_id
LEFT JOIN aggregated_result_rows arr
    ON arr.elementary_test_results_id = etr.id
