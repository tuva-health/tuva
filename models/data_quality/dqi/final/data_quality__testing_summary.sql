/* TO POPULATE RECORDS IN THIS TABLE, MAKE SURE TO RUN "dbt test" command after a full run and seed is completed*/
{{ config(materialized='view', enabled = ((target.type != 'fabric') and var('enable_input_layer_testing', true)) ) }}

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

    -- Map test names to categories
    CASE

        WHEN dt.tags LIKE '%"dqi_usability"%' THEN 'usability'

        -- Completeness tests
        WHEN dt.short_name = 'expect_column_to_exist' THEN 'completeness'
        WHEN dt.short_name = 'not_null' THEN 'completeness'

        -- Validity tests
        WHEN dt.short_name = 'expect_column_values_to_be_of_type' THEN 'validity'
        WHEN dt.short_name = 'relationships' THEN 'validity'
        WHEN dt.short_name = 'expect_column_values_to_match_regex_list' THEN 'validity'
        WHEN dt.short_name = 'expect_column_values_to_be_in_type_list' THEN 'validity'
        WHEN dt.short_name = 'accepted_values' THEN 'validity'
        WHEN dt.short_name = 'expect_column_values_to_match_regex' THEN 'validity'
        WHEN dt.short_name = 'expect_column_value_lengths_to_be_between' THEN 'validity'
        WHEN dt.short_name = 'expect_column_unique_value_count_to_be_between' THEN 'validity'
        WHEN dt.short_name = 'expect_column_value_lengths_to_equal' THEN 'validity'
        WHEN dt.short_name = 'expect_column_values_to_be_between' THEN 'validity'

        -- Consistency tests
        WHEN dt.short_name = 'unique' THEN 'consistency'
        WHEN dt.short_name = 'expect_column_pair_values_A_to_be_greater_than_B' THEN 'consistency'
        WHEN dt.short_name = 'expect_table_row_count_to_be_between' THEN 'consistency'
        WHEN dt.short_name = 'unique_combination_of_columns' THEN 'consistency'

        -- Default for unmapped tests
        ELSE 'other'
    END AS test_category,

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
INNER JOIN latest_test_invocation AS lti
    ON etr.invocation_id = lti.invocation_id
