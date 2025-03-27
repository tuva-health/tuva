/* TO POPULATE RECORDS IN THIS TABLE, MAKE SURE TO RUN "dbt test" command after a full run and seed is completed*/
{{ config(materialized='view', enabled = ((target.type != 'fabric') and var('enable_input_layer_testing', true)) ) }}

with latest_test_invocation as (
    select
        invocation_id
        , generated_at
    from (
        select
            invocation_id
            , generated_at
            , ROW_NUMBER() over (partition by command
order by generated_at desc) as row_num
        from {{ ref('dbt_invocations') }}
        where command = 'test'
    ) as ranked_invocations
    where row_num = 1
)

select
    dt.unique_id
    , dt.database_name
    , dt.schema_name
    , etr.table_name
    , dt.name as test_name
    , dt.short_name as test_short_name
    , dt.test_column_name
    , dt.severity
    , dt.warn_if
    , dt.error_if
    , dt.test_params
    , dt.test_original_name
    , dt.tags as test_tags
    , dt.description as test_description
    , dt.package_name as test_package_name
    , dt.type as test_type
    , dt.generated_at
    , dt.metadata_hash
    , dt.quality_dimension
    , etr.detected_at
    , etr.created_at
    , etr.column_name
    , etr.test_sub_type
    , etr.test_results_description
    , etr.test_results_query
    , etr.status
    , etr.failures
    , etr.failed_row_count

    -- Map test names to categories
    , case

        when dt.tags like '%"dqi_usability"%' then 'usability'

        -- Completeness tests
        when dt.short_name = 'expect_column_to_exist' then 'completeness'
        when dt.short_name = 'not_null' then 'completeness'

        -- Validity tests
        when dt.short_name = 'expect_column_values_to_be_of_type' then 'validity'
        when dt.short_name = 'relationships' then 'validity'
        when dt.short_name = 'expect_column_values_to_match_regex_list' then 'validity'
        when dt.short_name = 'expect_column_values_to_be_in_type_list' then 'validity'
        when dt.short_name = 'accepted_values' then 'validity'
        when dt.short_name = 'expect_column_values_to_match_regex' then 'validity'
        when dt.short_name = 'expect_column_value_lengths_to_be_between' then 'validity'
        when dt.short_name = 'expect_column_unique_value_count_to_be_between' then 'validity'
        when dt.short_name = 'expect_column_value_lengths_to_equal' then 'validity'
        when dt.short_name = 'expect_column_values_to_be_between' then 'validity'

        -- Consistency tests
        when dt.short_name = 'unique' then 'consistency'
        when dt.short_name = 'expect_column_pair_values_A_to_be_greater_than_B' then 'consistency'
        when dt.short_name = 'expect_table_row_count_to_be_between' then 'consistency'
        when dt.short_name = 'unique_combination_of_columns' then 'consistency'

        -- Default for unmapped tests
        else 'other'
    end as test_category

     -- Extract severity level from tags
    , case
        when dt.tags like '%"tuva_dqi_sev_1"%' then 1
        when dt.tags like '%"tuva_dqi_sev_2"%' then 2
        when dt.tags like '%"tuva_dqi_sev_3"%' then 3
        when dt.tags like '%"tuva_dqi_sev_4"%' then 4
        when dt.tags like '%"tuva_dqi_sev_5"%' then 5
        else null
    end as severity_level

    -- Create flag columns for different categories (0 or 1)
    , case when dt.tags like '%"dqi_service_categories"%' then 1 else 0 end as flag_service_categories
    , case when dt.tags like '%"dqi_ccsr"%' then 1 else 0 end as flag_ccsr
    , case when dt.tags like '%"dqi_cms_chronic_conditions"%' then 1 else 0 end as flag_cms_chronic_conditions
    , case when dt.tags like '%"dqi_tuva_chronic_conditions"%' then 1 else 0 end as flag_tuva_chronic_conditions
    , case when dt.tags like '%"dqi_cms_hccs"%' then 1 else 0 end as flag_cms_hccs
    , case when dt.tags like '%"dqi_ed_classification"%' then 1 else 0 end as flag_ed_classification
    , case when dt.tags like '%"dqi_financial_pmpm"%' then 1 else 0 end as flag_financial_pmpm
    , case when dt.tags like '%"dqi_quality_measures"%' then 1 else 0 end as flag_quality_measures
    , case when dt.tags like '%"dqi_readmission"%' then 1 else 0 end as flag_readmission

from {{ ref('dbt_tests') }} as dt
left outer join {{ ref('elementary_test_results') }} as etr
    on dt.unique_id = etr.test_unique_id
inner join latest_test_invocation as lti
    on etr.invocation_id = lti.invocation_id
