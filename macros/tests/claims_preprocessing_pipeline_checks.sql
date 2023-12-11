{#
    Collecting failures for pipeline data quality checks that must pass before
    Core and downstream marts are built.
#}

{% test claims_preprocessing_pipeline_checks(model) %}

    select *
    from {{ ref('data_quality__claims_preprocessing_test_detail' )}}
    where pipeline_test = 1

{% endtest %}