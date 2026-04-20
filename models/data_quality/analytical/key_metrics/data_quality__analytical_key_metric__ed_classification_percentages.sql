{{ dq_config_analytical_metric_model('analytical_key_metric__ed_classification_percentages') }}

{% set ed_classification_rel = dq_analytical_relation('ed_classification__summary') %}

{% if execute and ed_classification_rel is not none %}
    select
          classified.data_source
        , 'ed classification' as domain
        , {{ concat_custom([
            "'percent of ed encounters | '",
            "classified.classification"
          ]) }} as metric
        , {{ dq_analytical_decimal_result_sql(
            "case when totals.total_encounters = 0 then null else (cast(classified.encounters as " ~ dbt.type_numeric() ~ ") / cast(totals.total_encounters as " ~ dbt.type_numeric() ~ ")) * 100 end"
          ) }} as result
    from (
        select
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , coalesce(
                cast(ed_classification_description as {{ dbt.type_string() }}),
                cast('Not Classified' as {{ dbt.type_string() }})
              ) as classification
            , count(*) as encounters
        from {{ ed_classification_rel }}
        group by 1, 2
    ) as classified
    inner join (
        select
              cast(data_source as {{ dbt.type_string() }}) as data_source
            , count(*) as total_encounters
        from {{ ed_classification_rel }}
        group by 1
    ) as totals
        on classified.data_source = totals.data_source
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
