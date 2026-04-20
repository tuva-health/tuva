{{ dq_config_analytical_metric_model('analytical_key_metric__inpatient_mortality_rate') }}

{% set core_encounter_rel = dq_analytical_relation('core__encounter') %}

{% if execute and core_encounter_rel is not none %}
    select
          sources.data_source
        , 'acute inpatient' as domain
        , 'inpatient mortality rate' as metric
        , {{ dq_analytical_decimal_result_sql("coalesce(mortality.result, 0)") }} as result
    from (
        {{ dq_source_dimension_sql(core_encounter_rel) }}
    ) as sources
    left join (
        select
              coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
            , case
                when count(*) = 0 then 0
                else (
                    cast(sum(
                        case
                            when discharge_disposition_code in ('20', '40', '41', '42') then 1
                            else 0
                        end
                    ) as {{ dbt.type_numeric() }})
                    / cast(count(*) as {{ dbt.type_numeric() }})
                ) * 100
              end as result
        from {{ core_encounter_rel }}
        where encounter_type = 'acute inpatient'
          and discharge_disposition_code is not null
          and encounter_end_date is not null
        group by 1
    ) as mortality
        on sources.data_source_key = mortality.data_source_key
{% else %}
    {{ dq_analytical_empty_result_sql() }}
{% endif %}
