{{ config(
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'analytical_key_metrics',
     tags = ['data_quality', 'dqi'],
     materialized = 'table'
   )
}}

{% set dependency_names = [
    'core__patient',
    'core__medical_claim',
    'core__pharmacy_claim',
    'core__member_months',
    'core__encounter',
    'core__eligibility',
    'readmissions__readmission_summary',
    'readmissions__encounter_augmented',
    'ed_classification__summary',
    'chronic_conditions__tuva_chronic_conditions_long',
    'cms_hcc__patient_risk_factors'
] %}

{% for dependency_name in dependency_names %}
-- depends_on: {{ ref(dependency_name) }}
{% endfor %}

{% if execute %}
    {% set core_patient_node = dq_find_model_node('core__patient') %}
    {% set core_medical_claim_node = dq_find_model_node('core__medical_claim') %}
    {% set core_pharmacy_claim_node = dq_find_model_node('core__pharmacy_claim') %}
    {% set core_member_months_node = dq_find_model_node('core__member_months') %}
    {% set core_encounter_node = dq_find_model_node('core__encounter') %}
    {% set core_eligibility_node = dq_find_model_node('core__eligibility') %}
    {% set readmission_summary_node = dq_find_model_node('readmissions__readmission_summary') %}
    {% set readmission_augmented_node = dq_find_model_node('readmissions__encounter_augmented') %}
    {% set ed_classification_node = dq_find_model_node('ed_classification__summary') %}
    {% set chronic_conditions_node = dq_find_model_node('chronic_conditions__tuva_chronic_conditions_long') %}
    {% set cms_hcc_node = dq_find_model_node('cms_hcc__patient_risk_factors') %}

    {% set core_patient_rel = dq_actual_relation(core_patient_node) %}
    {% set core_medical_claim_rel = dq_actual_relation(core_medical_claim_node) %}
    {% set core_pharmacy_claim_rel = dq_actual_relation(core_pharmacy_claim_node) %}
    {% set core_member_months_rel = dq_actual_relation(core_member_months_node) %}
    {% set core_encounter_rel = dq_actual_relation(core_encounter_node) %}
    {% set core_eligibility_rel = dq_actual_relation(core_eligibility_node) %}
    {% set readmission_summary_rel = dq_actual_relation(readmission_summary_node) %}
    {% set readmission_augmented_rel = dq_actual_relation(readmission_augmented_node) %}
    {% set ed_classification_rel = dq_actual_relation(ed_classification_node) %}
    {% set chronic_conditions_rel = dq_actual_relation(chronic_conditions_node) %}
    {% set cms_hcc_rel = dq_actual_relation(cms_hcc_node) %}

    {% set metric_queries = [] %}

    {% if core_patient_rel is not none %}
        {% set patient_metrics_query %}
            select
                  sources.data_source
                , 'patient demographics' as domain
                , 'count distinct patients' as metric
                , cast(coalesce(patient_counts.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_patient_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(distinct person_id) as result
                from {{ core_patient_rel }}
                group by 1
            ) as patient_counts
                on sources.data_source_key = patient_counts.data_source_key

            union all

            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , 'patient demographics' as domain
                , {{ concat_custom([
                    "'count distinct patients | sex = '",
                    "coalesce(cast(sex as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
                  ]) }} as metric
                , cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as result
            from {{ core_patient_rel }}
            group by 1, 3

            union all

            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , 'patient demographics' as domain
                , {{ concat_custom([
                    "'count distinct patients | age_group = '",
                    "coalesce(cast(age_group as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
                  ]) }} as metric
                , cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as result
            from {{ core_patient_rel }}
            group by 1, 3

            union all

            select
                  sources.data_source
                , 'patient demographics' as domain
                , 'count dead' as metric
                , cast(coalesce(dead_counts.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_patient_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(
                        case
                            when coalesce(death_flag, 0) = 1 or death_date is not null then 1
                            else 0
                        end
                      ) as result
                from {{ core_patient_rel }}
                group by 1
            ) as dead_counts
                on sources.data_source_key = dead_counts.data_source_key
        {% endset %}
        {% do metric_queries.append(patient_metrics_query) %}
    {% endif %}

    {% if core_medical_claim_rel is not none %}
        {% set medical_claim_metrics_query %}
            select
                  sources.data_source
                , 'basic claims' as domain
                , 'sum paid_amount in core.medical_claim' as metric
                , cast(coalesce(paid_totals.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_medical_claim_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(paid_amount), 0) as result
                from {{ core_medical_claim_rel }}
                group by 1
            ) as paid_totals
                on sources.data_source_key = paid_totals.data_source_key

            union all

            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , 'basic claims' as domain
                , {{ concat_custom([
                    "'sum paid_amount by claim_type | '",
                    "coalesce(cast(claim_type as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
                  ]) }} as metric
                , cast(coalesce(sum(paid_amount), 0) as {{ dbt.type_numeric() }}) as result
            from {{ core_medical_claim_rel }}
            group by 1, 3

            union all

            select
                  sources.data_source
                , 'basic claims' as domain
                , 'sum allowed_amount in core.medical_claim' as metric
                , cast(coalesce(allowed_totals.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_medical_claim_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(allowed_amount), 0) as result
                from {{ core_medical_claim_rel }}
                group by 1
            ) as allowed_totals
                on sources.data_source_key = allowed_totals.data_source_key

            union all

            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , 'basic claims' as domain
                , {{ concat_custom([
                    "'sum allowed_amount by claim_type | '",
                    "coalesce(cast(claim_type as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
                  ]) }} as metric
                , cast(coalesce(sum(allowed_amount), 0) as {{ dbt.type_numeric() }}) as result
            from {{ core_medical_claim_rel }}
            group by 1, 3

            union all

            select
                  sources.data_source
                , 'basic claims' as domain
                , 'count distinct claim_id in core.medical_claim' as metric
                , cast(coalesce(claim_counts.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_medical_claim_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(distinct claim_id) as result
                from {{ core_medical_claim_rel }}
                group by 1
            ) as claim_counts
                on sources.data_source_key = claim_counts.data_source_key

            union all

            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , 'basic claims' as domain
                , {{ concat_custom([
                    "'count distinct claim_id by claim_type | '",
                    "coalesce(cast(claim_type as " ~ dbt.type_string() ~ "), cast('unknown' as " ~ dbt.type_string() ~ "))"
                  ]) }} as metric
                , cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as result
            from {{ core_medical_claim_rel }}
            group by 1, 3
        {% endset %}
        {% do metric_queries.append(medical_claim_metrics_query) %}
    {% endif %}

    {% if core_pharmacy_claim_rel is not none %}
        {% set pharmacy_claim_metrics_query %}
            select
                  sources.data_source
                , 'basic claims' as domain
                , 'sum paid_amount in core.pharmacy_claim' as metric
                , cast(coalesce(paid_totals.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(paid_amount), 0) as result
                from {{ core_pharmacy_claim_rel }}
                group by 1
            ) as paid_totals
                on sources.data_source_key = paid_totals.data_source_key

            union all

            select
                  sources.data_source
                , 'basic claims' as domain
                , 'sum allowed_amount in core.pharmacy_claim' as metric
                , cast(coalesce(allowed_totals.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , coalesce(sum(allowed_amount), 0) as result
                from {{ core_pharmacy_claim_rel }}
                group by 1
            ) as allowed_totals
                on sources.data_source_key = allowed_totals.data_source_key
        {% endset %}
        {% do metric_queries.append(pharmacy_claim_metrics_query) %}
    {% endif %}

    {% if core_member_months_rel is not none %}
        {% set member_month_metrics_query %}
            select
                  sources.data_source
                , 'basic enrollment' as domain
                , 'total member months' as metric
                , cast(coalesce(total_member_months.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as result
                from {{ core_member_months_rel }}
                group by 1
            ) as total_member_months
                on sources.data_source_key = total_member_months.data_source_key

            union all

            select
                  sources.data_source
                , 'basic enrollment' as domain
                , 'avg member months' as metric
                , cast(coalesce(member_month_averages.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      member_month_counts.data_source_key
                    , avg(cast(member_month_counts.member_months as {{ dbt.type_numeric() }})) as result
                from (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , person_id
                        , count(*) as member_months
                    from {{ core_member_months_rel }}
                    group by 1, 2
                ) as member_month_counts
                group by 1
            ) as member_month_averages
                on sources.data_source_key = member_month_averages.data_source_key

            union all

            select
                  sources.data_source
                , 'basic enrollment' as domain
                , 'max member months' as metric
                , cast(coalesce(member_month_maxima.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      member_month_counts.data_source_key
                    , max(member_month_counts.member_months) as result
                from (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , person_id
                        , count(*) as member_months
                    from {{ core_member_months_rel }}
                    group by 1, 2
                ) as member_month_counts
                group by 1
            ) as member_month_maxima
                on sources.data_source_key = member_month_maxima.data_source_key
        {% endset %}
        {% do metric_queries.append(member_month_metrics_query) %}
    {% endif %}

    {% set claims_member_queries = [] %}
    {% set claim_month_queries = [] %}

    {% if core_medical_claim_rel is not none %}
        {% set medical_claim_members_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
            from {{ core_medical_claim_rel }}
        {% endset %}
        {% do claims_member_queries.append(medical_claim_members_query) %}

        {% set medical_claim_months_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
                , {{ year_month('claim_start_date') }} as year_month
            from {{ core_medical_claim_rel }}
            where claim_start_date is not null
        {% endset %}
        {% do claim_month_queries.append(medical_claim_months_query) %}
    {% endif %}

    {% if core_pharmacy_claim_rel is not none %}
        {% set pharmacy_claim_members_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
            from {{ core_pharmacy_claim_rel }}
        {% endset %}
        {% do claims_member_queries.append(pharmacy_claim_members_query) %}

        {% set pharmacy_claim_months_query %}
            select distinct
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , person_id
                , {{ year_month('dispensing_date') }} as year_month
            from {{ core_pharmacy_claim_rel }}
            where dispensing_date is not null
        {% endset %}
        {% do claim_month_queries.append(pharmacy_claim_months_query) %}
    {% endif %}

    {% if claims_member_queries | length > 0 and core_eligibility_rel is not none %}
        {% set claims_without_enrollment_query %}
            select
                  sources.data_source
                , 'basic enrollment' as domain
                , 'members w/ claims w/o any enrollment' as metric
                , cast(coalesce(missing_members.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                select distinct
                      source_rows.data_source_key
                    , source_rows.data_source
                from (
                    {% if core_medical_claim_rel is not none and core_pharmacy_claim_rel is not none %}
                        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                        union all
                        {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                    {% elif core_medical_claim_rel is not none %}
                        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                    {% else %}
                        {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                    {% endif %}
                ) as source_rows
            ) as sources
            left join (
                select
                      claim_members.data_source_key
                    , count(distinct claim_members.person_id) as result
                from (
                    select
                          coalesce(data_source, '{{ dq_source_key_sentinel() }}') as data_source_key
                        , data_source
                        , person_id
                    from (
                        {{ claims_member_queries | join('\nunion\n') }}
                    ) as claim_members
                ) as claim_members
                left join (
                    select distinct
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , person_id
                    from {{ core_eligibility_rel }}
                ) as eligible_members
                    on claim_members.data_source_key = eligible_members.data_source_key
                    and claim_members.person_id = eligible_members.person_id
                where eligible_members.person_id is null
                group by 1
            ) as missing_members
                on sources.data_source_key = missing_members.data_source_key
        {% endset %}
        {% do metric_queries.append(claims_without_enrollment_query) %}
    {% endif %}

    {% if claim_month_queries | length > 0 and core_member_months_rel is not none %}
        {% set claims_missing_enrollment_month_query %}
            select
                  sources.data_source
                , 'basic enrollment' as domain
                , 'members w/ claims missing enrollment that month' as metric
                , cast(coalesce(missing_months.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                select distinct
                      source_rows.data_source_key
                    , source_rows.data_source
                from (
                    {% if core_medical_claim_rel is not none and core_pharmacy_claim_rel is not none %}
                        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                        union all
                        {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                    {% elif core_medical_claim_rel is not none %}
                        {{ dq_source_dimension_sql(core_medical_claim_rel) }}
                    {% else %}
                        {{ dq_source_dimension_sql(core_pharmacy_claim_rel) }}
                    {% endif %}
                ) as source_rows
            ) as sources
            left join (
                select
                      claim_member_months.data_source_key
                    , count(distinct claim_member_months.person_id) as result
                from (
                    select
                          coalesce(data_source, '{{ dq_source_key_sentinel() }}') as data_source_key
                        , data_source
                        , person_id
                        , year_month
                    from (
                        {{ claim_month_queries | join('\nunion\n') }}
                    ) as claim_member_months
                ) as claim_member_months
                left join (
                    select distinct
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , person_id
                        , year_month
                    from {{ core_member_months_rel }}
                ) as member_months
                    on claim_member_months.data_source_key = member_months.data_source_key
                    and claim_member_months.person_id = member_months.person_id
                    and claim_member_months.year_month = member_months.year_month
                where member_months.person_id is null
                group by 1
            ) as missing_months
                on sources.data_source_key = missing_months.data_source_key
        {% endset %}
        {% do metric_queries.append(claims_missing_enrollment_month_query) %}
    {% endif %}

    {% if core_encounter_rel is not none and core_member_months_rel is not none %}
        {% set encounter_rate_metrics_query %}
            select
                  sources.data_source
                , 'acute inpatient' as domain
                , 'acute inpatient visits per 1,000' as metric
                , cast(
                    case
                        when coalesce(member_month_totals.member_months, 0) = 0 then 0
                        else (
                            cast(coalesce(acute_inpatient_counts.encounter_count, 0) as {{ dbt.type_numeric() }})
                            / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                        ) * 12000
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounter_count
                from {{ core_encounter_rel }}
                where encounter_type = 'acute inpatient'
                group by 1
            ) as acute_inpatient_counts
                on sources.data_source_key = acute_inpatient_counts.data_source_key
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
                on sources.data_source_key = member_month_totals.data_source_key

            union all

            select
                  sources.data_source
                , 'skilled nursing' as domain
                , 'snf visits per 1,000' as metric
                , cast(
                    case
                        when coalesce(member_month_totals.member_months, 0) = 0 then 0
                        else (
                            cast(coalesce(snf_counts.encounter_count, 0) as {{ dbt.type_numeric() }})
                            / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                        ) * 12000
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounter_count
                from {{ core_encounter_rel }}
                where encounter_type = 'inpatient skilled nursing'
                group by 1
            ) as snf_counts
                on sources.data_source_key = snf_counts.data_source_key
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
                on sources.data_source_key = member_month_totals.data_source_key

            union all

            select
                  sources.data_source
                , 'emergency department' as domain
                , 'ed visits per 1,000' as metric
                , cast(
                    case
                        when coalesce(member_month_totals.member_months, 0) = 0 then 0
                        else (
                            cast(coalesce(ed_counts.encounter_count, 0) as {{ dbt.type_numeric() }})
                            / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                        ) * 12000
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounter_count
                from {{ core_encounter_rel }}
                where encounter_type = 'emergency department'
                group by 1
            ) as ed_counts
                on sources.data_source_key = ed_counts.data_source_key
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
                on sources.data_source_key = member_month_totals.data_source_key

            union all

            select
                  sources.data_source
                , 'office visits' as domain
                , 'office visits per 1,000' as metric
                , cast(
                    case
                        when coalesce(member_month_totals.member_months, 0) = 0 then 0
                        else (
                            cast(coalesce(office_counts.encounter_count, 0) as {{ dbt.type_numeric() }})
                            / cast(member_month_totals.member_months as {{ dbt.type_numeric() }})
                        ) * 12000
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                {{ dq_source_dimension_sql(core_member_months_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as encounter_count
                from {{ core_encounter_rel }}
                where encounter_group = 'office based'
                group by 1
            ) as office_counts
                on sources.data_source_key = office_counts.data_source_key
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , count(*) as member_months
                from {{ core_member_months_rel }}
                group by 1
            ) as member_month_totals
                on sources.data_source_key = member_month_totals.data_source_key
        {% endset %}
        {% do metric_queries.append(encounter_rate_metrics_query) %}
    {% endif %}

    {% if core_encounter_rel is not none %}
        {% set acute_inpatient_metrics_query %}
            select
                  sources.data_source
                , 'acute inpatient' as domain
                , 'inpatient alos' as metric
                , cast(coalesce(alos.result, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(core_encounter_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , avg(cast(length_of_stay as {{ dbt.type_numeric() }})) as result
                from {{ core_encounter_rel }}
                where encounter_type = 'acute inpatient'
                group by 1
            ) as alos
                on sources.data_source_key = alos.data_source_key

            union all

            select
                  sources.data_source
                , 'acute inpatient' as domain
                , 'inpatient mortality rate' as metric
                , cast(coalesce(mortality.result, 0) as {{ dbt.type_numeric() }}) as result
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
        {% endset %}
        {% do metric_queries.append(acute_inpatient_metrics_query) %}
    {% endif %}

    {% if readmission_summary_rel is not none and readmission_augmented_rel is not none %}
        {% set readmission_metrics_query %}
            select
                  sources.data_source
                , 'readmissions' as domain
                , 'index admissions' as metric
                , cast(coalesce(readmission_counts.index_admissions, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(readmission_augmented_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
                    , sum(case when summary.index_admission_flag = 1 and summary.unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
                from {{ readmission_summary_rel }} as summary
                inner join {{ readmission_augmented_rel }} as augmented
                    on summary.encounter_id = augmented.encounter_id
                group by 1
            ) as readmission_counts
                on sources.data_source_key = readmission_counts.data_source_key

            union all

            select
                  sources.data_source
                , 'readmissions' as domain
                , '30-day readmissions' as metric
                , cast(coalesce(readmission_counts.readmissions, 0) as {{ dbt.type_numeric() }}) as result
            from (
                {{ dq_source_dimension_sql(readmission_augmented_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
                    , sum(case when summary.index_admission_flag = 1 and summary.unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
                from {{ readmission_summary_rel }} as summary
                inner join {{ readmission_augmented_rel }} as augmented
                    on summary.encounter_id = augmented.encounter_id
                group by 1
            ) as readmission_counts
                on sources.data_source_key = readmission_counts.data_source_key

            union all

            select
                  sources.data_source
                , 'readmissions' as domain
                , '30-day readmission rate' as metric
                , cast(
                    case
                        when coalesce(readmission_counts.index_admissions, 0) = 0 then 0
                        else (
                            cast(readmission_counts.readmissions as {{ dbt.type_numeric() }})
                            / cast(readmission_counts.index_admissions as {{ dbt.type_numeric() }})
                        ) * 100
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                {{ dq_source_dimension_sql(readmission_augmented_rel) }}
            ) as sources
            left join (
                select
                      coalesce(cast(augmented.data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                    , sum(case when summary.index_admission_flag = 1 then 1 else 0 end) as index_admissions
                    , sum(case when summary.index_admission_flag = 1 and summary.unplanned_readmit_30_flag = 1 then 1 else 0 end) as readmissions
                from {{ readmission_summary_rel }} as summary
                inner join {{ readmission_augmented_rel }} as augmented
                    on summary.encounter_id = augmented.encounter_id
                group by 1
            ) as readmission_counts
                on sources.data_source_key = readmission_counts.data_source_key
        {% endset %}
        {% do metric_queries.append(readmission_metrics_query) %}
    {% endif %}

    {% if ed_classification_rel is not none %}
        {% set ed_classification_metrics_query %}
            select
                  classified.data_source
                , 'ed classification' as domain
                , {{ concat_custom([
                    "'percent of ed encounters | '",
                    "classified.classification"
                  ]) }} as metric
                , cast(
                    case
                        when totals.total_encounters = 0 then null
                        else (
                            cast(classified.encounters as {{ dbt.type_numeric() }})
                            / cast(totals.total_encounters as {{ dbt.type_numeric() }})
                        ) * 100
                    end as {{ dbt.type_numeric() }}
                  ) as result
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
        {% endset %}
        {% do metric_queries.append(ed_classification_metrics_query) %}
    {% endif %}

    {% if chronic_conditions_rel is not none and core_patient_rel is not none %}
        {% set chronic_condition_metrics_query %}
            select
                  ranked_conditions.data_source
                , 'top chronic condition prevalence' as domain
                , {{ concat_custom([
                    "'prevalence | '",
                    "ranked_conditions.condition"
                  ]) }} as metric
                , cast(
                    case
                        when patient_totals.total_patients = 0 then null
                        else (
                            cast(ranked_conditions.patient_count as {{ dbt.type_numeric() }})
                            / cast(patient_totals.total_patients as {{ dbt.type_numeric() }})
                        ) * 100
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                select
                      condition_counts.data_source
                    , condition_counts.condition
                    , condition_counts.patient_count
                    , row_number() over (
                        partition by condition_counts.data_source
                        order by condition_counts.patient_count desc, condition_counts.condition
                      ) as condition_rank
                from (
                    select
                          cast(patient.data_source as {{ dbt.type_string() }}) as data_source
                        , cast(chronic_conditions.condition as {{ dbt.type_string() }}) as condition
                        , count(distinct chronic_conditions.person_id) as patient_count
                    from {{ chronic_conditions_rel }} as chronic_conditions
                    inner join {{ core_patient_rel }} as patient
                        on chronic_conditions.person_id = patient.person_id
                    group by 1, 2
                ) as condition_counts
            ) as ranked_conditions
            inner join (
                select
                      cast(data_source as {{ dbt.type_string() }}) as data_source
                    , count(distinct person_id) as total_patients
                from {{ core_patient_rel }}
                group by 1
            ) as patient_totals
                on ranked_conditions.data_source = patient_totals.data_source
            where ranked_conditions.condition_rank <= 10
        {% endset %}
        {% do metric_queries.append(chronic_condition_metrics_query) %}
    {% endif %}

    {% if cms_hcc_rel is not none and core_patient_rel is not none %}
        {% set cms_hcc_metrics_query %}
            select
                  ranked_hcc.data_source
                , 'top hcc prevalence' as domain
                , {{ concat_custom([
                    "'prevalence | '",
                    "ranked_hcc.risk_factor_description"
                  ]) }} as metric
                , cast(
                    case
                        when patient_totals.total_patients = 0 then null
                        else (
                            cast(ranked_hcc.patient_count as {{ dbt.type_numeric() }})
                            / cast(patient_totals.total_patients as {{ dbt.type_numeric() }})
                        ) * 100
                    end as {{ dbt.type_numeric() }}
                  ) as result
            from (
                select
                      hcc_counts.data_source
                    , hcc_counts.risk_factor_description
                    , hcc_counts.patient_count
                    , row_number() over (
                        partition by hcc_counts.data_source
                        order by hcc_counts.patient_count desc, hcc_counts.risk_factor_description
                      ) as hcc_rank
                from (
                    select
                          cast(patient.data_source as {{ dbt.type_string() }}) as data_source
                        , cast(hcc.risk_factor_description as {{ dbt.type_string() }}) as risk_factor_description
                        , count(distinct hcc.person_id) as patient_count
                    from {{ cms_hcc_rel }} as hcc
                    inner join {{ core_patient_rel }} as patient
                        on hcc.person_id = patient.person_id
                    where hcc.factor_type = 'Disease'
                    group by 1, 2
                ) as hcc_counts
            ) as ranked_hcc
            inner join (
                select
                      cast(data_source as {{ dbt.type_string() }}) as data_source
                    , count(distinct person_id) as total_patients
                from {{ core_patient_rel }}
                group by 1
            ) as patient_totals
                on ranked_hcc.data_source = patient_totals.data_source
            where ranked_hcc.hcc_rank <= 10
        {% endset %}
        {% do metric_queries.append(cms_hcc_metrics_query) %}
    {% endif %}

    {% if metric_queries | length > 0 %}
        {{ metric_queries | join('\nunion all\n') }}
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as domain
            , cast(null as {{ dbt.type_string() }}) as metric
            , cast(null as {{ dbt.type_numeric() }}) as result
        where 1 = 0
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as domain
        , cast(null as {{ dbt.type_string() }}) as metric
        , cast(null as {{ dbt.type_numeric() }}) as result
    where 1 = 0
{% endif %}
