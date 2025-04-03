{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with date_stage as(

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'claim_start_date' as date_field
            , cast({{ date_part("year", "claim_start_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "claim_start_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast({{ date_part("year", "claim_start_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "claim_start_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'claim_end_date' as date_field
            , cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'admission_date' as date_field
            , cast({{ date_part("year", "admission_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "admission_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast({{ date_part("year", "admission_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "admission_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'discharge_date' as date_field
            , cast({{ date_part("year", "discharge_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "discharge_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast({{ date_part("year", "discharge_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "discharge_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'medical paid_date' as date_field
            , cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__medical_claim') }}
        group by
            cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'dispensing_date' as date_field
            , cast({{ date_part("year", "dispensing_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "dispensing_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__pharmacy_claim') }}
        group by
            cast({{ date_part("year", "dispensing_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "dispensing_date") }} as {{ dbt.type_string() }})
    )x

    union all

    select
        date_field
        , {{ concat_custom(["year", dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
        , result_count
    from
    (
        select
            'pharmacy paid_date' as date_field
            , cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }}) as year
            , cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }}) as month
            , count(distinct claim_id) as result_count
        from {{ ref('input_layer__pharmacy_claim') }}
        group by
            cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }})
            , cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }})
    )x

)


, all_date_range as (
    select distinct
        replace(cal.year_month,'-','') as year_month
    from {{ ref('reference_data__calendar') }} cal
    where (cal.year_month >= (select min(year_month) from date_stage)
    and cal.year_month <= (select max(year_month) from date_stage))

)

select
    cast(all_date.year_month as {{ dbt.type_int() }} ) as year_month
    , coalesce(claim_start.result_count,0) as claim_start_date
    , coalesce(claim_end.result_count,0) as claim_end_date
    , coalesce(admission_date.result_count,0) as admission_date
    , coalesce(discharge_date.result_count,0) as discharge_date
    , coalesce(med_paid_date.result_count,0) as medical_paid_date
    , coalesce(dispensing_date.result_count,0) as dispensing_date
    , coalesce(pharm_paid_date.result_count,0) as pharmacy_paid_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from all_date_range all_date
left join date_stage claim_start
    on all_date.year_month = claim_start.year_month
    and claim_start.date_field = 'claim_start_date'
left join date_stage claim_end
    on all_date.year_month = claim_end.year_month
    and claim_end.date_field = 'claim_end_date'
left join date_stage admission_date
    on all_date.year_month = admission_date.year_month
    and admission_date.date_field = 'admission_date'
left join date_stage discharge_date
    on all_date.year_month = discharge_date.year_month
    and discharge_date.date_field = 'discharge_date'
left join date_stage med_paid_date
    on all_date.year_month = med_paid_date.year_month
    and med_paid_date.date_field = 'medical paid_date'
left join date_stage dispensing_date
    on all_date.year_month = dispensing_date.year_month
    and dispensing_date.date_field = 'dispensing_date'
left join date_stage pharm_paid_date
    on all_date.year_month = pharm_paid_date.year_month
    and pharm_paid_date.date_field = 'pharmacy paid_date'
