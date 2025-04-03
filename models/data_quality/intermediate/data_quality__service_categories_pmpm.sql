{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with member_months as (
    select
        cast(count(1) as {{ dbt.type_numeric() }}) as member_months 
    from {{ ref('core__member_months') }} mm
)

, pmpm as (
    select
        cast('service categories pmpm' as {{dbt.type_string() }} ) as analytics_concept
        , mc.service_category_2 as analytics_measure
        , sum(mc.paid_amount) as total_paid
        , avg(mm.member_months) as member_months
        , case when avg(mm.member_months) = 0 then null
               else sum(mc.paid_amount) / avg(mm.member_months)
          end as data_source_value
    from {{ ref('core__medical_claim') }} mc
    cross join member_months mm
    group by
        mc.service_category_2
)

select
    pmpm.*
    , ref_data.analytics_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from pmpm
left join {{ ref('data_quality__reference_mart_analytics') }} ref_data
    on pmpm.analytics_concept = ref_data.analytics_concept
    and pmpm.analytics_measure = ref_data.analytics_measure
