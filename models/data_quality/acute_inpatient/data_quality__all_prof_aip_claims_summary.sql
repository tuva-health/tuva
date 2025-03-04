{{ config(
    enabled = var('claims_enabled', False)
) }}

with total_aip_prof_claims as (

    select
          cast(count(*) as {{ dbt.type_numeric() }}) as total
    from {{ ref('data_quality__all_prof_aip_claims') }}

)

,  aip_prof_claims_with_unusable_person_id as (

    select
          cast(count(*) as {{ dbt.type_numeric() }}) as total
    from {{ ref('data_quality__all_prof_aip_claims') }}
    where usable_person_id = 0

)

,  aip_prof_claims_with_unusable_merge_dates as (

    select
          cast(count(*) as {{ dbt.type_numeric() }}) as total
    from {{ ref('data_quality__all_prof_aip_claims') }}
    where usable_merge_dates = 0

)

,  usable_aip_prof_claims as (

    select
          cast(count(*) as {{ dbt.type_numeric() }}) as total
    from {{ ref('data_quality__all_prof_aip_claims') }}
    where usable_prof_claim = 1

)

, final as (

    select
        'total aip prof claims' as field
        , total_aip_prof_claims.total as field_value
    from total_aip_prof_claims

    union all

    select
        '(aip prof claims with unusable person_id) / (total aip prof claims) * 100' as field
        , round(aip_prof_claims_with_unusable_person_id.total * 100.0 / total_aip_prof_claims.total, 1) as field_value
    from aip_prof_claims_with_unusable_person_id, total_aip_prof_claims

    union all

    select
        '(aip prof claims with unusable merge dates) / (total aip prof claims) * 100' as field
        , round(aip_prof_claims_with_unusable_merge_dates.total * 100.0 / total_aip_prof_claims.total, 1) as field_value
    from aip_prof_claims_with_unusable_merge_dates, total_aip_prof_claims

    union all

    select
        '(usable aip prof claims) / (total aip prof claims) * 100' as field
        , round(usable_aip_prof_claims.total * 100.0 / total_aip_prof_claims.total, 1) as field_value
    from usable_aip_prof_claims, total_aip_prof_claims

)

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
