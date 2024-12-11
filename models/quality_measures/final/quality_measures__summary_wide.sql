{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

/*
    Each measure is pivoted into a boolean column by evaluating the
    denominator, numerator, and exclusion flags.
*/
with measures_long as (

        select
          person_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , performance_flag
        , measure_id
    from {{ ref('quality_measures__summary_long') }}

)

, nqf_2372 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF2372'

)

, nqf_0034 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0034'

)

, nqf_0059 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0059'

)

, cqm_236 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CQM236'

)

, nqf_0053 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0053'

)

, cbe_0055 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CBE0055'

)

, nqf_0097 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0097'

)

, cqm_438 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CQM438'

)

, nqf_0041 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0041'

)

, cbe_0101 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CBE0101'

)

, cqm_48 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CQM48'

)

, cqm_130 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'CQM130'

)

, nqf_0420 as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0420'

)

, adh_diabetes as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'ADH-Diabetes'

)

, adh_ras as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'ADH-RAS'
    
)

, supd as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'SUPD'

)

, adh_statins as (

    select
          person_id
        , performance_flag
    from measures_long
    where measure_id = 'ADH-Statins'

)

, joined as (

    select
          measures_long.person_id
        , max(nqf_2372.performance_flag) as nqf_2372
        , max(nqf_0034.performance_flag) as nqf_0034
        , max(nqf_0059.performance_flag) as nqf_0059
        , max(cqm_236.performance_flag) as cqm_236
        , max(nqf_0053.performance_flag) as nqf_0053
        , max(cbe_0055.performance_flag) as cbe_0055
        , max(nqf_0097.performance_flag) as nqf_0097
        , max(cqm_438.performance_flag) as cqm_438
        , max(nqf_0041.performance_flag) as nqf_0041
        , max(cbe_0101.performance_flag) as cbe_0101
        , max(cqm_48.performance_flag) as cqm_48
        , max(cqm_130.performance_flag) as cqm_130
        , max(nqf_0420.performance_flag) as nqf_0420
        , max(adh_diabetes.performance_flag) as adh_diabetes
        , max(adh_ras.performance_flag) as adh_ras
        , max(supd.performance_flag) as supd
        , max(adh_statins.performance_flag) as adh_statins
    from measures_long
        left join nqf_2372
            on measures_long.person_id = nqf_2372.person_id
        left join nqf_0034
            on measures_long.person_id = nqf_0034.person_id
        left join nqf_0059
            on measures_long.person_id = nqf_0059.person_id
        left join cqm_236
            on measures_long.person_id = cqm_236.person_id
        left join nqf_0053
            on measures_long.person_id = nqf_0053.person_id
        left join cbe_0055
            on measures_long.person_id = cbe_0055.person_id
        left join nqf_0097
            on measures_long.person_id = nqf_0097.person_id
        left join cqm_438
            on measures_long.person_id = cqm_438.person_id
        left join nqf_0041
            on measures_long.person_id = nqf_0041.person_id
        left join cbe_0101
            on measures_long.person_id = cbe_0101.person_id
        left join cqm_48
            on measures_long.person_id = cqm_48.person_id
        left join cqm_130
            on measures_long.person_id = cqm_130.person_id
        left join nqf_0420
            on measures_long.person_id = nqf_0420.person_id
        left join adh_diabetes
            on measures_long.person_id = adh_diabetes.person_id
        left join adh_ras
            on measures_long.person_id = adh_ras.person_id
        left join supd
            on measures_long.person_id = supd.person_id
        left join adh_statins
            on measures_long.person_id = adh_statins.person_id
    group by measures_long.person_id

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(nqf_2372 as integer) as nqf_2372
        , cast(nqf_0034 as integer) as nqf_0034
        , cast(nqf_0059 as integer) as nqf_0059
        , cast(cqm_236 as integer) as cqm_236
        , cast(nqf_0053 as integer) as nqf_0053
        , cast(cbe_0055 as integer) as cbe_0055
        , cast(nqf_0097 as integer) as nqf_0097
        , cast(cqm_438 as integer) as cqm_438 
        , cast(nqf_0041 as integer) as nqf_0041
        , cast(cbe_0101 as integer) as cbe_0101
        , cast(cqm_48 as integer) as cqm_48
        , cast(cqm_130 as integer) as cqm_130
        , cast(nqf_0420 as integer) as nqf_0420
        , cast(adh_diabetes as integer) as adh_diabetes
        , cast(adh_ras as integer) as adh_ras
        , cast(supd as integer) as supd
        , cast(adh_statins as integer) as adh_statins
    from joined

)

select
      person_id
    , nqf_2372
    , nqf_0034
    , nqf_0059
    , cqm_236
    , nqf_0053
    , cbe_0055
    , nqf_0097
    , cqm_438
    , nqf_0041
    , cbe_0101
    , cqm_48
    , cqm_130
    , nqf_0420
    , adh_diabetes
    , adh_ras
    , supd
    , adh_statins
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
