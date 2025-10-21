{{ config(
     enabled = var('claims_preprocessing_enabled', False) and var('assignment_methodology') == 'prospective'
 | as_bool)
}}

-- The latest AALR report
select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(in_va_max as {{ dbt.type_int() }}) as in_va_max
    , cast(va_tin as {{ dbt.type_string() }}) as va_tin
    , cast(va_npi as {{ dbt.type_string() }}) as va_npi
    , cast(cba_flag as {{ dbt.type_int() }}) as cba_flag
    , cast(assignment_type as {{ dbt.type_int() }}) as assignment_type
    , cast(excluded as {{ dbt.type_int() }}) as excluded
    , cast(deceased_excluded as {{ dbt.type_int() }}) as deceased_excluded
    , cast(other_reasons_excluded as {{ dbt.type_int() }}) as other_reasons_excluded
    , cast(part_a_b_only_excluded as {{ dbt.type_int() }}) as part_a_b_only_excluded
    , cast(ghp_excluded as {{ dbt.type_int() }}) as ghp_excluded
    , cast(outside_us_excluded as {{ dbt.type_int() }}) as outside_us_excluded
    , cast(other_shared_sav_init as {{ dbt.type_int() }}) as other_shared_sav_init     
    , cast(file_name as {{ dbt.type_string() }}) as file_name
from {{ref('alr1_prospective')}}