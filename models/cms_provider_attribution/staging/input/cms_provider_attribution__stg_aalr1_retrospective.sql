-- The latest AALR report
select
      cast(bene_mbi_id as {{ dbt.type_string() }}) as person_id
    , cast(in_va_max as {{ dbt.type_int() }}) as in_va_max
    , cast(va_tin as {{ dbt.type_string() }}) as va_tin
    , cast(va_npi as {{ dbt.type_string() }}) as va_npi
    , cast(cba_flag as {{ dbt.type_int() }}) as cba_flag
    , cast(assignment_type as {{ dbt.type_int() }}) as assignment_type
    , 0 as excluded
    , 0 as deceased_excluded
    , 0 as other_reasons_excluded
    , 0 as part_a_b_only_excluded
    , 0 as ghp_excluded
    , 0 as outside_us_excluded
    , 0 as other_shared_sav_init     
    , cast(filename as {{ dbt.type_string() }}) as file_name
from {{source('phds_lakehouse_test', 'yak_alr_1_1')}}
where filename = 'P.A5321.ACO.QALR.2025Q2.D259999.T0200000_1-1.csv'