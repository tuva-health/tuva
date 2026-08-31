{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with terminology_flags as (

    select 'bill_type' as terminology, cast(bill_type_code as {{ dbt.type_string() }}) as code, deprecated
    from {{ ref('terminology__bill_type') }}

    union all

    select 'apr_drg' as terminology, cast(apr_drg_code as {{ dbt.type_string() }}) as code, deprecated
    from {{ ref('terminology__apr_drg') }}

    union all

    select 'cvx', cast(cvx as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__cvx') }}

    union all

    select 'fips_county', cast(fips_code as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__fips_county') }}

    union all

    select 'hcpcs_level_2', cast(hcpcs as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__hcpcs_level_2') }}

    union all

    select 'icd10_pcs_cms_ontology', cast(icd10pcs_code as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__icd10_pcs_cms_ontology') }}

    union all

    select 'icd_10_cm', cast(icd_10_cm as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__icd_10_cm') }}

    union all

    select 'icd_10_pcs', cast(icd_10_pcs as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__icd_10_pcs') }}

    union all

    select 'icd_9_cm', cast(icd_9_cm as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__icd_9_cm') }}

    union all

    select 'icd_9_pcs', cast(icd_9_pcs as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__icd_9_pcs') }}

    union all

    select 'loinc', cast(loinc as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__loinc') }}

    union all

    select 'medicare_dual_eligibility', cast(dual_status_code as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__medicare_dual_eligibility') }}

    union all

    select 'ms_drg', cast(ms_drg_code as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__ms_drg') }}

    union all

    select 'snomed_ct', cast(snomed_ct as {{ dbt.type_string() }}), deprecated
    from {{ ref('terminology__snomed_ct') }}

)

select
    terminology
  , code
  , deprecated
from terminology_flags
where deprecated is null
   or cast(deprecated as {{ dbt.type_string() }}) not in ('0', '1')
