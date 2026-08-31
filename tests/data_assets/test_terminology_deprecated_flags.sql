{{ config(
     tags = ['data_assets', 'package_invariant', 'terminology'],
     severity = 'error'
   )
}}

with terminology_flags as (

    select 'bill_type' as terminology, cast(bill_type_seed.bill_type_code as {{ dbt.type_string() }}) as code, bill_type_seed.deprecated
    from {{ ref('terminology__bill_type') }} as bill_type_seed

    union all

    select 'apr_drg' as terminology, cast(apr_drg_seed.apr_drg_code as {{ dbt.type_string() }}) as code, apr_drg_seed.deprecated
    from {{ ref('terminology__apr_drg') }} as apr_drg_seed

    union all

    select 'cvx', cast(cvx_seed.cvx as {{ dbt.type_string() }}), cvx_seed.deprecated
    from {{ ref('terminology__cvx') }} as cvx_seed

    union all

    select 'fips_county', cast(fips_county_seed.fips_code as {{ dbt.type_string() }}), fips_county_seed.deprecated
    from {{ ref('terminology__fips_county') }} as fips_county_seed

    union all

    select 'hcpcs_level_2', cast(hcpcs_seed.hcpcs as {{ dbt.type_string() }}), hcpcs_seed.deprecated
    from {{ ref('terminology__hcpcs_level_2') }} as hcpcs_seed

    union all

    select 'icd10_pcs_cms_ontology', cast(icd10_pcs_ontology_seed.icd10pcs_code as {{ dbt.type_string() }}), icd10_pcs_ontology_seed.deprecated
    from {{ ref('terminology__icd10_pcs_cms_ontology') }} as icd10_pcs_ontology_seed

    union all

    select 'icd_10_cm', cast(icd_10_cm_seed.icd_10_cm as {{ dbt.type_string() }}), icd_10_cm_seed.deprecated
    from {{ ref('terminology__icd_10_cm') }} as icd_10_cm_seed

    union all

    select 'icd_10_pcs', cast(icd_10_pcs_seed.icd_10_pcs as {{ dbt.type_string() }}), icd_10_pcs_seed.deprecated
    from {{ ref('terminology__icd_10_pcs') }} as icd_10_pcs_seed

    union all

    select 'icd_9_cm', cast(icd_9_cm_seed.icd_9_cm as {{ dbt.type_string() }}), icd_9_cm_seed.deprecated
    from {{ ref('terminology__icd_9_cm') }} as icd_9_cm_seed

    union all

    select 'icd_9_pcs', cast(icd_9_pcs_seed.icd_9_pcs as {{ dbt.type_string() }}), icd_9_pcs_seed.deprecated
    from {{ ref('terminology__icd_9_pcs') }} as icd_9_pcs_seed

    union all

    select 'loinc', cast(loinc_seed.loinc as {{ dbt.type_string() }}), loinc_seed.deprecated
    from {{ ref('terminology__loinc') }} as loinc_seed

    union all

    select 'medicare_dual_eligibility', cast(dual_eligibility_seed.dual_status_code as {{ dbt.type_string() }}), dual_eligibility_seed.deprecated
    from {{ ref('terminology__medicare_dual_eligibility') }} as dual_eligibility_seed

    union all

    select 'ms_drg', cast(ms_drg_seed.ms_drg_code as {{ dbt.type_string() }}), ms_drg_seed.deprecated
    from {{ ref('terminology__ms_drg') }} as ms_drg_seed

    union all

    select 'snomed_ct', cast(snomed_seed.snomed_ct as {{ dbt.type_string() }}), snomed_seed.deprecated
    from {{ ref('terminology__snomed_ct') }} as snomed_seed

)

select
    terminology
  , code
  , deprecated
from terminology_flags
where deprecated is null
   or cast(deprecated as {{ dbt.type_string() }}) not in ('0', '1')
