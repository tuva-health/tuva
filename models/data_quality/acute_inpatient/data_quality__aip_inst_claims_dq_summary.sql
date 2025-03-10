{{ config(
    enabled = var('claims_enabled', False)
) }}

with total_institutional_claims as (

    select
          total_claims
    from {{ ref('data_quality__calculated_claim_type_percentages') }}
    where calculated_claim_type = 'institutional'

)

, total_claims as (

    select
          sum(total_claims) as totsum
    from {{ ref('data_quality__calculated_claim_type_percentages') }}

)

, total_aip_inst_claims as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}

)

, usable_aip_inst_claims as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_for_aip_encounter = 1

)

, aip_inst_claims_with_dq_problem as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where dq_problem = 1

)

, aip_inst_claims_with_unusable_person_id as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_person_id = 0

)

, aip_inst_claims_with_unusable_merge_dates as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_merge_dates = 0

)

, aip_inst_claims_with_unusable_drg_code as (

    select 
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_drg_code = 0

)

, aip_inst_claims_with_unusable_diagnosis_code_1 as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_diagnosis_code_1 = 0

)

, aip_inst_claims_with_unusable_atc as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_admit_type_code = 0

)

, aip_inst_claims_with_unusable_asc as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_admit_source_code = 0

)

, aip_inst_claims_with_unusable_ddc as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_discharge_disposition_code = 0

)

, aip_inst_claims_with_unusable_facility_npi as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_facility_npi = 0

)

, aip_inst_claims_with_unusable_rendering_npi as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as tot
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_rendering_npi = 0

)

, final as (
    
    select
      'total # of claims' as field
    , (select * from total_claims) as field_value

    union all

    select
        '# inst claims' as field
        , (select * from total_institutional_claims) as field_value

    union all

    select
        '# AIP inst claims' as field
        , (select * from total_aip_inst_claims) as field_value

    union all

    select
        '(# AIP inst claims) / (# inst claims) * 100' as field
        , round((select * from total_aip_inst_claims) * 100.0 / (select * from total_institutional_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims) / (total # of claims) * 100' as field
        , round((select * from total_aip_inst_claims) * 100.0 / (select * from total_claims), 1) as field_value

    union all

    select
        '(# usable AIP inst claims) / (# AIP inst claims) * 100' as field
        , round((select * from usable_aip_inst_claims) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with DQ problems) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_dq_problem) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable person_id) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_person_id) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable merge dates) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_merge_dates) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable drg_code) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_drg_code) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value
    
    union all

    select
        '(# AIP inst claims with unusable diagnosis_code_1) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_diagnosis_code_1) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable ATC) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_atc) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable ASC) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_asc) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable DDC) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_ddc) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable facility_npi) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_facility_npi) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

    union all

    select
        '(# AIP inst claims with unusable rendering_npi) / (# AIP inst claims) * 100' as field
        , round((select * from aip_inst_claims_with_unusable_rendering_npi) * 100.0 / (select * from total_aip_inst_claims), 1) as field_value

)

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final