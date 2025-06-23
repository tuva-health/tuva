{{ config(
    enabled = var('claims_enabled', False)
) }}

with medical_claims as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
)

, professional_claims as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('data_quality__claim_type') }}
    where calculated_claim_type = 'professional'
)

, institutional_claims as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('data_quality__claim_type') }}
    where calculated_claim_type = 'institutional'
)

, invalid_drg_code_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__ms_drg') }} bb
        on aa.drg_code_type = 'ms-drg'
        and aa.drg_code = bb.ms_drg_code
    left join {{ ref('terminology__apr_drg') }} dd
        on aa.drg_code_type = 'apr-drg'
        and aa.drg_code = dd.apr_drg_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where aa.drg_code is not null
        and coalesce(bb.ms_drg_code, dd.apr_drg_code) is null 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_drg_code_perc as (
    select 
        round(
            (select claim_count from invalid_drg_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_drg_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct aa.drg_code) as count_of_drg_codes
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count(distinct aa.drg_code) > 1
    ) as subquery
)

, dupe_drg_code_perc as (
    select 
        round(
            (select claim_count from dupe_drg_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, missing_revenue_center_code_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id
            , max(aa.revenue_center_code) as max_revenue_center_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
    ) as subquery
    where max_revenue_center_code is null
)

, missing_revenue_center_code_perc as (
    select round(
        (select * from missing_revenue_center_code_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    ) as percentage
)

, invalid_revenue_center_code_count as (
    select count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__revenue_center') }} bb
        on aa.revenue_center_code = bb.revenue_center_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where cc.calculated_claim_type = 'institutional'
        and aa.revenue_center_code is not null 
        and bb.revenue_center_code is null
)

, invalid_revenue_center_code_perc as (
    select round(
        (select * from invalid_revenue_center_code_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    ) as percentage
)

, missing_hcpcs_code_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id
            , max(aa.hcpcs_code) as max_hcpcs_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'professional'
        group by aa.claim_id
    ) as subquery
    where max_hcpcs_code is null
)

, missing_hcpcs_code_perc as (
    select round(
        (select * from missing_hcpcs_code_count) * 100.0 /
        (select * from professional_claims)
        , 1
    ) as percentage
)

, missing_rendering_npi_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where rendering_npi is null
)

, missing_rendering_npi_perc as (
    select round(
        (select * from missing_rendering_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, invalid_rendering_npi_count as (
    select count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__provider') }} bb
        on aa.rendering_npi = bb.npi
    where aa.rendering_npi is not null 
        and bb.npi is null
)

, invalid_rendering_npi_perc as (
    select round(
        (select * from invalid_rendering_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, dupe_rendering_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id
            , count(distinct rendering_npi) as count_of_rendering_npis
        from {{ ref('medical_claim') }}
        group by claim_id
        having count(distinct rendering_npi) > 1
    ) as subquery
)

, dupe_rendering_npi_perc as (
    select round(
        (select * from dupe_rendering_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, missing_rendering_tin_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where rendering_tin is null
)

, missing_rendering_tin_perc as (
    select round(
        (select * from missing_rendering_tin_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, dupe_rendering_tin_count as (
    select
        count(*) as rendering_tin_count
    from (
            select
                claim_id
            , count(distinct rendering_tin) as count_of_rendering_tins
            from {{ ref('medical_claim') }}
            group by claim_id
            having count(distinct rendering_tin) > 1
        ) as subquery
)

, dupe_rendering_tin_perc as (
    select
        round(
        (select rendering_tin_count from dupe_rendering_tin_count) * 100.0 /
        (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_billing_npi_count as (
    select cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where billing_npi is null
)

, missing_billing_npi_perc as (
    select round(
        (select * from missing_billing_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, invalid_billing_npi_count as (
    select count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__provider') }} bb
        on aa.billing_npi = bb.npi
    where aa.billing_npi is not null 
        and bb.npi is null
)

, invalid_billing_npi_perc as (
    select round(
        (select * from invalid_billing_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, dupe_billing_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id
            , count(distinct billing_npi) as count_of_billing_npis
        from {{ ref('medical_claim') }}
        group by claim_id
        having count(distinct billing_npi) > 1
    ) as subquery
)

, dupe_billing_npi_perc as (
    select round(
        (select * from dupe_billing_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    ) as percentage
)

, missing_billing_tin_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }}
    where billing_tin is null
)

, missing_billing_tin_perc as (
    select 
        round(
        (select * from missing_billing_tin_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, dupe_billing_tin_count as (
    select 
        count(*) as claim_count
    from (
            select 
            claim_id
            , count(distinct billing_tin) as count_of_billing_tins
            from {{ ref('medical_claim') }}
            group by claim_id
            having count(distinct billing_tin) > 1
        ) as subquery
)

, dupe_billing_tin_perc as (
    select 
        round(
        (select * from dupe_billing_tin_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, missing_facility_npi_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('data_quality__claim_type') }} bb
        on aa.claim_id = bb.claim_id
    where (aa.facility_npi is null) 
        and (bb.calculated_claim_type = 'institutional')
)

, missing_facility_npi_perc as (
    select 
        round(
        (select * from missing_facility_npi_count) * 100.0 /
        (select * from institutional_claims), 1
        ) as percentage
)

, invalid_facility_npi_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__provider') }} bb
        on aa.rendering_npi = bb.npi
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where (aa.facility_npi is not null) 
        and (bb.npi is null) 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_facility_npi_perc as (
    select 
        round(
        (select * from invalid_facility_npi_count) * 100.0 /
        (select * from institutional_claims), 1
        ) as percentage
)

, dupe_facility_npi_count as (
    select 
        count(*) as claim_count
    from (
            select 
              aa.claim_id
            , count(distinct aa.facility_npi) as count_of_facility_npis
            from {{ ref('medical_claim') }} aa
            left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
            where bb.calculated_claim_type = 'institutional'
            group by aa.claim_id
            having count(distinct aa.facility_npi) > 1
        ) as subquery
)

, dupe_facility_npi_perc as (
    select 
        round(
        (select * from dupe_facility_npi_count) * 100.0 /
        (select * from institutional_claims), 1
        ) as percentage
)

, missing_paid_date_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }}
    where paid_date is null
)

, missing_paid_date_perc as (
    select 
        round(
        (select * from missing_paid_date_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, invalid_paid_date_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.paid_date = bb.full_date
    where (aa.paid_date is not null and bb.full_date is null)
        or (aa.paid_date < aa.claim_start_date)
            or (aa.paid_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
)

, invalid_paid_date_perc as (
    select 
        round(
        (select * from invalid_paid_date_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, missing_paid_amount_count as (
    select 
        count(distinct claim_id) as claim_count
    from (
            select 
              claim_id
            , max(paid_amount) as max_paid_amount
            from {{ ref('medical_claim') }} 
            group by claim_id
        ) as subquery
    where max_paid_amount is null
)

, missing_paid_amount_perc as (
    select 
        round(
        (select * from missing_paid_amount_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, missing_allowed_amount_count as (
    select 
        count(distinct claim_id) as claim_count
    from (
            select 
            claim_id
            , max(allowed_amount) as max_allowed_amount
            from {{ ref('medical_claim') }} 
            group by claim_id
        ) as subquery
    where max_allowed_amount is null
)

, missing_allowed_amount_perc as (
    select 
        round(
        (select * from missing_allowed_amount_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, missing_diagnosis_code_type_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }}
    where diagnosis_code_type is null
)

, missing_diagnosis_code_type_perc as (
    select 
        round(
        (select * from missing_diagnosis_code_type_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, invalid_diagnosis_code_type_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }}
    where (diagnosis_code_type is not null) 
        and diagnosis_code_type not in ('icd-9-cm', 'icd-10-cm')
)

, invalid_diagnosis_code_type_perc as (
    select 
        round(
        (select * from invalid_diagnosis_code_type_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, dupe_diagnosis_code_type_count as (
    select 
        count(*) as claim_count
    from (
            select 
            claim_id
            , count(distinct diagnosis_code_type) as count_of_diagnosis_code_types
            from {{ ref('medical_claim') }}
            group by claim_id
            having count(distinct diagnosis_code_type) > 1
        ) as subquery
)

, dupe_diagnosis_code_type_perc as (
    select 
        round(
        (select * from dupe_diagnosis_code_type_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, missing_diagnosis_code_1_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }}
    where diagnosis_code_1 is null
)

, missing_diagnosis_code_1_perc as (
    select 
        round(
        (select * from missing_diagnosis_code_1_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, invalid_diagnosis_code_1_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__icd_10_cm') }} bb
        on aa.diagnosis_code_1 = bb.icd_10_cm
    where (aa.diagnosis_code_1 is not null) 
        and (bb.icd_10_cm is null)
)

, invalid_diagnosis_code_1_perc as (
    select 
        round(
        (select * from invalid_diagnosis_code_1_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, dupe_diagnosis_code_1_count as (
    select 
        count(*) as claim_count
    from (
            select 
            claim_id
            , count(distinct diagnosis_code_1) as count_of_diagnosis_code_1s
            from {{ ref('medical_claim') }}
            group by claim_id
            having count(distinct diagnosis_code_1) > 1
        ) as subquery
)

, dupe_diagnosis_code_1_perc as (
    select 
        round(
        (select * from dupe_diagnosis_code_1_count) * 100.0 /
        (select * from medical_claims), 1
        ) as percentage
)

, invalid_procedure_code_type_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('data_quality__claim_type') }} bb
        on aa.claim_id = bb.claim_id
    where aa.procedure_code_type not in ('icd-9-pcs', 'icd-10-pcs')
        and bb.calculated_claim_type = 'institutional'
)

, invalid_procedure_code_type_perc as (
    select 
        round(
        (select * from invalid_procedure_code_type_count) * 100.0 /
        (select * from institutional_claims), 1
        ) as percentage
)

, invalid_procedure_code_1_count as (
    select 
        count(distinct claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__icd_10_pcs') }} bb
        on aa.procedure_code_1 = bb.icd_10_pcs
    where (aa.procedure_code_1 is not null) 
        and (bb.icd_10_pcs is null)
)

, invalid_procedure_code_1_perc as (
    select 
        round(
        (select * from invalid_procedure_code_1_count) * 100.0 /
        (select * from institutional_claims), 1
        ) as percentage
)

, final2 as (

    select 
          'drg_code' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_drg_code_count) as invalid_count
        , (select * from invalid_drg_code_perc) as invalid_perc
        , (select * from dupe_drg_code_count) as duplicated_count
        , (select * from dupe_drg_code_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
          'revenue_center_code' as field
        , (select * from missing_revenue_center_code_count) as missing_count
        , (select * from missing_revenue_center_code_perc) as missing_perc
        , (select * from invalid_revenue_center_code_count) as invalid_count
        , (select * from invalid_revenue_center_code_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
          'hcpcs_code' as field
        , (select * from missing_hcpcs_code_count) as missing_count
        , (select * from missing_hcpcs_code_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'professional' as claim_type

    union all

    select 
          'rendering_npi' as field
        , (select * from missing_rendering_npi_count) as missing_count
        , (select * from missing_rendering_npi_perc) as missing_perc
        , (select * from invalid_rendering_npi_count) as invalid_count
        , (select * from invalid_rendering_npi_perc) as invalid_perc
        , (select * from dupe_rendering_npi_count) as duplicated_count
        , (select * from dupe_rendering_npi_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'rendering_tin' as field
        , (select * from missing_rendering_tin_count) as missing_count
        , (select * from missing_rendering_tin_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_rendering_tin_count) as duplicated_count
        , (select * from dupe_rendering_tin_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'billing_npi' as field
        , (select * from missing_billing_npi_count) as missing_count
        , (select * from missing_billing_npi_perc) as missing_perc
        , (select * from invalid_billing_npi_count) as invalid_count
        , (select * from invalid_billing_npi_perc) as invalid_perc
        , (select * from dupe_billing_npi_count) as duplicated_count
        , (select * from dupe_billing_npi_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select
        'billing_tin' as field,
        (select * from missing_billing_tin_count) as missing_count,
        (select * from missing_billing_tin_perc) as missing_perc,
        null as invalid_count,
        null as invalid_perc,
        (select * from dupe_billing_tin_count) as duplicated_count,
        (select * from dupe_billing_tin_perc) as duplicated_perc,
        'all' as claim_type

    union all

    select
        'facility_npi' as field,
        (select * from missing_facility_npi_count) as missing_count,
        (select * from missing_facility_npi_perc) as missing_perc,
        (select * from invalid_facility_npi_count) as invalid_count,
        (select * from invalid_facility_npi_perc) as invalid_perc,
        (select * from dupe_facility_npi_count) as duplicated_count,
        (select * from dupe_facility_npi_perc) as duplicated_perc,
        'institutional' as claim_type

    union all

    select
        'paid_date' as field,
        (select * from missing_paid_date_count) as missing_count,
        (select * from missing_paid_date_perc) as missing_perc,
        (select * from invalid_paid_date_count) as invalid_count,
        (select * from invalid_paid_date_perc) as invalid_perc,
        null as duplicated_count,
        null as duplicated_perc,
        'all' as claim_type

    union all

    select
        'paid_amount' as field,
        (select * from missing_paid_amount_count) as missing_count,
        (select * from missing_paid_amount_perc) as missing_perc,
        null as invalid_count,
        null as invalid_perc,
        null as duplicated_count,
        null as duplicated_perc,
        'all' as claim_type

    union all

    select
        'allowed_amount' as field,
        (select * from missing_allowed_amount_count) as missing_count,
        (select * from missing_allowed_amount_perc) as missing_perc,
        null as invalid_count,
        null as invalid_perc,
        null as duplicated_count,
        null as duplicated_perc,
        'all' as claim_type

    union all

    select
        'diagnosis_code_type' as field,
        (select * from missing_diagnosis_code_type_count) as missing_count,
        (select * from missing_diagnosis_code_type_perc) as missing_perc,
        (select * from invalid_diagnosis_code_type_count) as invalid_count,
        (select * from invalid_diagnosis_code_type_perc) as invalid_perc,
        (select * from dupe_diagnosis_code_type_count) as duplicated_count,
        (select * from dupe_diagnosis_code_type_perc) as duplicated_perc,
        'all' as claim_type

    union all

    select
        'diagnosis_code_1' as field,
        (select * from missing_diagnosis_code_1_count) as missing_count,
        (select * from missing_diagnosis_code_1_perc) as missing_perc,
        (select * from invalid_diagnosis_code_1_count) as invalid_count,
        (select * from invalid_diagnosis_code_1_perc) as invalid_perc,
        (select * from dupe_diagnosis_code_1_count) as duplicated_count,
        (select * from dupe_diagnosis_code_1_perc) as duplicated_perc,
        'all' as claim_type

    union all

    select
        'procedure_code_type' as field,
        null as missing_count,
        null as missing_perc,
        (select * from invalid_procedure_code_type_count) as invalid_count,
        (select * from invalid_procedure_code_type_perc) as invalid_perc,
        null as duplicated_count,
        null as duplicated_perc,
        'institutional' as claim_type

    union all

    select
        'procedure_code_1' as field,
        null as missing_count,
        null as missing_perc,
        (select * from invalid_procedure_code_1_count) as invalid_count,
        (select * from invalid_procedure_code_1_perc) as invalid_perc,
        null as duplicated_count,
        null as duplicated_perc,
        'institutional' as claim_type

)

select
      field
    , claim_type
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
from final2
