{{ config(
    enabled = var('claims_enabled', False)
) }}

with medical_claims as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
)

, professional_claims as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('data_quality__claim_type') }}
    where calculated_claim_type = 'professional'
)

, institutional_claims as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('data_quality__claim_type') }}
    where calculated_claim_type = 'institutional'
)

, missing_patient_id_count as (
    select
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where patient_id is null
)

, missing_patient_id_perc as (
    select
        round(
            (select claim_count from missing_patient_id_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_patient_id_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct patient_id) as count_of_patient_ids
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_patient_ids > 1
    )
)

, dupe_patient_id_perc as (
    select 
        round(
            (select claim_count from dupe_patient_id_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_payer_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where payer is null
)

, missing_payer_perc as (
    select 
        round(
            (select claim_count from missing_payer_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_payer_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct payer) as count_of_payers
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_payers > 1
    )
)

, dupe_payer_perc as (
    select 
        round(
            (select claim_count from dupe_payer_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_plan_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where {{ quote_column('plan') }} is null
)

, missing_plan_perc as (
    select 
        round(
            (select claim_count from missing_plan_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_plan_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct {{ quote_column('plan') }}) as count_of_plans
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_plans > 1
    )
)

, dupe_plan_perc as (
    select 
        round(
            (select claim_count from dupe_plan_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_claim_start_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where claim_start_date is null
)

, missing_claim_start_date_perc as (
    select 
        round(
            (select claim_count from missing_claim_start_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_claim_start_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_start_date = bb.full_date
    where aa.claim_start_date is not null and bb.full_date is null
)

, invalid_claim_start_date_perc as (
    select 
        round(
            (select claim_count from invalid_claim_start_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_claim_start_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct claim_start_date) as count_of_claim_start_dates
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_claim_start_dates > 1
    )
)

, dupe_claim_start_date_perc as (
    select 
        round(
            (select claim_count from dupe_claim_start_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_claim_end_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where claim_end_date is null
)

, missing_claim_end_date_perc as (
    select 
        round(
            (select claim_count from missing_claim_end_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_claim_end_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_end_date = bb.full_date
    where aa.claim_end_date is not null and bb.full_date is null
)

, invalid_claim_end_date_perc as (
    select 
        round(
            (select claim_count from invalid_claim_end_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_claim_end_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct claim_end_date) as count_of_claim_end_dates
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_claim_end_dates > 1
    )
)

, dupe_claim_end_date_perc as (
    select 
        round(
            (select claim_count from dupe_claim_end_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_claim_line_start_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where claim_line_start_date is null
)

, missing_claim_line_start_date_perc as (
    select 
        round(
            (select claim_count from missing_claim_line_start_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_claim_line_start_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_line_start_date = bb.full_date
    where aa.claim_line_start_date is not null and bb.full_date is null
)

, invalid_claim_line_start_date_perc as (
    select 
        round(
            (select claim_count from invalid_claim_line_start_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_claim_line_end_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where claim_line_end_date is null
)

, missing_claim_line_end_date_perc as (
    select 
        round(
            (select claim_count from missing_claim_line_end_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_claim_line_end_date_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_line_end_date = bb.full_date
    where aa.claim_line_end_date is not null and bb.full_date is null
)

, invalid_claim_line_end_date_perc as (
    select 
        round(
            (select claim_count from invalid_claim_line_end_date_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_admission_date_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.admission_date = bb.full_date
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where aa.admission_date is not null 
        and bb.full_date is null
        and cc.calculated_claim_type = 'institutional'
)

, invalid_admission_date_perc as (
    select 
        round(
            (select claim_count from invalid_admission_date_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_admission_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct admission_date) as count_of_admission_dates
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_admission_dates > 1
    )
)

, dupe_admission_date_perc as (
    select 
        round(
            (select claim_count from dupe_admission_date_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, invalid_discharge_date_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.discharge_date = bb.full_date
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where aa.discharge_date is not null 
        and bb.full_date is null
        and cc.calculated_claim_type = 'institutional'
)

, invalid_discharge_date_perc as (
    select 
        round(
            (select claim_count from invalid_discharge_date_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_discharge_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct discharge_date) as count_of_discharge_dates
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_discharge_dates > 1
    )
)

, dupe_discharge_date_perc as (
    select 
        round(
            (select claim_count from dupe_discharge_date_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, invalid_ddc_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__discharge_disposition') }} bb
        on aa.discharge_disposition_code = bb.discharge_disposition_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where bb.discharge_disposition_code is null 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_ddc_perc as (
    select 
        round(
            (select claim_count from invalid_ddc_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_ddc_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct discharge_disposition_code) as count_of_ddcs
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_ddcs > 1
    )
)

, dupe_ddc_perc as (
    select 
        round(
            (select claim_count from dupe_ddc_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, missing_pos_count as (
    select 
        cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id as claim_id,
            max(aa.place_of_service_code) as max_place_of_service_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'professional'
        group by aa.claim_id
    )
    where max_place_of_service_code is null
)

, missing_pos_perc as (
    select 
        round(
            (select claim_count from missing_pos_count) * 100.0 /
            (select claim_count from professional_claims), 1
        ) as percentage
)

, invalid_pos_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__place_of_service') }} bb
        on aa.place_of_service_code = bb.place_of_service_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where cc.calculated_claim_type = 'professional' 
        and aa.place_of_service_code is not null 
        and bb.place_of_service_code is null
)

, invalid_pos_perc as (
    select 
        round(
            (select claim_count from invalid_pos_count) * 100.0 /
            (select claim_count from professional_claims), 1
        ) as percentage
)

, missing_bill_type_code_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('data_quality__claim_type') }} bb
        on aa.claim_id = bb.claim_id
    where aa.bill_type_code is null 
        and bb.calculated_claim_type = 'institutional'
)

, missing_bill_type_code_perc as (
    select 
        round(
            (select claim_count from missing_bill_type_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, invalid_bill_type_code_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__bill_type') }} bb
        on aa.bill_type_code = bb.bill_type_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where aa.bill_type_code is not null 
        and bb.bill_type_code is null 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_bill_type_code_perc as (
    select 
        round(
            (select claim_count from invalid_bill_type_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_bill_type_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct aa.bill_type_code) as count_of_bill_type_codes
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_bill_type_codes > 1
    )
)

, dupe_bill_type_code_perc as (
    select 
        round(
            (select claim_count from dupe_bill_type_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, invalid_ms_drg_code_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__ms_drg') }} bb
        on aa.ms_drg_code = bb.ms_drg_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where aa.ms_drg_code is not null 
        and bb.ms_drg_code is null 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_ms_drg_code_perc as (
    select 
        round(
            (select claim_count from invalid_ms_drg_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, dupe_ms_drg_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id,
            count(distinct aa.ms_drg_code) as count_of_ms_drg_codes
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_ms_drg_codes > 1
    )
)

, dupe_ms_drg_code_perc as (
    select 
        round(
            (select claim_count from dupe_ms_drg_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)

, invalid_apr_drg_code_count as (
    select 
        count(distinct aa.claim_id) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__apr_drg') }} bb
        on aa.apr_drg_code = bb.apr_drg_code
    left join {{ ref('data_quality__claim_type') }} cc
        on aa.claim_id = cc.claim_id
    where (aa.apr_drg_code is not null) 
        and (bb.apr_drg_code is null) 
        and cc.calculated_claim_type = 'institutional'
)

, invalid_apr_drg_code_perc as (
    select round(
        (select * from invalid_apr_drg_code_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    )
)

, dupe_apr_drg_code_count as (
    select cast(count(*) as {{ dbt.type_numeric() }})
    from (
        select 
            aa.claim_id
            , count(distinct apr_drg_code) as count_of_apr_drg_codes
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
        having count_of_apr_drg_codes > 1
    )
)

, dupe_apr_drg_code_perc as (
    select round(
        (select * from dupe_apr_drg_code_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    )
)

, missing_revenue_center_code_count as (
    select cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }})
    from (
        select
            aa.claim_id
            , max(aa.revenue_center_code) as max_revenue_center_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'institutional'
        group by aa.claim_id
    )
    where max_revenue_center_code is null
)

, missing_revenue_center_code_perc as (
    select round(
        (select * from missing_revenue_center_code_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    )
)

, invalid_revenue_center_code_count as (
    select count(distinct aa.claim_id)
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
        (select * from invalid_pos_count) * 100.0 /
        (select * from institutional_claims)
        , 1
    )
)

, missing_hcpcs_code_count as (
    select cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }})
    from (
        select
            aa.claim_id
            , max(aa.hcpcs_code) as max_hcpcs_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'professional'
        group by aa.claim_id
    )
    where max_hcpcs_code is null
)

, missing_hcpcs_code_perc as (
    select round(
        (select * from missing_hcpcs_code_count) * 100.0 /
        (select * from professional_claims)
        , 1
    )
)

, missing_rendering_npi_count as (
    select cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }})
    from {{ ref('medical_claim') }}
    where rendering_npi is null
)

, missing_rendering_npi_perc as (
    select round(
        (select * from missing_rendering_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    )
)

, invalid_rendering_npi_count as (
    select count(distinct aa.claim_id)
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
    )
)

, dupe_rendering_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }})
    from (
        select
            claim_id
            , count(distinct rendering_npi) as count_of_rendering_npis
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_rendering_npis > 1
    )
)

, dupe_rendering_npi_perc as (
    select round(
        (select * from dupe_rendering_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    )
)

, missing_rendering_tin_count as (
    select cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }})
    from {{ ref('medical_claim') }}
    where rendering_tin is null
)

, missing_rendering_tin_perc as (
    select round(
        (select * from missing_rendering_tin_count) * 100.0 /
        (select * from medical_claims)
        , 1
    )
)

, missing_billing_npi_count as (
    select cast(nullif(count(distinct claim_id), 0) as {{ dbt.type_numeric() }})
    from {{ ref('medical_claim') }}
    where billing_npi is null
)

, missing_billing_npi_perc as (
    select round(
        (select * from missing_billing_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    )
)

, invalid_billing_npi_count as (
    select count(distinct aa.claim_id)
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
    )
)

, dupe_billing_npi_count as (
    select cast(count(*) as {{ dbt.type_numeric() }})
    from (
        select
            claim_id
            , count(distinct billing_npi) as count_of_billing_npis
        from {{ ref('medical_claim') }}
        group by claim_id
        having count_of_billing_npis > 1
    )
)

, dupe_billing_npi_perc as (
    select round(
        (select * from dupe_billing_npi_count) * 100.0 /
        (select * from medical_claims)
        , 1
    )
)

, final as (
    select
        1 as rank_id
        , 'patient_id' as field
        , (select * from missing_patient_id_count) as missing_count
        , (select * from missing_patient_id_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_patient_id_count) as duplicated_count
        , (select * from dupe_patient_id_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        2 as rank_id
        , 'payer' as field
        , (select * from missing_payer_count) as missing_count
        , (select * from missing_payer_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_payer_count) as duplicated_count
        , (select * from dupe_payer_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        3 as rank_id
        , 'plan' as field
        , (select * from missing_plan_count) as missing_count
        , (select * from missing_plan_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_plan_count) as duplicated_count
        , (select * from dupe_plan_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        4 as rank_id
        , 'claim_start_date' as field
        , (select * from missing_claim_start_date_count) as missing_count
        , (select * from missing_claim_start_date_perc) as missing_perc
        , (select * from invalid_claim_start_date_count) as invalid_count
        , (select * from invalid_claim_start_date_perc) as invalid_perc
        , (select * from dupe_claim_start_date_count) as duplicated_count
        , (select * from dupe_claim_start_date_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        5 as rank_id
        , 'claim_end_date' as field
        , (select * from missing_claim_end_date_count) as missing_count
        , (select * from missing_claim_end_date_perc) as missing_perc
        , (select * from invalid_claim_end_date_count) as invalid_count
        , (select * from invalid_claim_end_date_perc) as invalid_perc
        , (select * from dupe_claim_end_date_count) as duplicated_count
        , (select * from dupe_claim_end_date_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        6 as rank_id
        , 'claim_line_start_date' as field
        , (select * from missing_claim_line_start_date_count) as missing_count
        , (select * from missing_claim_line_start_date_perc) as missing_perc
        , (select * from invalid_claim_line_start_date_count) as invalid_count
        , (select * from invalid_claim_line_start_date_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        7 as rank_id
        , 'claim_line_end_date' as field
        , (select * from missing_claim_line_end_date_count) as missing_count
        , (select * from missing_claim_line_end_date_perc) as missing_perc
        , (select * from invalid_claim_line_end_date_count) as invalid_count
        , (select * from invalid_claim_line_end_date_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        8 as rank_id
        , 'admission_date' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_admission_date_count) as invalid_count
        , (select * from invalid_admission_date_perc) as invalid_perc
        , (select * from dupe_admission_date_count) as duplicated_count
        , (select * from dupe_admission_date_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        9 as rank_id
        , 'discharge_date' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_discharge_date_count) as invalid_count
        , (select * from invalid_discharge_date_perc) as invalid_perc
        , (select * from dupe_discharge_date_count) as duplicated_count
        , (select * from dupe_discharge_date_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        10 as rank_id
        , 'discharge_disposition_code' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_ddc_count) as invalid_count
        , (select * from invalid_ddc_perc) as invalid_perc
        , (select * from dupe_ddc_count) as duplicated_count
        , (select * from dupe_ddc_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        11 as rank_id
        , 'place_of_service_code' as field
        , (select * from missing_pos_count) as missing_count
        , (select * from missing_pos_perc) as missing_perc
        , (select * from invalid_pos_count) as invalid_count
        , (select * from invalid_pos_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'professional' as claim_type

    union all

    select 
        12 as rank_id
        , 'bill_type_code' as field
        , (select * from missing_bill_type_code_count) as missing_count
        , (select * from missing_bill_type_code_perc) as missing_perc
        , (select * from invalid_bill_type_code_count) as invalid_count
        , (select * from invalid_bill_type_code_perc) as invalid_perc
        , (select * from dupe_bill_type_code_count) as duplicated_count
        , (select * from dupe_bill_type_code_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        13 as rank_id
        , 'ms_drg_code' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_ms_drg_code_count) as invalid_count
        , (select * from invalid_ms_drg_code_perc) as invalid_perc
        , (select * from dupe_ms_drg_code_count) as duplicated_count
        , (select * from dupe_ms_drg_code_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        14 as rank_id
        , 'apr_drg_code' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_apr_drg_code_count) as invalid_count
        , (select * from invalid_apr_drg_code_perc) as invalid_perc
        , (select * from dupe_apr_drg_code_count) as duplicated_count
        , (select * from dupe_apr_drg_code_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        15 as rank_id
        , 'revenue_center_code' as field
        , (select * from missing_revenue_center_code_count) as missing_count
        , (select * from missing_revenue_center_code_perc) as missing_perc
        , (select * from invalid_revenue_center_code_count) as invalid_count
        , (select * from invalid_revenue_center_code_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
        16 as rank_id
        , 'hcpcs_code' as field
        , (select * from missing_hcpcs_code_count) as missing_count
        , (select * from missing_hcpcs_code_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'professional' as claim_type

    union all

    select 
        17 as rank_id
        , 'rendering_npi' as field
        , (select * from missing_rendering_npi_count) as missing_count
        , (select * from missing_rendering_npi_perc) as missing_perc
        , (select * from invalid_rendering_npi_count) as invalid_count
        , (select * from invalid_rendering_npi_perc) as invalid_perc
        , (select * from dupe_rendering_npi_count) as duplicated_count
        , (select * from dupe_rendering_npi_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        18 as rank_id
        , 'rendering_tin' as field
        , (select * from missing_rendering_tin_count) as missing_count
        , (select * from missing_rendering_tin_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'all' as claim_type

    union all

    select 
        19 as rank_id
        , 'billing_npi' as field
        , (select * from missing_billing_npi_count) as missing_count
        , (select * from missing_billing_npi_perc) as missing_perc
        , (select * from invalid_billing_npi_count) as invalid_count
        , (select * from invalid_billing_npi_perc) as invalid_perc
        , (select * from dupe_billing_npi_count) as duplicated_count
        , (select * from dupe_billing_npi_perc) as duplicated_perc
        , 'all' as claim_type
)

select
      rank_id
    , field
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
