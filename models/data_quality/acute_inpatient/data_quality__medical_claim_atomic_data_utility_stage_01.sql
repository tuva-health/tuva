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

, missing_claim_type_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where claim_type is null
)

, missing_claim_type_perc as (
    select
        round(
            (select claim_count from missing_claim_type_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, invalid_claim_type_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('terminology__claim_type') }} bb
        on aa.claim_type = bb.claim_type
    where aa.claim_type is not null and bb.claim_type is null
)

, invalid_claim_type_perc as (
    select
        round(
            (select claim_count from invalid_claim_type_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_claim_type_count as (
    select
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct claim_type) as count_of_claim_types
        from {{ ref('medical_claim') }}
        group by claim_id
        having count(distinct claim_type) > 1
    ) as subquery
)

, dupe_claim_type_perc as (
    select
        round(
            (select claim_count from dupe_claim_type_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_person_id_count as (
    select
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }}
    where person_id is null
)

, missing_person_id_perc as (
    select
        round(
            (select claim_count from missing_person_id_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, dupe_person_id_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            claim_id,
            count(distinct person_id) as count_of_person_ids
        from {{ ref('medical_claim') }}
        group by claim_id
        having count(distinct person_id) > 1
    ) as subquery
)

, dupe_person_id_perc as (
    select 
        round(
            (select claim_count from dupe_person_id_count) * 100.0 /
            (select claim_count from medical_claims), 1
        ) as percentage
)

, missing_payer_count as (
    select 
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        having count(distinct payer) > 1
    ) as subquery
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        having count(distinct {{ quote_column('plan') }}) > 1
    ) as subquery
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_start_date = bb.full_date
    where (aa.claim_start_date is not null and bb.full_date is null)
        or (aa.claim_start_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
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
        having  count(distinct claim_start_date) > 1
    ) as subquery
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_end_date = bb.full_date
    where (aa.claim_end_date is not null and bb.full_date is null)
        or (aa.claim_end_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
         or (aa.claim_end_date < aa.claim_start_date)
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
        having count(distinct claim_end_date) > 1
    ) as subquery
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_line_start_date = bb.full_date
    where (aa.claim_line_start_date is not null and bb.full_date is null)
        or (aa.claim_line_start_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from {{ ref('medical_claim') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.claim_line_end_date = bb.full_date
    where (aa.claim_line_end_date is not null and bb.full_date is null)
        or (aa.claim_line_end_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))
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
    where ((aa.admission_date is not null 
        and bb.full_date is null)
        or (aa.admission_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date)))
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
        having count(distinct admission_date) > 1
    ) as subquery
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
    where ((aa.discharge_date is not null and bb.full_date is null)
            or aa.discharge_date > aa.admission_date)
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
        having count(distinct discharge_date) > 1
    ) as subquery
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
    where (aa.discharge_disposition_code is not null) and
            (bb.discharge_disposition_code is null) and
            (cc.calculated_claim_type = 'institutional')
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
        having count(distinct discharge_disposition_code) > 1
    ) as subquery
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
        cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as claim_count
    from (
        select
            aa.claim_id as claim_id,
            max(aa.place_of_service_code) as max_place_of_service_code
        from {{ ref('medical_claim') }} aa
        left join {{ ref('data_quality__claim_type') }} bb
            on aa.claim_id = bb.claim_id
        where bb.calculated_claim_type = 'professional'
        group by aa.claim_id
    ) as subquery
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
        having count(distinct aa.bill_type_code) > 1
    ) as subquery
)

, dupe_bill_type_code_perc as (
    select 
        round(
            (select claim_count from dupe_bill_type_code_count) * 100.0 /
            (select claim_count from institutional_claims), 1
        ) as percentage
)


, final1 as (
    select
          'claim_type' as field
        , (select * from missing_claim_type_count) as missing_count
        , (select * from missing_claim_type_perc) as missing_perc
        , (select * from invalid_claim_type_count) as invalid_count
        , (select * from invalid_claim_type_perc) as invalid_perc
        , (select * from dupe_claim_type_count) as duplicated_count
        , (select * from dupe_claim_type_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select
          'person_id' as field
        , (select * from missing_person_id_count) as missing_count
        , (select * from missing_person_id_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_person_id_count) as duplicated_count
        , (select * from dupe_person_id_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'payer' as field
        , (select * from missing_payer_count) as missing_count
        , (select * from missing_payer_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_payer_count) as duplicated_count
        , (select * from dupe_payer_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'plan' as field
        , (select * from missing_plan_count) as missing_count
        , (select * from missing_plan_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_plan_count) as duplicated_count
        , (select * from dupe_plan_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'claim_start_date' as field
        , (select * from missing_claim_start_date_count) as missing_count
        , (select * from missing_claim_start_date_perc) as missing_perc
        , (select * from invalid_claim_start_date_count) as invalid_count
        , (select * from invalid_claim_start_date_perc) as invalid_perc
        , (select * from dupe_claim_start_date_count) as duplicated_count
        , (select * from dupe_claim_start_date_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'claim_end_date' as field
        , (select * from missing_claim_end_date_count) as missing_count
        , (select * from missing_claim_end_date_perc) as missing_perc
        , (select * from invalid_claim_end_date_count) as invalid_count
        , (select * from invalid_claim_end_date_perc) as invalid_perc
        , (select * from dupe_claim_end_date_count) as duplicated_count
        , (select * from dupe_claim_end_date_perc) as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'claim_line_start_date' as field
        , (select * from missing_claim_line_start_date_count) as missing_count
        , (select * from missing_claim_line_start_date_perc) as missing_perc
        , (select * from invalid_claim_line_start_date_count) as invalid_count
        , (select * from invalid_claim_line_start_date_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'claim_line_end_date' as field
        , (select * from missing_claim_line_end_date_count) as missing_count
        , (select * from missing_claim_line_end_date_perc) as missing_perc
        , (select * from invalid_claim_line_end_date_count) as invalid_count
        , (select * from invalid_claim_line_end_date_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'all' as claim_type

    union all

    select 
          'admission_date' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_admission_date_count) as invalid_count
        , (select * from invalid_admission_date_perc) as invalid_perc
        , (select * from dupe_admission_date_count) as duplicated_count
        , (select * from dupe_admission_date_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select 
          'discharge_date' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_discharge_date_count) as invalid_count
        , (select * from invalid_discharge_date_perc) as invalid_perc
        , (select * from dupe_discharge_date_count) as duplicated_count
        , (select * from dupe_discharge_date_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select
          'discharge_disposition_code' as field
        , null as missing_count
        , null as missing_perc
        , (select * from invalid_ddc_count) as invalid_count
        , (select * from invalid_ddc_perc) as invalid_perc
        , (select * from dupe_ddc_count) as duplicated_count
        , (select * from dupe_ddc_perc) as duplicated_perc
        , 'institutional' as claim_type

    union all

    select
          'place_of_service_code' as field
        , (select * from missing_pos_count) as missing_count
        , (select * from missing_pos_perc) as missing_perc
        , (select * from invalid_pos_count) as invalid_count
        , (select * from invalid_pos_perc) as invalid_perc
        , null as duplicated_count
        , null as duplicated_perc
        , 'professional' as claim_type

    union all

    select
          'bill_type_code' as field
        , (select * from missing_bill_type_code_count) as missing_count
        , (select * from missing_bill_type_code_perc) as missing_perc
        , (select * from invalid_bill_type_code_count) as invalid_count
        , (select * from invalid_bill_type_code_perc) as invalid_perc
        , (select * from dupe_bill_type_code_count) as duplicated_count
        , (select * from dupe_bill_type_code_perc) as duplicated_perc
        , 'institutional' as claim_type

)

select
      field
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
    , claim_type
from final1
