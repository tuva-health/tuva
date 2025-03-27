{{ config(
     enabled = var('clinical_enabled',False)
   )
}}

with valid_conditions as (
    select
        *
    from
        {{ ref('data_quality__patient_patient_id') }}
    where
        bucket_name = 'valid'
)
, uniqueness_check as (
        select
                field_value
                , COUNT(*) as duplicate_count
        from
                valid_conditions
        group by
                field_value
        having
                COUNT(*) > 1
)

, random_sample as (
    select
        data_source
        , source_date
        , table_name
        , drill_down_key
        , drill_down_value
        , field_name
        , field_value
        , bucket_name
        , ROW_NUMBER() over (
order by drill_down_key) as row_number_value
    from
        {{ ref('data_quality__patient_patient_id') }}
    where
        bucket_name = 'valid'

)

, duplicates_summary as (
    select
        a.data_source
        , a.source_date
        , a.table_name
        , a.drill_down_key
        , a.drill_down_value
        , a.field_name
        , a.field_value
        , a.bucket_name
        , b.duplicate_count
        , ROW_NUMBER() over (
order by drill_down_key) as row_number_value
    from
        {{ ref('data_quality__patient_patient_id') }} as a
    inner join
        uniqueness_check as b on a.field_value = b.field_value
)

select
    *
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from
    duplicates_summary
where row_number_value <= 5

union all

select
    *
    , 0 as duplicate_count
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from
    random_sample
where
    row_number_value <= 5
    and not exists (select 1 from duplicates_summary)
