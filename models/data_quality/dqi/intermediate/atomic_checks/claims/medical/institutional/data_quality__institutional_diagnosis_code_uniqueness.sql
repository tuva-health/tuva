{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
    select
          claim_id
        , data_source
        , claim_start_date
        , diagnosis_code_1
        , diagnosis_code_2
        , diagnosis_code_3
        , diagnosis_code_4
        , diagnosis_code_5
        , diagnosis_code_6
        , diagnosis_code_7
        , diagnosis_code_8
        , diagnosis_code_9
        , diagnosis_code_10
        , diagnosis_code_11
        , diagnosis_code_12
        , diagnosis_code_13
        , diagnosis_code_14
        , diagnosis_code_15
        , diagnosis_code_16
        , diagnosis_code_17
        , diagnosis_code_18
        , diagnosis_code_19
        , diagnosis_code_20
        , diagnosis_code_21
        , diagnosis_code_22
        , diagnosis_code_23
        , diagnosis_code_24
        , diagnosis_code_25
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'
),

unpivot_dx as (
    {% for i in range(1, 26) %}
    select
          claim_id
        , data_source
        , claim_start_date
        , 'diagnosis_code_{{ i }}' as dx_position
        , diagnosis_code_{{ i }} as diagnosis_code
    from base
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

duplicate_codes as (
    select
          claim_id
        , data_source
        , diagnosis_code
        , count(*) as occurrences
    from unpivot_dx
    group by
          claim_id
        , data_source
        , diagnosis_code
    having count(*) > 1
),

claim_duplicates as (
    select
          d.claim_id
        , d.data_source
        , {{ dbt.listagg(
              measure=dbt.concat(["d.diagnosis_code", "' ('", "cast(d.occurrences as " ~ dbt.type_string() ~ ")", "'x)'"]),
              delimiter_text="', '",
              order_by_clause="order by d.diagnosis_code"
           ) }} as duplicate_codes
    from duplicate_codes as d
    group by
          d.claim_id
        , d.data_source
)

select distinct
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}), cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' as table_name
    , 'Claim ID' as drill_down_key
    , coalesce(m.claim_id, 'NULL') as drill_down_value
    , 'institutional' as claim_type
    , 'DIAGNOSIS_CODE_UNIQUENESS' as field_name
    , case
        when dup.claim_id is not null then 'duplicate'
        else 'valid'
      end as bucket_name
    , case
        when dup.claim_id is not null
            then 'Same diagnosis code appears in multiple positions'
        else null
      end as invalid_reason
    , cast({{ substring('dup.duplicate_codes', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from base as m
left outer join claim_duplicates as dup
    on m.claim_id = dup.claim_id
    and m.data_source = dup.data_source
