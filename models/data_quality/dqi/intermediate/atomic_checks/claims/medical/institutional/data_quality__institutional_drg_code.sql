{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
select *
from {{ ref('data_quality__stg_institutional_inpatient') }}
),

unique_field as (
    select distinct claim_id
        , {{ concat_custom(["base.drg_code", "'|'", "coalesce(ms.ms_drg_description, apr.apr_drg_description, '')"]) }} as field
        , data_source
    from base
    left join {{ ref('terminology__ms_drg')}} as ms on base.drg_code = ms.ms_drg_code and base.drg_code_type = 'ms-drg'
    left join {{ ref('terminology__apr_drg')}} as apr on base.drg_code = apr.apr_drg_code and base.drg_code_type = 'apr-drg'
),

claim_grain as (
    select claim_id
        , data_source
        , count(*) as frequency
    from unique_field
    group by claim_id
        , data_source
),

claim_agg as (
select
      claim_id
    , data_source
    , {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} as field_aggregated
from
    unique_field
group by
      claim_id
    , data_source

)

select distinct -- to bring to claim_id grain
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' AS table_name
    , 'Claim ID' AS drill_down_key
    , coalesce(m.claim_id, 'NULL') AS drill_down_value
    , 'institutional' AS claim_type
    , 'DRG_CODE' AS field_name
    , case when cg.frequency > 1                then 'multiple'
          when coalesce(ms.ms_drg_code, apr.apr_drg_code) is not null then 'valid'
          when m.drg_code is not null    then 'invalid'
                                               else 'null' end as bucket_name
    , case
        when cg.frequency = 1
            and m.drg_code is not null
            and coalesce(ms.ms_drg_code, apr.apr_drg_code) is null
            then 'DRG code does not join to Terminology DRG table'
        else null
    end as invalid_reason
    , cast({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from base m
left join claim_grain cg on m.claim_id = cg.claim_id and m.data_source = cg.data_source
left join {{ ref('terminology__ms_drg')}} as ms on m.drg_code = ms.ms_drg_code and m.drg_code_type = 'ms-drg'
left join {{ ref('terminology__apr_drg')}} as apr on m.drg_code = apr.apr_drg_code and m.drg_code_type = 'apr-drg'
left join claim_agg agg on m.claim_id = agg.claim_id and m.data_source = agg.data_source