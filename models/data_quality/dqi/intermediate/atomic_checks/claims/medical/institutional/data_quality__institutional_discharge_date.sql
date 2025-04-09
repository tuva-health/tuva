{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
select *
from {{ ref('data_quality__stg_institutional_inpatient') }}
)
, tuva_last_run as(
    select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as tuva_last_run
)
,unique_field as (
    select distinct claim_id
        ,cast(discharge_date as {{ dbt.type_string() }}) as field
        ,data_source
    from base
),
claim_grain as (
    select claim_id
        ,data_source
        ,count(*) as frequency
    from unique_field
    group by claim_id
        ,data_source
),
claim_agg as (
select
    claim_id,
    data_source,
    {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} as field_aggregated -- noqa
from
    unique_field
group by
    claim_id,
    data_source
)
select distinct -- to bring to claim_id grain
    m.data_source
    ,coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'MEDICAL_CLAIM' as table_name
    ,'Claim ID' as drill_down_key
    ,coalesce(m.claim_id, 'NULL') as drill_down_value
    ,'institutional' as claim_type
    ,'DISCHARGE_DATE' as field_name
    ,case
        when cg.frequency > 1 then 'multiple'
        when m.discharge_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.discharge_date < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} then 'invalid'
        when m.discharge_date < m.admission_date then 'invalid'
        when m.discharge_date is null then 'null'
        else 'valid'
    end as bucket_name
    ,case
        when cg.frequency > 1 then 'multiple'
        when m.discharge_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.discharge_date < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} then 'too old'
        when m.discharge_date < m.admission_date then 'discharge date before admission date'
        else null
    end as invalid_reason
    ,cast({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
left outer join claim_grain as cg on m.claim_id = cg.claim_id and m.data_source = cg.data_source
left outer join claim_agg as agg on m.claim_id = agg.claim_id and m.data_source = agg.data_source
cross join tuva_last_run as cte
