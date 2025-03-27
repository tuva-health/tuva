{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
    select * 
    from {{ ref('eligibility') }}

),
unique_field as (
    select distinct member_id
        ,cast(death_date as {{ dbt.type_string() }}) as field
        ,data_source
    from base
),
claim_grain as (
    select member_id
        ,data_source
        ,count(*) as frequency
    from unique_field
    group by member_id
        ,data_source
),
claim_agg as (
select
    member_id,
    data_source
    , {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} as field_aggregated -- noqa
from
    unique_field
group by
    data_source,
    member_id
)
select distinct
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' as table_name
    ,'Member ID' as drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' as claim_type
    ,'DEATH_DATE' as field_name
    ,case
        when cg.frequency > 1 then 'multiple'
        when m.death_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.death_date <= cast('1901-01-01' as date) then 'invalid'
        when m.death_date <= m.birth_date then 'invalid'
        when m.death_date is null then 'null'
        else 'valid'
    end as bucket_name
    ,case
        when cg.frequency > 1 then 'multiple'
        when m.death_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.death_date <= cast('1901-01-01' as date) then 'too old'
        when m.death_date <= m.birth_date then 'death date before birth date'
        else null
    end as invalid_reason
    ,cast({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
left outer join claim_grain as cg on m.member_id = cg.member_id and m.data_source = cg.data_source
left outer join claim_agg as agg on m.member_id = agg.member_id and m.data_source = agg.data_source
