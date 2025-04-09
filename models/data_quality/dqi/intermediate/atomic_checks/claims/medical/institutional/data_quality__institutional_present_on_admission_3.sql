{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
    select *
    from {{ ref('medical_claim') }}
    where claim_type = 'institutional'
),
unique_field as (
    select distinct claim_id
        ,cast(diagnosis_poa_3 as {{ dbt.type_string() }})  as field
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
    data_source
    , {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} as field_aggregated -- noqa
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
    ,'DIAGNOSIS_POA_3' as field_name
       ,case when cg.frequency > 1                then 'multiple'
        when m.diagnosis_poa_3 in ('y','n')       then 'valid'
        when m.diagnosis_poa_3 is null            then 'null'
                                                  else 'invalid' end as bucket_name
    ,cast(null as {{ dbt.type_string() }}) as invalid_reason
    ,cast({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
left outer join claim_grain as cg on m.claim_id = cg.claim_id and m.data_source = cg.data_source
left outer join claim_agg as agg on m.claim_id = agg.claim_id and m.data_source = agg.data_source
