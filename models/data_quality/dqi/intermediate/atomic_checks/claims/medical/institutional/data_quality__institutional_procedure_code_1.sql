{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
select *
from {{ ref('data_quality__stg_institutional_inpatient') }}
),

unique_field as (
    select distinct claim_id
        , {{ concat_custom(["procedure_code_1", "'|'", "coalesce(term.description, '')"]) }} as field
        , data_source
    from base
    left outer join {{ ref('terminology__icd_10_pcs') }} as term on base.procedure_code_1 = term.icd_10_pcs
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
    , {{ dbt.listagg(measure="coalesce(field, 'null')", delimiter_text="', '", order_by_clause="order by field desc") }} as field_aggregated -- noqa
from
    unique_field
group by
      claim_id
    , data_source
	)

select distinct -- to bring to claim_id grain
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' as table_name
    , 'Claim ID' as drill_down_key
    , coalesce(m.claim_id, 'NULL') as drill_down_value
    , 'institutional' as claim_type
    , 'PROCEDURE_CODE_1' as field_name
    , case when cg.frequency > 1                then 'multiple'
          when term.icd_10_pcs is not null      then 'valid'
          when m.procedure_code_1 is not null  then 'invalid'
                                               else 'null' end as bucket_name
    , case
        when cg.frequency = 1
            and m.procedure_code_1 is not null
            and term.icd_10_pcs is null
            then 'procedure code does not join to terminology procedure code table'
        else null
    end as invalid_reason
    , cast({{ substring('agg.field_aggregated', 1, 255) }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
left outer join claim_grain as cg on m.claim_id = cg.claim_id and m.data_source = cg.data_source
left outer join {{ ref('terminology__icd_10_pcs') }} as term on m.procedure_code_1 = term.icd_10_pcs
left outer join claim_agg as agg on m.claim_id = agg.claim_id and m.data_source = agg.data_source
