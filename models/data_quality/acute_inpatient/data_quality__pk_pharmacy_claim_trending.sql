{{ config(
    enabled = var('claims_enabled', False)
) }}

with pharmacy_claim_rows as (

  select
      ingest_datetime
    , count(*) as pharmacy_claim_row_count
  from {{ ref('pharmacy_claim') }}
  group by ingest_datetime

)

, pharmacy_claim_distinct_pk as (

  select
      ingest_datetime
    , count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as pharmacy_claim_pk_count
  from {{ ref('pharmacy_claim') }}
  group by ingest_datetime

)

, join_both as (

  select
      aa.ingest_datetime
    , aa.pharmacy_claim_row_count
    , bb.pharmacy_claim_pk_count
    , case
        when (aa.pharmacy_claim_row_count = bb.pharmacy_claim_pk_count) then 'YES'
        else 'NO'
      end as correct_pk
  from pharmacy_claim_rows aa
  left join pharmacy_claim_distinct_pk bb
    on (aa.ingest_datetime = bb.ingest_datetime)
       or (aa.ingest_datetime is null and bb.ingest_datetime is null)

)

select
    ingest_datetime
  , pharmacy_claim_row_count
  , pharmacy_claim_pk_count
  , correct_pk
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from join_both
