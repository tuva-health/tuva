{{ config(
    enabled = var('claims_enabled', False)
) }}

with medical_claim_rows as (

  select
      ingest_datetime
    , count(*) as medical_claim_row_count
  from {{ ref('medical_claim') }}
  group by ingest_datetime

)

, medical_claim_distinct_pk as (

  select
      ingest_datetime
    , count(distinct {{ dbt.concat(["claim_id", "'|'", "cast(claim_line_number as " ~ dbt.type_string() ~ ")"]) }}) as medical_claim_pk_count
  from {{ ref('medical_claim') }}
  group by ingest_datetime

)

, join_both as (

  select
      aa.ingest_datetime
    , aa.medical_claim_row_count
    , bb.medical_claim_pk_count
    , case
        when (aa.medical_claim_row_count = bb.medical_claim_pk_count) then 'YES'
        else 'NO'
      end as correct_pk
  from medical_claim_rows aa
  left join medical_claim_distinct_pk bb
    on (aa.ingest_datetime = bb.ingest_datetime)
       or (aa.ingest_datetime is null and bb.ingest_datetime is null)

)

select
    ingest_datetime
  , medical_claim_row_count
  , medical_claim_pk_count
  , correct_pk
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from join_both
