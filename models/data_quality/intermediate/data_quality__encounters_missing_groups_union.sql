
select
    'number of missing encounter groups' as data_quality_check
  , count(*) as result_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__encounters_missing_groups') }}
