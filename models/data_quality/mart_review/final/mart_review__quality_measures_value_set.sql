select *    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('quality_measures__measures') }} p