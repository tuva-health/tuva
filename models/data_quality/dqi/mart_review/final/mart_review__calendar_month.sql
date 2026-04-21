{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('claims_enabled', var('clinical_enabled', False)) | as_bool)
   )
}}


select distinct
cast(year_month_int as {{ dbt.type_string() }}) as year_month
, full_date
, cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('reference_data__calendar') }} as c
where day = 1
