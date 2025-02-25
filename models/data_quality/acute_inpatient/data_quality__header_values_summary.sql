{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      field
    , field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__header_values_graph') }}
where field in (
      '(inst claims with usable bill type) / (total inst claims) * 100',
      '(inst claims with usable drg) / (total inst claims) * 100',
      '(inst claims with usable admit type) / (total inst claims) * 100',
      '(inst claims with usable admit source) / (total inst claims) * 100',
      '(inst claims with usable discharge disp) / (total inst claims) * 100',
      '(inst claims with usable dx1) / (total inst claims) * 100',
      '(prof claims with usable dx1) / (total prof claims) * 100',
      '(claims with usable dx1) / (total claims) * 100'
)
