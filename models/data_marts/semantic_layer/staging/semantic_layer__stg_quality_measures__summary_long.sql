{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    sl.person_id
  , sl.denominator_flag
  , sl.numerator_flag
  , sl.exclusion_flag
  , sl.performance_flag
  , sl.evidence_date
  , sl.evidence_value
  , sl.exclusion_date
  , sl.exclusion_reason
  , sl.performance_period_begin
  , sl.performance_period_end
  , sl.measure_id
  , sl.measure_name
  , sl.measure_version
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('quality_measures__summary_long') }} sl