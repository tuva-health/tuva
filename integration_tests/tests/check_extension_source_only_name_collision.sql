-- A stripped extension name may match a standard source column that the Core
-- output intentionally omits. Only fixed columns in the current Core output
-- reserve final names.

{% set passthrough_config = var('passthrough', {}) %}
{% set extension_prefix = passthrough_config.get('prefix', 'x_') %}
{% set strip_prefix = passthrough_config.get('strip', false) %}

{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            and strip_prefix
            and (extension_prefix | lower) in ['x_', 'ext_'],
     tags = ['extension_columns'],
     severity = 'error'
   )
}}

select
      eligibility_id
    , 'stripped source-only name did not preserve the extension value' as failure_reason
from {{ ref('core__eligibility') }}
where first_name <> 'extension-first-name'
   or first_name is null
