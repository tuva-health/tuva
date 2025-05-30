{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      eligibility_id
    , person_id
    , member_id
    , payer_type
    , payer
    , {{ quote_column('plan') }}
    , enrollment_start_date
    , enrollment_end_date
    , subscriber_relation
    , subscriber_id
    , data_source
from {{ ref('core__eligibility') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      eligibility_id
    , person_id
    , member_id
    , payer_type
    , payer
    , {{ quote_column('plan') }}
    , enrollment_start_date
    , enrollment_end_date
    , subscriber_relation
    , subscriber_id
    , data_source
from {{ ref('core__eligibility') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }} ) as eligibility_id
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as payer_type
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as enrollment_start_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as enrollment_end_date
    , cast(null as {{ dbt.type_string() }} ) as subscriber_relation
    , cast(null as {{ dbt.type_string() }} ) as subscriber_id
    , cast(null as {{ dbt.type_string() }} ) as data_source
{{ limit_zero()}}

{%- endif %}
