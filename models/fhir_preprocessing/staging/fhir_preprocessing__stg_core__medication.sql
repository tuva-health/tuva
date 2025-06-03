{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      medication_id
    , person_id
    , source_code_type
    , ndc_code
    , ndc_description
    , rxnorm_code
    , rxnorm_description
    , days_supply
    , dispensing_date
    , data_source
from {{ ref('core__medication') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      medication_id
    , person_id
    , source_code_type
    , ndc_code
    , ndc_description
    , rxnorm_code
    , rxnorm_description
    , days_supply
    , dispensing_date
    , data_source
from {{ ref('core__medication') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }} ) as medication_id
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_string() }} ) as ndc_description
    , cast(null as {{ dbt.type_string() }} ) as rxnorm_code
    , cast(null as {{ dbt.type_string() }} ) as rxnorm_description
    , cast(null as {{ dbt.type_string() }} ) as days_supply
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as data_source
{{ limit_zero()}}

{%- endif %}
