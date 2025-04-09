{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('normalized_input__eligibility') }}
union all
select distinct
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(null as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('input_layer__patient') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(null as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('input_layer__patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select distinct
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('normalized_input__eligibility') }}

{%- endif %}
