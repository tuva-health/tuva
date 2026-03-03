{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set tuva_core_columns -%}
      unioned.person_id
    , unioned.name_suffix
    , unioned.first_name
    , unioned.middle_name
    , unioned.last_name
    , unioned.sex
    , unioned.race
    , unioned.birth_date
    , unioned.death_date
    , unioned.death_flag
    , unioned.social_security_number
    , unioned.address
    , unioned.city
    , unioned.state
    , unioned.zip_code
    , unioned.county
    , unioned.latitude
    , unioned.longitude
    , unioned.phone
    , unioned.email
    , unioned.ethnicity
    , unioned.age
    , unioned.age_group
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , unioned.data_source
    , unioned.tuva_last_run
{%- endset -%}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{# When both claims and clinical enabled, use eligibility extensions (claims takes priority) #}
{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__eligibility')) }}
{%- endset -%}

with person_list_to_exclude_because_in_claims as (
    select distinct person_id
    from {{ ref('core__stg_claims_patient') }}
)

, unioned as (
    {{ smart_union([ref('core__stg_claims_patient'), ref('core__stg_clinical_patient')]) }}
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from unioned
where _source = 1

union all

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from unioned
left outer join person_list_to_exclude_because_in_claims as pltebic
    on unioned.person_id = pltebic.person_id
/* IF EXISTS IN CLAIMS, CHOOSE CLAIMS RECORD OVER CLINICAL RECORD */
where _source = 2
  and pltebic.person_id is null

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_core_columns_clinical -%}
      person_id
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
    , ethnicity
    , age
    , age_group
{%- endset -%}

{%- set tuva_metadata_columns_clinical -%}
    , data_source
    , tuva_last_run
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__patient')) }}
{%- endset -%}

select
    {{ tuva_core_columns_clinical }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns_clinical }}
from {{ ref('core__stg_clinical_patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{%- set tuva_core_columns_claims -%}
      person_id
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
    , ethnicity
    , age
    , age_group
{%- endset -%}

{%- set tuva_metadata_columns_claims -%}
    , data_source
    , tuva_last_run
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__eligibility')) }}
{%- endset -%}

select
    {{ tuva_core_columns_claims }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns_claims }}
from {{ ref('core__stg_claims_patient') }}

{%- endif %}
