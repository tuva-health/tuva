{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with all_medications as (

    select
          person_id
        , payer
        , dispensing_date
        , drug_code
        , code_system
        , data_source
    from {{ ref('hcc_suspecting__int_all_medications') }}

)

, seed_clinical_concepts as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('hcc_suspecting__clinical_concepts') }}

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
        , 'CMS-HCC-V28' as model_version
    from {{ ref('hcc_suspecting__hcc_descriptions') }}

)

, billed_hccs as (

    select distinct
          person_id
        , payer
        , data_source
        , model_version
        , hcc_code
        , current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}

)

/* BEGIN HCC 155 logic (Major Depression, Moderate or Severe, without Psychosis)

   antidepressant medication taken within the past five years
*/
, hcc_155_suspect as (

    select
          all_medications.person_id
        , all_medications.payer
        , all_medications.dispensing_date
        , all_medications.drug_code
        , all_medications.code_system
        , all_medications.data_source
        , seed_clinical_concepts.concept_name
        , seed_hcc_descriptions.model_version
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from all_medications
        inner join seed_clinical_concepts
            on all_medications.code_system = seed_clinical_concepts.code_system
            and all_medications.drug_code = seed_clinical_concepts.code
        cross join seed_hcc_descriptions
    where lower(seed_clinical_concepts.concept_name) = 'antidepressant medication'
        and all_medications.dispensing_date >= {{ dbt.dateadd (
                datepart = "year"
                , interval = -5
                , from_date_or_timestamp = dbt.current_timestamp()
            ) }}
        and seed_hcc_descriptions.hcc_code = '155'

)
/* END HCC 155 logic */

, unioned as (

    select * from hcc_155_suspect

)

, add_billed_flag as (

    select
          unioned.person_id
        , unioned.payer
        , unioned.data_source
        , unioned.model_version
        , unioned.hcc_code
        , unioned.hcc_description
        , unioned.concept_name
        , unioned.dispensing_date
        , unioned.drug_code
        , billed_hccs.current_year_billed
    from unioned
        left outer join billed_hccs
            on unioned.person_id = billed_hccs.person_id
            and unioned.payer = billed_hccs.payer
            and unioned.data_source = billed_hccs.data_source
            and unioned.hcc_code = billed_hccs.hcc_code
            and unioned.model_version = billed_hccs.model_version
)

, add_standard_fields as (

    select
          person_id
        , payer
        , data_source
        , model_version
        , hcc_code
        , hcc_description
        , dispensing_date
        , drug_code
        , current_year_billed
        , cast('Medication suspect' as {{ dbt.type_string() }}) as reason
        , {{ concat_custom([
            "concept_name",
            "drug_code",
            "') dispensed on '",
            "dispensing_date" ]) }} as contributing_factor
        , dispensing_date as suspect_date
    from add_billed_flag

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(dispensing_date as date) as dispensing_date
        , cast(drug_code as {{ dbt.type_string() }}) as drug_code
        {% if target.type == 'fabric' %}
            , cast(current_year_billed as bit) as current_year_billed
        {% else %}
            , cast(current_year_billed as boolean) as current_year_billed
        {% endif %}
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
        , cast(suspect_date as date) as suspect_date
    from add_standard_fields

)

select
      person_id
    , payer
    , data_source
    , model_version
    , hcc_code
    , hcc_description
    , dispensing_date
    , drug_code
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types
