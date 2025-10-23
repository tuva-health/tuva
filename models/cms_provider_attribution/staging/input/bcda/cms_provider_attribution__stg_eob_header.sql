{{ config(
     enabled = var('cms_provider_attribution_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

with union_cte as (
    select *
         , 'main' as runout_type
    from {{ source('phds_lakehouse_test','yak_bcda_explanationofbenefit_header') }}

)

,cte as (
    select *
         , row_number() over (
             partition by id
             order by meta_lastupdated desc, file_date desc
           ) as most_recent_record
    from union_cte
)

select 
id
,identifier_0_system
,identifier_0_type_coding_0_code
,identifier_0_type_coding_0_display
,identifier_0_type_coding_0_system
,identifier_0_value
,identifier_1_system
,identifier_1_type_coding_0_code
,identifier_1_type_coding_0_display
,identifier_1_type_coding_0_system
,identifier_1_value
,identifier_2_system
,identifier_2_type_coding_0_code
,identifier_2_type_coding_0_display
,identifier_2_type_coding_0_system
,identifier_2_value
,[status]
,created
,[use]
,disposition
,payment_date
,billableperiod_start
,billableperiod_end
,billableperiod_extension_0_url
,billableperiod_extension_0_valuecoding_code
,billableperiod_extension_0_valuecoding_display
,billableperiod_extension_0_valuecoding_system
,contained_0_active
,contained_0_id
,contained_0_identifier_0_type_coding_0_code
,contained_0_identifier_0_type_coding_0_system
,contained_0_identifier_0_value
,contained_0_identifier_1_system
,contained_0_identifier_1_type_coding_0_code
,contained_0_identifier_1_type_coding_0_system
,contained_0_identifier_1_value
,contained_0_meta_profile_0
,contained_0_name
,contained_0_resourcetype
,facility_extension_0_url
,facility_extension_0_valuecoding_code
,facility_extension_0_valuecoding_display
,facility_extension_0_valuecoding_system
,facility_identifier_type_coding_0_code
,facility_identifier_type_coding_0_system
,facility_identifier_value
,provider_identifier_system
,provider_identifier_value
,provider_reference
,insurance_0_coverage_reference
,insurance_0_focal
,insurer_identifier_value
,insurance_0_coverage_extension_0_url
,insurance_0_coverage_extension_0_valueidentifier_system
,insurance_0_coverage_extension_0_valueidentifier_value
,insurance_0_coverage_extension_1_url
,insurance_0_coverage_extension_1_valueidentifier_system
,insurance_0_coverage_extension_1_valueidentifier_value
,outcome
,patient_reference
,payment_amount_currency
,cast(payment_amount_value as decimal(18,2)) as payment_amount_value
,resourcetype
,benefitbalance_0_category_coding_0_code
,benefitbalance_0_category_coding_0_display
,benefitbalance_0_category_coding_0_system
,total_0_amount_currency
,total_0_amount_value
,total_0_category_coding_0_code
,total_0_category_coding_0_display
,total_0_category_coding_0_system
,total_0_category_coding_1_code
,total_0_category_coding_1_display
,total_0_category_coding_1_system
,total_1_amount_currency
,total_1_amount_value
,total_1_category_coding_0_code
,total_1_category_coding_0_display
,total_1_category_coding_0_system
,type_coding_0_code
,type_coding_0_display
,type_coding_0_system
,type_coding_1_code
,type_coding_1_display
,type_coding_1_system
,type_coding_2_code
,type_coding_2_display
,type_coding_2_system
,subtype_coding_0_code
,subtype_coding_0_system
,subtype_text
,meta_lastupdated
,meta_profile_0
,[filename]
,processed_datetime
,ingest_datetime
,since_date
,file_date
,file_name
from cte
where most_recent_record = 1