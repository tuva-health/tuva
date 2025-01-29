with condition_grouper_distinct as (

    select distinct 
          condition_grouper_1
        , condition_grouper_2
        , condition_grouper_3
    from {{ ref('medical_economics__specialty_condition_grouper_medical_claim') }}

),

condition_grouper_final as (

    select  
          condition_grouper_1
        , condition_grouper_2
        , condition_grouper_3
        , row_number() over (
            order by 
                  condition_grouper_1
                , condition_grouper_2
                , condition_grouper_3
        ) as condition_grouper_id
        , case 
            when condition_grouper_1 = 'Congenital malformations, deformations and chromosomal abnormalities' 
            then 'Birth defects'
            when condition_grouper_1 = 'Diseases of the digestive system' 
            then 'Digestive issues'
            when condition_grouper_1 = 'Diseases of the genitourinary system' 
            then 'Urinary/reproductive issues'
            when condition_grouper_1 = 'Factors influencing health status and contact with health services' 
            then 'Health factors'
            when condition_grouper_1 = 'Diseases of the respiratory system' 
            then 'Respiratory issues'
            when condition_grouper_1 = 'Injury, poisoning and certain other consequences of external causes' 
            then 'Injuries/poisoning'
            when condition_grouper_1 = 'Endocrine, nutritional, and metabolic diseases' 
            then 'Hormonal/metabolic issues'
            when condition_grouper_1 = 'Certain conditions originating in the perinatal period' 
            then 'Perinatal conditions'
            when condition_grouper_1 = 'Certain infectious and parasitic diseases' 
            then 'Infections/parasitic diseases'
            when condition_grouper_1 = 'Diseases of the ear and mastoid process' 
            then 'Ear problems'
            when condition_grouper_1 = 'Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism' 
            then 'Blood/immune issues'
            when condition_grouper_1 = 'Pregnancy, childbirth, and the puerperium' 
            then 'Pregnancy/childbirth'
            when condition_grouper_1 = 'Mental, behavioral, and neurodevelopmental disorders' 
            then 'Mental health'
            when condition_grouper_1 = 'Dental diseases (added in v2023.1)' 
            then 'Dental issues'
            when condition_grouper_1 = 'Neoplasms' 
            then 'Cancers'
            when condition_grouper_1 = 'Diseases of the skin and subcutaneous tissue' 
            then 'Skin problems'
            when condition_grouper_1 = 'Diseases of the eye and adnexa' 
            then 'Eye problems'
            when condition_grouper_1 = 'External causes of morbidity' 
            then 'External causes'
            when condition_grouper_1 = 'Diseases of the nervous system' 
            then 'Neurological issues'
            when condition_grouper_1 = 'Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified' 
            then 'Unexplained symptoms'
            when condition_grouper_1 = 'Diseases of the circulatory system' 
            then 'Heart issues'
            when condition_grouper_1 = 'Diseases of the musculoskeletal system and connective tissue' 
            then 'Musculoskeletal issues'
            else null 
        end as condition_grouper_1_simplified
    from condition_grouper_distinct

)

select * 
from condition_grouper_final