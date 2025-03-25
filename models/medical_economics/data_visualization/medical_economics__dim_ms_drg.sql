with ms_drg as (

    select 
          *
        , CASE 
        WHEN MDC_Code = 'MDC 01' THEN 'Diseases and Disorders of the Nervous System'
        WHEN MDC_Code = 'MDC 02' THEN 'Diseases and Disorders of the Eye'
        WHEN MDC_Code = 'MDC 03' THEN 'Diseases and Disorders of the Ear, Nose, Mouth, and Throat'
        WHEN MDC_Code = 'MDC 04' THEN 'Diseases and Disorders of the Respiratory System'
        WHEN MDC_Code = 'MDC 05' THEN 'Diseases and Disorders of the Circulatory System'
        WHEN MDC_Code = 'MDC 06' THEN 'Diseases and Disorders of the Digestive System'
        WHEN MDC_Code = 'MDC 07' THEN 'Diseases and Disorders of the Hepatobiliary System and Pancreas'
        WHEN MDC_Code = 'MDC 08' THEN 'Diseases and Disorders of the Musculoskeletal System and Connective Tissue'
        WHEN MDC_Code = 'MDC 09' THEN 'Diseases and Disorders of the Skin, Subcutaneous Tissue, and Breast'
        WHEN MDC_Code = 'MDC 10' THEN 'Diseases and Disorders of the Endocrine System'
        WHEN MDC_Code = 'MDC 11' THEN 'Diseases and Disorders of the Kidney and Urinary Tract'
        WHEN MDC_Code = 'MDC 12' THEN 'Diseases and Disorders of the Male Reproductive System'
        WHEN MDC_Code = 'MDC 13' THEN 'Diseases and Disorders of the Female Reproductive System'
        WHEN MDC_Code = 'MDC 14' THEN 'Pregnancy, Childbirth, and the Puerperium'
        WHEN MDC_Code = 'MDC 15' THEN 'Newborns and Other Neonates with Conditions Originating in the Perinatal Period'
        WHEN MDC_Code = 'MDC 16' THEN 'Diseases and Disorders of the Blood and Blood-Forming Organs'
        WHEN MDC_Code = 'MDC 17' THEN 'Neoplastic Diseases'
        WHEN MDC_Code = 'MDC 18' THEN 'Infectious and Parasitic Diseases'
        WHEN MDC_Code = 'MDC 19' THEN 'Mental Diseases and Disorders'
        WHEN MDC_Code = 'MDC 20' THEN 'Alcohol/Drug Use and Alcohol/Drug Induced Organic Mental Disorders'
        WHEN MDC_Code = 'MDC 21' THEN 'Injury, Poisoning, and Toxic Effects of Drugs'
        WHEN MDC_Code = 'MDC 22' THEN 'Burns'
        WHEN MDC_Code = 'MDC 23' THEN 'Human Immunodeficiency Virus Infections'
        WHEN MDC_Code = 'MDC 24' THEN 'Other Factors Influencing Health Status and Contact with Health Services'
        ELSE 'No MDC assignment'
    END AS MDC_Description
    from {{ ref('terminology__ms_drg') }}

)

select * 
from ms_drg