select 
      txnmy.* 
    , "NPI" as npi
from {{source('phds_lakehouse_test','provider_taxonomy_codes')}} txnmy