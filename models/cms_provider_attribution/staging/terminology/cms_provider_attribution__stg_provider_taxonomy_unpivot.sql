select
      cast(prov.npi as varchar) as npi
    , taxonomy_col
    , taxonomy_code
from {{ref('terminology__provider_taxonomy_unpivot')}}