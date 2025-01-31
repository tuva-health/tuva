select distinct 
      comparative_population_id
    , comparative_population
from {{ ref('medical_economics__fact_comparative_population') }}