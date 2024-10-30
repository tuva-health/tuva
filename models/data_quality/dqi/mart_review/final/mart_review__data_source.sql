{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with data_sources as (
SELECT DISTINCT data_source
FROM {{ ref('core__condition')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__eligibility')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__encounter')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__location')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__medical_claim')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__member_months')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__patient')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__pharmacy_claim')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__practitioner')}}

UNION ALL

SELECT DISTINCT data_source
FROM {{ ref('core__procedure')}}
)

SELECT DISTINCT
    data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from data_sources