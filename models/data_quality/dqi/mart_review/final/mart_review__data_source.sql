{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with data_sources as (
select distinct data_source
from {{ ref('core__condition') }}

union all

select distinct data_source
from {{ ref('core__eligibility') }}

union all

select distinct data_source
from {{ ref('core__encounter') }}

union all

select distinct data_source
from {{ ref('core__location') }}

union all

select distinct data_source
from {{ ref('core__medical_claim') }}

union all

select distinct data_source
from {{ ref('core__member_months') }}

union all

select distinct data_source
from {{ ref('core__patient') }}

union all

select distinct data_source
from {{ ref('core__pharmacy_claim') }}

union all

select distinct data_source
from {{ ref('core__practitioner') }}

union all

select distinct data_source
from {{ ref('core__procedure') }}
)

select distinct
    data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from data_sources
