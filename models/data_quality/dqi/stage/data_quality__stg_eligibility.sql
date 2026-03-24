{{ config(
     enabled = var('claims_enabled', False)
 | as_bool
   )
}}

select * from {{ ref('eligibility') }}
