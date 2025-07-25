{{ dbt_utils.deduplicate(
    relation=ref('encounters__int_office_visits__union'),
    partition_by='medical_claim_sk',
    order_by='priority_number',
   )
}}