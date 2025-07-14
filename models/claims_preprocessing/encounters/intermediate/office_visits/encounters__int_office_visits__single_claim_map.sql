{{ dbt_utils.deduplicate(
    relation=ref('encounters__int_office_visits__union'),
    partition_by='encounter_id',
    order_by='priority_number',
   )
}}