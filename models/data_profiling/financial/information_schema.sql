select
  lower(table_schema) as table_schema
  , lower(table_name) as table_name
  , lower(column_name) as column_name
  , is_nullable = 'YES' as is_nullable
  , lower(data_type) as data_type
  from information_schema.columns
where lower(table_name) in ('medical_claim', 'pharmacy_claim', 'eligibility')
   and lower(table_schema) = 'tuva_synthetic'
order by table_name, ordinal_position
