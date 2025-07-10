    select npi
        ,case entity_type_code
            when 1 then concat(provider_last_name, ', ', provider_first_name)
            when 2 then provider_organization_name
            end as provider_name
    from {{ ref('tuva_data_assets', 'npi') }}