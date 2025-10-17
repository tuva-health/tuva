-- Combine all steps
-- TODO: Add to models
select * from {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_1')}}
union all
select * from {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_2')}}
{% if var('performance_year') >= 2025 %}
union all
select * from {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_3')}}
{% endif %}