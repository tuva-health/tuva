with enrollment__member_month as (
    select *
    from {{ ref('enrollment__member_month') }}
)
select
    member_month_sk
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , year_month
    , month_start_date
    , month_end_date

--  , b.payer_attributed_provider
--  , b.payer_attributed_provider_practice
--  , b.payer_attributed_provider_organization
--  , b.payer_attributed_provider_lob
--  , b.custom_attributed_provider
--  , b.custom_attributed_provider_practice
--  , b.custom_attributed_provider_organization
--  , b.custom_attributed_provider_lob

from enrollment__member_month
-- TODO: Add in provider attribution?