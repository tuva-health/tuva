{% macro blank_to_null(column) %}
{#
    Treats an empty or whitespace-only string as a missing value.

    An empty string is a real join key: '' = '' is true, so two rows that both
    carry a blank claim_id match each other and the claim line is duplicated --
    silently, on a green run. NULL does not do this, because NULL = NULL is
    unknown and never matches.

    Applied to the grain keys as they enter the normalized layer, which is the
    layer whose job is normalisation. The input layer deliberately keeps the
    blanks so the data quality tests can still see and report them.

    Code and NPI columns are not normalised here. They join Tuva's own
    terminology and provider seeds, and those carry no blank keys, so a blank
    on the claim side already matches nothing.

    Note the trim applies to padded values too, not only blank ones: a key of
    '  123  ' becomes '123'. That is deliberate -- it is the same key -- but it
    is a second behavioural change and belongs in the release notes alongside
    the blank one.

        , {{ blank_to_null('claim_id') }}
#}
nullif(trim({{ column }}), '') as {{ column }}
{% endmacro %}
