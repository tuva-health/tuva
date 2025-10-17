-- NOTE: This is temporary until this can be integrated into the Tuva project
select
    *

from {{source('input_layer', 'medical_claim')}} med