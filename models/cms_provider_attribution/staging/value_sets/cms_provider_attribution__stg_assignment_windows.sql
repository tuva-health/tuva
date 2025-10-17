select
      performance_year
    , service_year
    , assignment_methodology
    , CONVERT(date, expanded_window_start, 101) as expanded_window_start
    , CONVERT(date, window_start, 101) as window_start
    , CONVERT(date, window_end, 101) as window_end
from {{ref('cms_provider_attribution__assignment_windows')}}
