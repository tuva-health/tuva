By default, these tests rely on dbt's native data test runner. 
- test_observation_results_non_numeric_should_fail.sql is a "negative" test that returns rows when non-numeric values bypass filters. 
  You can toggle it with: --vars '{"enable_negative_case_tests": true}' to keep it enabled locally.
Run order:
1) dbt seed --select tests_hcc_suspecting
2) dbt run --select models/tests_hcc_suspecting
3) dbt test --select tag:unit,tag:hcc_suspecting