Testing framework: dbt singular SQL tests.
- We validate model output by comparing ACTUAL rows against EXPECTED fixtures using EXCEPT in both directions.
- New scenarios should be added to tests/hcc_suspecting/test_observation_suspects_extended.sql by appending to the VALUES block in the expected CTE.
- The ACTUAL side is sourced via a dispatchable macro so projects can redirect to the canonical model or reuse the basic CTE chain without duplication.