## Using Integration Tests

#### In CLI:
- Open project in parent folder
- Change terminal context to integration_tests folder
- Make sure any parent package refs are sources and models in integration_tests
- dbt deps before building and/or running

#### In Cloud:
- In account settings > projects > project, set **project subdirectory** to `integration_tests`
- Make sure any parent package refs are sources and models in integration_tests
- dbt deps before building and/or running
- If it's not working, try switching to classic ide and back 
