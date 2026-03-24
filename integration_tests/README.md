## Using Integration Tests

1. Set the project subdirectory to “integration_tests” if using dbt cloud or change directory to "integration_tests" (`cd integration_tests`) if using CLI.
2. Configure synthetic seed loading:
   - Set `synthetic_data_size` to `small` or `large` (`small` is the default)
   - Set `tuva_seed_version` and the appropriate S3 bucket vars when testing published artifacts
3. Run `dbt deps`.
4. Run `dbt seed` or `dbt build` to load the synthetic data into `raw_data`.
5. Run `dbt run` only after the synthetic seed tables have already been loaded.
