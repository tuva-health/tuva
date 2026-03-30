## Using Integration Tests

1. Set the project subdirectory to “integration_tests” if using dbt cloud or change directory to "integration_tests" (`cd integration_tests`) if using CLI.
2. Integration tests run against synthetic demo data only.
3. Leave `synthetic_data_size` at `small` for the default test dataset or set it to `large`.
4. Set `tuva_seed_version`, `tuva_seed_versions`, and the appropriate bucket vars when testing published artifacts.
5. Run `dbt deps`.
6. Run `dbt seed`. The local seed CSVs keep headers only; the seed post-hooks load the actual synthetic data from the published release on S3.
7. Run `dbt build`.
