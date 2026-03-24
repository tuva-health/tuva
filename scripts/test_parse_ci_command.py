import importlib.util
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().with_name("parse_ci_command.py")
SPEC = importlib.util.spec_from_file_location("parse_ci_command", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ParseCiCommandTests(unittest.TestCase):
    def test_default_comment_runs_all_warehouses(self):
        parsed = MODULE.parse_comment_body("/ci")
        validated = MODULE.validate_dbt_command(parsed.command_tokens)

        self.assertEqual(parsed.targets, MODULE.WAREHOUSES)
        self.assertEqual(validated.command_tokens, ["dbt", "run"])

    def test_explicit_warehouse_list_and_selector(self):
        parsed = MODULE.parse_comment_body("/ci snowflake databricks dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_command(parsed.command_tokens)

        self.assertEqual(parsed.targets, ["snowflake", "databricks"])
        self.assertEqual(
            validated.command_tokens,
            ["dbt", "seed", "--select", "tag:tuva_demo"],
        )
        self.assertTrue(validated.refreshes_seeds)

    def test_legacy_alias_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.parse_comment_body("/ci build-snowflake")

    def test_dispatch_resolution_uses_explicit_command(self):
        parsed = MODULE.resolve_dispatch_inputs(
            dbt_command="dbt build --select tag:tuva_demo",
            targets_csv="bigquery,fabric",
            operation="run",
            target="snowflake",
        )
        validated = MODULE.validate_dbt_command(parsed.command_tokens)

        self.assertEqual(parsed.targets, ["bigquery", "fabric"])
        self.assertEqual(validated.subcommand, "build")
        self.assertTrue(validated.requires_seed_baseline)
        self.assertEqual(
            validated.command_tokens,
            ["dbt", "build", "--select", "tag:tuva_demo", "--exclude", "resource_type:seed"],
        )

    def test_multiple_selector_values_are_allowed(self):
        validated = MODULE.validate_dbt_command(
            ["dbt", "build", "--select", "input_layer__eligibility", "tag:tuva_demo"]
        )

        self.assertEqual(
            validated.command_tokens,
            [
                "dbt",
                "build",
                "--select",
                "input_layer__eligibility",
                "tag:tuva_demo",
                "--exclude",
                "resource_type:seed",
            ],
        )

    def test_invalid_warehouse_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.parse_comment_body("/ci postgres dbt run")

    def test_unsupported_argument_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.validate_dbt_command(["dbt", "run", "--profiles-dir", "./foo"])

    def test_all_warehouse_seed_requires_maintainer(self):
        parsed = MODULE.parse_comment_body("/ci dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_command(parsed.command_tokens)

        with self.assertRaises(MODULE.ValidationError):
            MODULE._authorize_request("COLLABORATOR", parsed, validated)

    def test_single_warehouse_seed_is_allowed_for_collaborator(self):
        parsed = MODULE.parse_comment_body("/ci snowflake dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_command(parsed.command_tokens)

        MODULE._authorize_request("COLLABORATOR", parsed, validated)

    def test_build_full_refresh_still_refreshes_seeds(self):
        validated = MODULE.validate_dbt_command(["dbt", "build", "--full-refresh"])

        self.assertEqual(validated.command_tokens, ["dbt", "build", "--full-refresh"])
        self.assertFalse(validated.requires_seed_baseline)
        self.assertTrue(validated.refreshes_seeds)


if __name__ == "__main__":
    unittest.main()
