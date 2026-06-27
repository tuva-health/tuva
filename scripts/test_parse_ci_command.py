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
    def test_default_comment_runs_snowflake_core_seed_then_run(self):
        parsed = MODULE.parse_comment_body("/ci")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [
                [
                    "dbt",
                    "seed",
                    "--full-refresh",
                    "--select",
                    "package:integration_tests",
                    "package:the_tuva_project",
                ],
                ["dbt", "run", "--select", "package:integration_tests", "package:the_tuva_project"],
            ],
        )
        self.assertTrue(validated.refreshes_seeds)

    def test_run_alias_runs_core_seed_then_run_on_active_warehouses(self):
        parsed = MODULE.parse_comment_body("/ci run")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, MODULE.ACTIVE_WAREHOUSES)
        self.assertEqual(len(validated.commands), 2)
        self.assertEqual(validated.commands[0].subcommand, "seed")
        self.assertEqual(validated.commands[1].subcommand, "run")

    def test_run_single_warehouse_alias_runs_core_seed_then_run(self):
        parsed = MODULE.parse_comment_body("/ci run-snowflake")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(len(validated.commands), 2)
        self.assertEqual(validated.commands[0].subcommand, "seed")
        self.assertEqual(validated.commands[1].subcommand, "run")

    def test_run_alias_with_flags_preserves_single_run_command(self):
        parsed = MODULE.parse_comment_body("/ci run-snowflake --select tag:tuva_demo")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [["dbt", "run", "--select", "tag:tuva_demo"]],
        )

    def test_build_alias_runs_core_build_on_active_warehouses(self):
        parsed = MODULE.parse_comment_body("/ci build")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, MODULE.ACTIVE_WAREHOUSES)
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [
                [
                    "dbt",
                    "build",
                    "--full-refresh",
                    "--select",
                    "package:integration_tests",
                    "package:the_tuva_project",
                ]
            ],
        )
        self.assertTrue(validated.refreshes_seeds)

    def test_build_single_warehouse_alias_is_supported(self):
        parsed = MODULE.parse_comment_body("/ci build-fabric")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["fabric"])
        self.assertEqual(validated.commands[0].command_tokens[0:3], ["dbt", "build", "--full-refresh"])

    def test_marts_alias_runs_all_packages_on_snowflake(self):
        parsed = MODULE.parse_comment_body("/ci marts")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [["dbt", "seed", "--full-refresh"], ["dbt", "run"]],
        )

    def test_explicit_warehouse_list_and_selector(self):
        parsed = MODULE.parse_comment_body("/ci snowflake databricks dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake", "databricks"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [["dbt", "seed", "--select", "tag:tuva_demo"]],
        )
        self.assertTrue(validated.refreshes_seeds)

    def test_sequence_parse_preserves_order(self):
        parsed = MODULE.parse_comment_body("/ci snowflake fabric dbt seed dbt run")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake", "fabric"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [["dbt", "seed"], ["dbt", "run"]],
        )
        self.assertTrue(validated.refreshes_seeds)

    def test_sequence_parse_preserves_step_flags(self):
        parsed = MODULE.parse_comment_body(
            "/ci snowflake fabric dbt seed --select tag:tuva_demo dbt run --select tag:tuva_demo"
        )
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [
                ["dbt", "seed", "--select", "tag:tuva_demo"],
                ["dbt", "run", "--select", "tag:tuva_demo"],
            ],
        )

    def test_trailing_dbt_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.validate_dbt_sequence(MODULE.parse_comment_body("/ci snowflake dbt seed dbt").command_sequences)

    def test_dispatch_resolution_uses_explicit_sequence(self):
        parsed = MODULE.resolve_dispatch_inputs(
            dbt_command="dbt seed --select tag:tuva_demo dbt run --select tag:tuva_demo",
            targets_csv="bigquery,fabric",
            operation="run",
            target="snowflake",
        )
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["bigquery", "fabric"])
        self.assertEqual(
            [command.command_tokens for command in validated.commands],
            [
                ["dbt", "seed", "--select", "tag:tuva_demo"],
                ["dbt", "run", "--select", "tag:tuva_demo"],
            ],
        )

    def test_dispatch_resolution_defaults_to_core_seed_run(self):
        parsed = MODULE.resolve_dispatch_inputs(
            dbt_command="",
            targets_csv="",
            operation="run",
            target="snowflake",
        )
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(len(validated.commands), 2)
        self.assertEqual(validated.commands[0].subcommand, "seed")
        self.assertEqual(validated.commands[1].subcommand, "run")

    def test_dispatch_resolution_uses_core_build_for_build_operation(self):
        parsed = MODULE.resolve_dispatch_inputs(
            dbt_command="",
            targets_csv="",
            operation="build",
            target="snowflake",
        )
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        self.assertEqual(parsed.targets, ["snowflake"])
        self.assertEqual(
            validated.commands[0].command_tokens,
            [
                "dbt",
                "build",
                "--full-refresh",
                "--select",
                "package:integration_tests",
                "package:the_tuva_project",
            ],
        )

    def test_dispatch_all_excludes_redshift(self):
        parsed = MODULE.resolve_dispatch_inputs(
            dbt_command="",
            targets_csv="",
            operation="build",
            target="all",
        )

        self.assertEqual(parsed.targets, MODULE.ACTIVE_WAREHOUSES)
        self.assertNotIn("redshift", parsed.targets)

    def test_redshift_is_still_available_explicitly(self):
        parsed = MODULE.parse_comment_body("/ci redshift dbt seed dbt run")

        self.assertEqual(parsed.targets, ["redshift"])

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
            ],
        )

    def test_invalid_warehouse_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.parse_comment_body("/ci postgres dbt run")

    def test_unsupported_argument_is_rejected(self):
        with self.assertRaises(MODULE.ValidationError):
            MODULE.validate_dbt_sequence([["dbt", "run", "--profiles-dir", "./foo"]])

    def test_all_warehouse_seed_requires_maintainer(self):
        parsed = MODULE.parse_comment_body("/ci dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        with self.assertRaises(MODULE.ValidationError):
            MODULE._authorize_request("COLLABORATOR", parsed, validated)

    def test_all_warehouse_seed_run_sequence_requires_maintainer(self):
        parsed = MODULE.parse_comment_body("/ci dbt seed dbt run")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        with self.assertRaises(MODULE.ValidationError):
            MODULE._authorize_request("COLLABORATOR", parsed, validated)

    def test_single_warehouse_seed_is_allowed_for_collaborator(self):
        parsed = MODULE.parse_comment_body("/ci snowflake dbt seed --select tag:tuva_demo")
        validated = MODULE.validate_dbt_sequence(parsed.command_sequences)

        MODULE._authorize_request("COLLABORATOR", parsed, validated)


if __name__ == "__main__":
    unittest.main()
