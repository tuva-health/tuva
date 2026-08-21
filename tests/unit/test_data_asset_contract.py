#!/usr/bin/env python3

import csv
import json
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FAMILY_FOLDERS = {
    "terminology": "terminology",
    "provider_data": "provider-data",
    "value_sets": "value-sets",
}


def parse_catalog():
    text = (ROOT / "data_assets.yml").read_text()
    schema_version = re.search(r"^schema_version: (\d+)$", text, re.MULTILINE)
    package = re.search(r"^package: (\S+)$", text, re.MULTILINE)
    entries = re.findall(
        r"^  - seed: (\S+)\n    path: (\S+)$",
        text,
        re.MULTILINE,
    )
    return int(schema_version.group(1)), package.group(1), entries


def parse_seed_hooks():
    hooks = {}
    for family in (*FAMILY_FOLDERS, "synthetic_data"):
        for yaml_path in sorted((ROOT / "seeds" / family).rglob("*.yml")):
            seed_name = None
            for line in yaml_path.read_text().splitlines():
                name_match = re.match(r"^  - name: (\S+)$", line)
                if name_match:
                    seed_name = name_match.group(1)
                    continue

                regular_match = re.search(
                    r"load_versioned_seed\('([^']+)','([^']+)'\)", line
                )
                if regular_match:
                    hooks[seed_name] = (
                        regular_match.group(1),
                        regular_match.group(2),
                    )

                synthetic_match = re.search(
                    r"load_versioned_synthetic_seed\('([^']+)'\)", line
                )
                if synthetic_match:
                    hooks[seed_name] = ("synthetic_data", synthetic_match.group(1))
    return hooks


class DataAssetContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema_version, cls.package, cls.entries = parse_catalog()
        cls.hooks = parse_seed_hooks()

    def test_catalog_identity_and_unique_paths(self):
        self.assertEqual(self.schema_version, 1)
        self.assertEqual(self.package, "tuva-core")
        self.assertEqual(len(self.entries), 101)

        paths = [path for _, path in self.entries]
        self.assertEqual(len(paths), len(set(paths)))

    def test_catalog_exactly_matches_seed_hooks(self):
        expected_entries = set()
        self.assertEqual(len(self.hooks), 86)

        for seed_name, (family, object_name) in self.hooks.items():
            if family == "synthetic_data":
                self.assertEqual(object_name, seed_name)
                for size in ("small", "large"):
                    expected_entries.add(
                        (seed_name, f"synthetic-data/{size}/{seed_name}.csv.gz")
                    )
            else:
                self.assertEqual(object_name, f"{seed_name}.csv.gz")
                expected_entries.add(
                    (seed_name, f"{FAMILY_FOLDERS[family]}/{object_name}")
                )

        self.assertEqual(set(self.entries), expected_entries)

    def test_every_seed_has_expected_variant_count_and_header_contract(self):
        entry_counts = Counter(seed for seed, _ in self.entries)
        self.assertEqual(set(entry_counts), set(self.hooks))

        for seed_name, count in entry_counts.items():
            expected_count = 2 if seed_name.startswith("synthetic_data__") else 1
            self.assertEqual(count, expected_count, seed_name)

            header_files = list((ROOT / "seeds").rglob(f"{seed_name}.csv"))
            self.assertEqual(len(header_files), 1, seed_name)
            with header_files[0].open(encoding="utf-8-sig", newline="") as handle:
                rows = csv.reader(handle)
                header = next(rows, [])
                self.assertTrue(header, seed_name)
                self.assertTrue(all(header), seed_name)
                self.assertIsNone(next(rows, None), seed_name)

    def test_package_version_is_the_only_asset_version(self):
        project_text = (ROOT / "dbt_project.yml").read_text()
        obsolete_version_var = "tuva_seed_" + "versions"
        self.assertRegex(project_text, r"(?m)^version: '[1-9][0-9]*\.[0-9]+\.[0-9]+'$")

        searchable_files = [
            ROOT / "dbt_project.yml",
            ROOT / "integration_tests" / "dbt_project.yml",
            ROOT / "integration_tests" / "README.md",
            ROOT / "README.md",
            ROOT / "AGENTS.md",
            ROOT / "macros" / "cross_database_utils" / "versioned_seed_paths.sql",
        ]
        for path in searchable_files:
            self.assertNotIn(obsolete_version_var, path.read_text(), str(path))

    def test_loader_security_and_cross_cloud_contract(self):
        version_macro = (
            ROOT / "macros" / "system_utils" / "get_tuva_package_version.sql"
        ).read_text()
        path_macro = (
            ROOT / "macros" / "cross_database_utils" / "versioned_seed_paths.sql"
        ).read_text()
        load_macro = (
            ROOT / "macros" / "cross_database_utils" / "load_seed.sql"
        ).read_text()

        self.assertIn("macro get_installed_package_version(package_name)", version_macro)
        self.assertIn(
            "macro load_package_seed(package_name, package_slug, object_path",
            path_macro,
        )
        self.assertIn(
            "macro get_versioned_seed_uri(database, version_override=none)",
            path_macro,
        )
        self.assertIn(
            "macro load_versioned_seed(database, seed_object_name, version=none",
            path_macro,
        )
        self.assertIn(
            "macro load_versioned_synthetic_seed(seed_name, version=none",
            path_macro,
        )
        self.assertIn("version overrides were removed in 1.0", path_macro)
        self.assertIn("macro get_seed_bucket(database, package_slug=none)", path_macro)
        self.assertIn("'the_tuva_project',\n      'tuva-core'", path_macro)
        self.assertIn("var('custom_bucket_name', 'tuva-public-resources')", path_macro)
        self.assertIn("var('tuva_seed_buckets', {})", path_macro)

        self.assertIn("iam_role default", load_macro.lower())
        self.assertIn(
            "null as '__tuva_reserved_null_marker_1_0__'",
            load_macro.lower(),
        )
        self.assertIn("COMPRESSION = 'GZIP'", load_macro)
        self.assertIn("ROWTERMINATOR = '0x0A'", load_macro)
        self.assertIn("nullstr = ''", load_macro)
        self.assertNotIn("nullstr = ['',", load_macro)
        self.assertIn("escape_unenclosed_field = NONE", load_macro)
        self.assertIn("null_if = ('__TUVA_RESERVED_NULL_MARKER_1_0__')", load_macro)
        self.assertIn(
            "macro snowflake_seed_rows_loaded(column_names, data, uri, pattern)",
            load_macro,
        )
        self.assertIn(
            "normalized_column_names.index('rows_loaded')",
            load_macro,
        )
        self.assertIn(
            "Snowflake seed load processed no files from s3://",
            load_macro,
        )
        self.assertNotIn("sum(attribute=2)", load_macro)
        self.assertIn("null_markers = ['']", load_macro)
        self.assertNotIn("null_markers = ['', '\\\\N'", load_macro)
        self.assertIn("set null_char = ''", load_macro)
        self.assertIn("'escapeChar' = '\\0'", load_macro)
        self.assertIn("'nullValue' = ''", load_macro)
        self.assertIn('options.append(", header true") if headers == true', load_macro)
        self.assertIn(
            "PostgreSQL COPY CSV already treats an unquoted empty field as NULL",
            load_macro,
        )
        self.assertNotIn("null ''\\\\N''", load_macro)

        readme = (ROOT / "README.md").read_text()
        self.assertIn("Redshift loading uses `IAM_ROLE default`", readme)
        self.assertIn("`Content-Encoding: gzip`", readme)
        for forbidden in (
            "access_key_" + "part",
            "secret_key_" + "part",
            "full_access_" + "key",
            "full_secret_" + "key",
            "access_key_" + "id '{{",
            "secret_access_" + "key",
        ):
            self.assertNotIn(forbidden, load_macro)

    def test_bigquery_header_only_seed_materialization_contract(self):
        macro = (
            ROOT / "integration_tests" / "macros" / "bigquery_seed.sql"
        ).read_text()
        integration_project = (
            ROOT / "integration_tests" / "dbt_project.yml"
        ).read_text()

        self.assertIn(
            "macro bigquery_create_seed_relation(model, agate_table, relation)",
            macro,
        )
        self.assertIn("macro bigquery__create_csv_table(model, agate_table)", macro)
        self.assertIn(
            "macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table)",
            macro,
        )
        self.assertIn("agate_table.rows | length == 0", macro)
        self.assertIn("for column_name in agate_table.column_names", macro)
        self.assertIn("column_types that exactly match its CSV header", macro)
        self.assertIn("api.Column.translate_type(column_type)", macro)
        self.assertIn("create or replace table {{ relation.render() }}", macro)
        self.assertIn("adapter.drop_relation(old_relation)", macro)
        self.assertNotIn("macro bigquery__load_csv_rows", macro)
        self.assertIn("macro_namespace: 'dbt'", integration_project)
        self.assertIn("search_order: ['integration_tests', 'dbt']", integration_project)

    def test_release_requires_all_cloud_receipts_before_tagging(self):
        workflow = (ROOT / ".github" / "workflows" / "create-release.yml").read_text()
        receipt_step = workflow.index("- name: Verify data asset release receipts")
        tag_step = workflow.index("- name: Create Tag")

        self.assertLess(receipt_step, tag_step)
        self.assertIn('receipt_path="tuva-core/${PACKAGE_VERSION}/_release.json"', workflow)
        self.assertIn(
            "https://tuva-public-resources.s3.amazonaws.com/${receipt_path}",
            workflow,
        )
        self.assertIn(
            "https://storage.googleapis.com/tuva-public-resources/${receipt_path}",
            workflow,
        )
        self.assertIn(
            "https://tuvapublicresources.blob.core.windows.net/"
            "tuva-public-resources/${receipt_path}",
            workflow,
        )
        self.assertIn('SUPPORTED_SCHEMA_VERSIONS = {1}', workflow)
        self.assertIn('"package_commit",', workflow)
        self.assertIn('PACKAGE_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")', workflow)
        self.assertIn('package_commit must equal GITHUB_SHA', workflow)
        self.assertIn(
            'FILE_KEYS = {"path", "sha256", "bytes", "rows"}', workflow
        )
        self.assertIn('paths != expected_paths', workflow)
        self.assertIn('receipt_bytes[provider] != baseline', workflow)
        self.assertNotIn("actions/checkout@v", workflow)
        self.assertNotIn("releases/latest", workflow)
        self.assertNotIn("action-gh-release@v", workflow)
        action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s]+)", workflow)
        self.assertTrue(action_refs)
        for action_ref in action_refs:
            self.assertRegex(action_ref, r"^[0-9a-f]{40}$")

    def test_release_recovery_is_main_only_and_uses_current_main_commit(self):
        workflow = (ROOT / ".github" / "workflows" / "create-release.yml").read_text()
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("concurrency:", workflow)
        self.assertIn('git show-ref --verify --quiet "refs/tags/${tag}"', workflow)
        self.assertIn("if: github.ref == 'refs/heads/main'", workflow)
        self.assertIn("git ls-remote origin refs/heads/main", workflow)
        self.assertEqual(
            workflow.count("git ls-remote origin refs/heads/main"), 2
        )
        self.assertIn('test "$actual" = "$GITHUB_SHA"', workflow)
        self.assertIn('test "$actual" = "$main"', workflow)
        self.assertIn(
            'os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"', workflow
        )
        self.assertIn("BEFORE_SHA: ${{ github.event.before }}", workflow)
        self.assertIn('f"{before_sha}:dbt_project.yml"', workflow)
        self.assertNotIn('"HEAD^:dbt_project.yml"', workflow)
        self.assertIn("if not is_recovery and new_version == old_version", workflow)

    def test_release_receipt_validator_accepts_only_strict_identical_receipts(self):
        workflow = (ROOT / ".github" / "workflows" / "create-release.yml").read_text()
        invocation = (
            'python3 - "$receipt_dir" "$PACKAGE_VERSION" "$GITHUB_SHA" '
            "data_assets.yml <<'PY'"
        )
        script_start = workflow.index(invocation) + len(invocation)
        script_end = workflow.index("\n          PY", script_start)
        validator = textwrap.dedent(workflow[script_start:script_end]).lstrip("\n")

        expected_paths = sorted(path for _, path in self.entries)
        valid_package_commit = "b" * 40
        valid_receipt = {
            "schema_version": 1,
            "package": "tuva-core",
            "package_commit": valid_package_commit,
            "version": "1.0.0",
            "files": [
                {
                    "path": path,
                    "sha256": "a" * 64,
                    "bytes": 1,
                    "rows": 1,
                }
                for path in expected_paths
            ],
        }

        def run_validator(receipts, package_commit=valid_package_commit):
            with tempfile.TemporaryDirectory() as temporary_directory:
                receipt_dir = Path(temporary_directory)
                for provider, payload in receipts.items():
                    if isinstance(payload, bytes):
                        encoded = payload
                    else:
                        encoded = json.dumps(
                            payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    (receipt_dir / f"{provider}.json").write_bytes(encoded)
                return subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        validator,
                        str(receipt_dir),
                        "1.0.0",
                        package_commit,
                        str(ROOT / "data_assets.yml"),
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )

        valid = {
            provider: valid_receipt for provider in ("s3", "gcs", "azure")
        }
        result = run_validator(valid)
        self.assertEqual(result.returncode, 0, result.stderr)

        result = run_validator(valid, "B" * 40)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GITHUB_SHA must be 40 lowercase hex", result.stderr)

        def clone_receipt():
            return json.loads(json.dumps(valid_receipt))

        invalid_receipts = []

        invalid_schema = clone_receipt()
        invalid_schema["schema_version"] = 2
        invalid_receipts.append((invalid_schema, "unsupported schema_version"))

        wrong_package = clone_receipt()
        wrong_package["package"] = "quality-measures"
        invalid_receipts.append((wrong_package, "package must be 'tuva-core'"))

        invalid_package_commit = clone_receipt()
        invalid_package_commit["package_commit"] = "B" * 40
        invalid_receipts.append(
            (invalid_package_commit, "package_commit must be 40 lowercase hex")
        )

        wrong_package_commit = clone_receipt()
        wrong_package_commit["package_commit"] = "c" * 40
        invalid_receipts.append(
            (wrong_package_commit, "package_commit must equal GITHUB_SHA")
        )

        wrong_version = clone_receipt()
        wrong_version["version"] = "1.0.1"
        invalid_receipts.append((wrong_version, "version must be '1.0.0'"))

        unexpected_key = clone_receipt()
        unexpected_key["published_at"] = "2026-08-19T00:00:00Z"
        invalid_receipts.append((unexpected_key, "top-level keys must be exactly"))

        empty_files = clone_receipt()
        empty_files["files"] = []
        invalid_receipts.append((empty_files, "files must be a nonempty array"))

        unsorted_files = clone_receipt()
        unsorted_files["files"][0], unsorted_files["files"][1] = (
            unsorted_files["files"][1],
            unsorted_files["files"][0],
        )
        invalid_receipts.append((unsorted_files, "files must be sorted by path"))

        duplicate_path = clone_receipt()
        duplicate_path["files"][1]["path"] = duplicate_path["files"][0]["path"]
        invalid_receipts.append((duplicate_path, "file paths must be unique"))

        missing_path = clone_receipt()
        missing_path["files"].pop()
        invalid_receipts.append((missing_path, "exactly match data_assets.yml"))

        invalid_sha256 = clone_receipt()
        invalid_sha256["files"][0]["sha256"] = "not-a-sha256"
        invalid_receipts.append((invalid_sha256, "64 lowercase hex"))

        zero_bytes = clone_receipt()
        zero_bytes["files"][0]["bytes"] = 0
        invalid_receipts.append((zero_bytes, "bytes must be a positive integer"))

        negative_rows = clone_receipt()
        negative_rows["files"][0]["rows"] = -1
        invalid_receipts.append((negative_rows, "rows must be a positive integer"))

        zero_rows = clone_receipt()
        zero_rows["files"][0]["rows"] = 0
        invalid_receipts.append((zero_rows, "rows must be a positive integer"))

        for invalid_receipt, expected_error in invalid_receipts:
            with self.subTest(expected_error=expected_error):
                result = run_validator(
                    {provider: invalid_receipt for provider in valid}
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)

        canonical_bytes = json.dumps(
            valid_receipt,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        differently_formatted_bytes = json.dumps(
            valid_receipt,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        result = run_validator(
            {
                "s3": canonical_bytes,
                "gcs": canonical_bytes,
                "azure": differently_formatted_bytes,
            }
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not byte-identical", result.stderr)


if __name__ == "__main__":
    unittest.main()
