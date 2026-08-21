#!/usr/bin/env python3

import re
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CI_PATH = ROOT / ".github" / "workflows" / "ci.yml"
EXTERNAL_CI_PATH = ROOT / ".github" / "workflows" / "ci-external-pr.yml"
PACKAGES_PATH = ROOT / "integration_tests" / "packages.yml"
RELEASE_PACKAGES_PATH = ROOT / "integration_tests" / "packages.release.yml"
INTEGRATION_PROJECT_PATH = ROOT / "integration_tests" / "dbt_project.yml"
PROFILE_PATHS = {
    warehouse: ROOT / "integration_tests" / "profiles" / warehouse / "profiles.yml"
    for warehouse in (
        "snowflake",
        "bigquery",
        "databricks",
        "fabric",
        "redshift",
    )
}

WAREHOUSES = (
    "snowflake",
    "bigquery",
    "databricks",
    "fabric",
    "redshift",
)

RELEASE_PACKAGES = (
    (
        "ahrq_quality_indicators",
        "ahrq_quality_indicators",
        "TUVA_CI_AHRQ_QUALITY_INDICATORS_SHA",
    ),
    ("ccsr", "ccsr", "TUVA_CI_CCSR_SHA"),
    (
        "cms_chronic_conditions",
        "cms_chronic_conditions",
        "TUVA_CI_CMS_CHRONIC_CONDITIONS_SHA",
    ),
    ("cms_hcc", "cms_hcc", "TUVA_CI_CMS_HCC_SHA"),
    (
        "fhir_preprocessing",
        "fhir_preprocessing",
        "TUVA_CI_FHIR_PREPROCESSING_SHA",
    ),
    (
        "nyu_ed_classification",
        "nyu_ed_classification",
        "TUVA_CI_NYU_ED_CLASSIFICATION_SHA",
    ),
    (
        "quality_measures",
        "quality_measures",
        "TUVA_CI_QUALITY_MEASURES_SHA",
    ),
    ("semantic-layer", "semantic_layer", "TUVA_CI_SEMANTIC_LAYER_SHA"),
)

DEFAULT_SELECTOR = (
    "package:integration_tests",
    "package:the_tuva_project",
)
RELEASE_SELECTOR = DEFAULT_SELECTOR + tuple(
    f"package:{dbt_package}" for _, dbt_package, _ in RELEASE_PACKAGES
)


def extract_javascript_array(text, name, terminator):
    match = re.search(
        rf"const {re.escape(name)} = \[(.*?)\]{terminator}",
        text,
        re.DOTALL,
    )
    if not match:
        raise AssertionError(f"Could not find JavaScript array {name}")
    return tuple(re.findall(r'"([^"]+)"', match.group(1)))


def extract_action_uses(text):
    return re.findall(r"(?m)^\s*uses:\s+([^\s#]+)", text)


class CiContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = CI_PATH.read_text(encoding="utf-8")
        cls.external_workflow = EXTERNAL_CI_PATH.read_text(encoding="utf-8")
        cls.packages = PACKAGES_PATH.read_text(encoding="utf-8")
        cls.release_packages = RELEASE_PACKAGES_PATH.read_text(encoding="utf-8")
        cls.integration_project = INTEGRATION_PROJECT_PATH.read_text(
            encoding="utf-8"
        )
        cls.profiles = {
            warehouse: path.read_text(encoding="utf-8")
            for warehouse, path in PROFILE_PATHS.items()
        }

    def test_primary_ci_exposes_only_the_two_automatic_modes(self):
        event_block = self.workflow.split("\npermissions:", 1)[0]
        self.assertRegex(event_block, r"(?m)^  pull_request:$")
        self.assertRegex(event_block, r"(?m)^  workflow_call:$")
        self.assertNotRegex(event_block, r"(?m)^  workflow_dispatch:$")

        default_selector_match = re.search(
            r'const defaultSelector =\s*"([^"]+)";', self.workflow
        )
        self.assertIsNotNone(default_selector_match)
        self.assertEqual(
            tuple(default_selector_match.group(1).split()), DEFAULT_SELECTOR
        )
        self.assertEqual(
            extract_javascript_array(
                self.workflow, "releaseSelector", r'\.join\(" "\);'
            ),
            RELEASE_SELECTOR,
        )

        self.assertIn('let warehouse = "snowflake";', self.workflow)
        self.assertIn('let mode = "default";', self.workflow)
        self.assertIn("if (baseVersion !== mergeVersion) {", self.workflow)
        self.assertIn('mode = "release";', self.workflow)
        self.assertIn('warehouse = "all";', self.workflow)
        self.assertIn("selector = releaseSelector;", self.workflow)
        self.assertIn(
            'if (isReusableCall && warehouse !== "snowflake")', self.workflow
        )

        # Routine CI installs only the checked-out Core package. Standalone
        # package sources are exclusive to the version-gated release mode.
        self.assertEqual(self.packages.strip(), "packages:\n  - local: ../")
        self.assertIn("checkoutRef = context.sha;", self.workflow)
        self.assertIn("mergeSha = context.sha;", self.workflow)
        self.assertIn('source: "pull_request_test_merge"', self.workflow)

    def test_release_warehouse_and_package_inventories_are_exact(self):
        self.assertEqual(
            extract_javascript_array(self.workflow, "supportedWarehouses", r";"),
            WAREHOUSES,
        )
        self.assertIn(
            'warehouse === "all" ? supportedWarehouses : [warehouse]',
            self.workflow,
        )

        release_package_block = re.search(
            r"const releasePackages = \[(.*?)\];\n\s*const exactSha",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(release_package_block)
        package_definitions = tuple(
            re.findall(
                r"\{\s*repository: \"([^\"]+)\",\s*"
                r"dbtPackage: \"([^\"]+)\",\s*"
                r"envVar: \"([^\"]+)\"\s*\}",
                release_package_block.group(1),
                re.DOTALL,
            )
        )
        self.assertEqual(package_definitions, RELEASE_PACKAGES)

        manifest_sources = tuple(
            re.findall(
                r"(?m)^  - git: https://github\.com/tuva-health/([^/]+)\.git\n"
                r"    revision: \"\{\{ env_var\('([^']+)'\) \}\}\"$",
                self.release_packages,
            )
        )
        expected_manifest_sources = tuple(
            (repository, env_var)
            for repository, _, env_var in RELEASE_PACKAGES
        )
        self.assertEqual(manifest_sources, expected_manifest_sources)
        self.assertEqual(self.release_packages.count("  - local: ../"), 1)
        self.assertNotRegex(self.release_packages, r"(?m)^\s*revision:\s*main\s*$")

    def test_release_sources_are_resolved_once_and_locked_to_exact_commits(self):
        resolver_start = self.workflow.index("const releasePackages = [")
        build_start = self.workflow.index("\n  build:")
        resolver = self.workflow[resolver_start:build_start]
        build = self.workflow[build_start:]

        self.assertIn("await Promise.all(", resolver)
        self.assertIn("releasePackages.map(async (item) =>", resolver)
        self.assertIn("github.rest.repos.getCommit({", resolver)
        self.assertIn("repo: item.repository,", resolver)
        self.assertIn('ref: "main"', resolver)
        self.assertIn("revision: commit.sha.toLowerCase()", resolver)
        self.assertIn("branch: \"main\"", resolver)
        self.assertIn("const exactSha = /^[0-9a-f]{40}$/i;", resolver)
        # The final status job re-resolves the PR merge ref to prevent a stale
        # run from winning. It must not resolve standalone package branches a
        # second time inside any matrix job.
        self.assertNotIn("repo: item.repository,", build)
        self.assertNotIn("releasePackages.map(async (item) =>", build)

        self.assertIn("standalone_packages: standalonePackages", resolver)
        self.assertIn("revision: mergeSha.toLowerCase()", resolver)
        self.assertIn('core.setOutput("source_lock", sourceLock);', resolver)
        self.assertIn(
            "packages = source_lock.get(\"standalone_packages\")", build
        )
        self.assertIn(
            'if not isinstance(packages, list) or len(packages) != 8:', build
        )
        self.assertIn(
            'exact_sha = re.compile(r"^[0-9a-f]{40}$")', build
        )
        self.assertIn('env_path = Path(os.environ["GITHUB_ENV"])', build)
        self.assertIn(
            "integration_dir / \"packages.release.yml\"", build
        )
        self.assertIn(
            'integration_dir / "ci-source-lock.json"', build
        )

        expected_env_vars = {env_var for _, _, env_var in RELEASE_PACKAGES}
        configured_env_vars_match = re.search(
            r"expected_env_vars = \{(.*?)\n\s*\}", build, re.DOTALL
        )
        self.assertIsNotNone(configured_env_vars_match)
        self.assertEqual(
            set(re.findall(r'"(TUVA_CI_[A-Z0-9_]+_SHA)"', configured_env_vars_match.group(1))),
            expected_env_vars,
        )

    def test_both_modes_use_small_synthetic_data_without_parity(self):
        self.assertEqual(
            self.workflow.count('"use_synthetic_data": true'), 1
        )
        self.assertEqual(
            self.workflow.count('"synthetic_data_size": "small"'), 1
        )
        self.assertEqual(self.workflow.count('"parity_enabled": false'), 1)
        self.assertIn("strategy:\n      fail-fast: false", self.workflow)
        self.assertEqual(self.workflow.count("dbt build --full-refresh"), 5)
        self.assertNotIn('"synthetic_data_size": "large"', self.workflow)

    def test_ci_actions_are_immutable_and_release_evidence_is_retained(self):
        action_uses = extract_action_uses(self.workflow) + extract_action_uses(
            self.external_workflow
        )
        local_uses = [use for use in action_uses if use.startswith("./")]
        external_uses = [use for use in action_uses if not use.startswith("./")]

        self.assertEqual(local_uses, ["./.github/workflows/ci.yml"])
        self.assertTrue(external_uses)
        for use in external_uses:
            action, separator, revision = use.rpartition("@")
            self.assertTrue(separator, use)
            self.assertTrue(action, use)
            self.assertRegex(revision, r"^[0-9a-f]{40}$", use)

        expected_actions = {
            "actions/github-script": "3a2844b7e9c422d3c10d287c895573f7108da1b3",
            "actions/checkout": "11d5960a326750d5838078e36cf38b85af677262",
            "actions/setup-python": "a26af69be951a213d495a4c3e4e4022e16d87065",
            "actions/upload-artifact": "ea165f8d65b6e75b540449e92b4886f43607fa02",
        }
        observed = Counter(external_uses)
        self.assertEqual(
            set(observed),
            {f"{action}@{revision}" for action, revision in expected_actions.items()},
        )
        self.assertEqual(
            observed[
                "actions/upload-artifact@"
                "ea165f8d65b6e75b540449e92b4886f43607fa02"
            ],
            1,
        )

        artifact_match = re.search(
            r"- name: Upload release evidence(.*?)retention-days: 90",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(artifact_match)
        artifact_block = artifact_match.group(1)
        self.assertIn("if: always() && env.CI_MODE == 'release'", artifact_block)
        self.assertIn("if-no-files-found: warn", artifact_block)
        artifact_paths = tuple(
            re.findall(r"(?m)^\s+(integration_tests/\S+)$", artifact_block)
        )
        self.assertEqual(
            artifact_paths,
            (
                "integration_tests/ci-evidence.json",
                "integration_tests/package-lock.yml",
                "integration_tests/packages.yml",
                "integration_tests/ci-source-lock.json",
            ),
        )
        for raw_artifact in (
            "integration_tests/target/manifest.json",
            "integration_tests/target/run_results.json",
            "integration_tests/logs/dbt.log",
        ):
            self.assertNotIn(raw_artifact, artifact_block)

        prepare_match = re.search(
            r"- name: Prepare release evidence(.*?)"
            r"\n\s*- name: Upload release evidence",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(prepare_match)
        prepare_block = prepare_match.group(1)
        self.assertIn("target_dir / \"manifest.json\"", prepare_block)
        self.assertIn("target_dir / \"run_results.json\"", prepare_block)
        for result_field in (
            "unique_id",
            "status",
            "execution_time",
            "failures",
        ):
            self.assertIn(
                f'"{result_field}": result.get("{result_field}")',
                prepare_block,
            )
        self.assertIn(
            'for result in run_results.get("results", [])', prepare_block
        )
        self.assertIn(
            'integration_dir / "ci-evidence.json"', prepare_block
        )

    def test_required_status_context_is_stable_across_both_modes(self):
        status_contexts = re.findall(
            r'(?:context:\s*|status\.context === )"([^"]+)"', self.workflow
        )
        self.assertGreaterEqual(len(status_contexts), 3)
        self.assertEqual(set(status_contexts), {"Tuva CI / Required"})
        self.assertNotIn("Tuva CI / Snowflake", self.workflow)
        self.assertIn(
            "[process.env.HEAD_SHA, process.env.MERGE_SHA].map((sha)",
            self.workflow,
        )
        self.assertIn(
            "const statusRefs = [expectedHead, expectedMerge];", self.workflow
        )
        self.assertIn("Full release compatibility matrix passed", self.workflow)
        self.assertIn("Snowflake Core build passed", self.workflow)

    def test_unscoped_package_resources_have_run_specific_schema_overrides(self):
        models = self.integration_project.split("\nmodels:\n", 1)[1].split(
            "\nseeds:\n", 1
        )[0]
        seeds = self.integration_project.split("\nseeds:\n", 1)[1].split(
            "\nflags:\n", 1
        )[0]

        for package_name, suffix in (
            ("ccsr", "ccsr"),
            ("semantic_layer", "semantic_layer"),
        ):
            self.assertRegex(
                models,
                rf"(?ms)^  {package_name}:\n    \+schema: \|\n"
                rf"      .*var\('tuva_schema_prefix'\).*"
                rf"\}}\}}_{suffix}.*else.*{suffix}",
            )
        self.assertRegex(
            seeds,
            r"(?ms)^  ccsr:\n    \+schema: \|\n"
            r"      .*var\('tuva_schema_prefix'\).*\}\}_ccsr.*else.*ccsr",
        )
        self.assertIn(
            '"tuva_schema_prefix": "${{ needs.resolve.outputs.schema_prefix }}"',
            self.workflow,
        )

    def test_every_warehouse_target_appends_the_run_specific_schema_prefix(self):
        self.assertIn(
            "TUVA_CI_SCHEMA_PREFIX: "
            "${{ needs.resolve.outputs.schema_prefix }}",
            self.workflow,
        )
        expected_target_locations = {
            "snowflake": (
                "schema: \"{{ env_var('DBT_SNOWFLAKE_CI_SCHEMA') }}_"
                "{{ env_var('TUVA_CI_SCHEMA_PREFIX') }}\""
            ),
            "bigquery": (
                "dataset: \"connector_"
                "{{ env_var('TUVA_CI_SCHEMA_PREFIX') }}\""
            ),
            "databricks": (
                "schema: \"default_"
                "{{ env_var('TUVA_CI_SCHEMA_PREFIX') }}\""
            ),
            "fabric": (
                "schema: \"{{ env_var('DBT_FABRIC_CI_SCHEMA') }}_"
                "{{ env_var('TUVA_CI_SCHEMA_PREFIX') }}\""
            ),
            "redshift": (
                "schema: \"public_"
                "{{ env_var('TUVA_CI_SCHEMA_PREFIX') }}\""
            ),
        }
        self.assertEqual(set(self.profiles), set(WAREHOUSES))
        for warehouse, target_location in expected_target_locations.items():
            with self.subTest(warehouse=warehouse):
                profile = self.profiles[warehouse]
                self.assertIn(target_location, profile)
                self.assertEqual(profile.count("TUVA_CI_SCHEMA_PREFIX"), 1)

    def test_external_dispatch_rejects_version_changing_pull_requests(self):
        self.assertRegex(
            self.external_workflow, r"(?m)^  workflow_dispatch:$"
        )
        self.assertIn("projectVersion(pr.base.sha)", self.external_workflow)
        self.assertIn(
            "projectVersion(mergeCommit.sha)", self.external_workflow
        )
        rejection = "if (baseVersion !== mergeVersion) {"
        rejection_message = (
            "Version-changing pull requests must use an internal branch so the "
        )
        self.assertIn(rejection, self.external_workflow)
        self.assertIn(rejection_message, self.external_workflow)
        self.assertLess(
            self.external_workflow.index(rejection),
            self.external_workflow.index('core.setOutput("pr_number"'),
        )
        self.assertIn("warehouse: snowflake", self.external_workflow)
        self.assertNotIn("warehouse: all", self.external_workflow)


if __name__ == "__main__":
    unittest.main()
