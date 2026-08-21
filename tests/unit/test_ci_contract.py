#!/usr/bin/env python3

import re
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CI_PATH = ROOT / ".github" / "workflows" / "ci.yml"
ALL_WAREHOUSES_CI_PATH = (
    ROOT / ".github" / "workflows" / "ci-all-warehouses.yml"
)
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

CORE_SELECTOR = (
    "package:integration_tests",
    "package:the_tuva_project",
)
ALL_PACKAGES_SELECTOR = CORE_SELECTOR + tuple(
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
        cls.all_warehouses_workflow = ALL_WAREHOUSES_CI_PATH.read_text(
            encoding="utf-8"
        )
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

    def test_public_ci_surface_is_two_simple_workflows(self):
        self.assertTrue(self.workflow.startswith("name: Tuva CI -- Snowflake\n"))
        primary_events = self.workflow.split("\npermissions:", 1)[0]
        self.assertRegex(primary_events, r"(?m)^  pull_request:$")
        self.assertRegex(primary_events, r"(?m)^  workflow_call:$")
        self.assertNotRegex(primary_events, r"(?m)^  workflow_dispatch:$")

        self.assertTrue(
            self.all_warehouses_workflow.startswith(
                "name: Tuva CI -- All Warehouses\n"
            )
        )
        all_events = self.all_warehouses_workflow.split("\npermissions:", 1)[0]
        self.assertRegex(all_events, r"(?m)^  workflow_dispatch:$")
        self.assertEqual(
            re.findall(r"(?m)^      ([a-z_]+):$", all_events),
            ["pr_number"],
        )
        self.assertNotIn("warehouse:", all_events)
        self.assertNotIn("selector:", all_events)
        self.assertNotIn("command:", all_events)

    def test_automatic_pull_requests_are_always_core_on_snowflake(self):
        core_selector_match = re.search(
            r'const coreSelector =\s*"([^"]+)";', self.workflow
        )
        self.assertIsNotNone(core_selector_match)
        self.assertEqual(
            tuple(core_selector_match.group(1).split()), CORE_SELECTOR
        )
        self.assertEqual(
            extract_javascript_array(
                self.workflow, "allPackagesSelector", r'\.join\(" "\);'
            ),
            ALL_PACKAGES_SELECTOR,
        )

        pull_request_branch = self.workflow[
            self.workflow.index(
                '} else if (context.eventName === "pull_request") {'
            ) : self.workflow.index("const supportedRequests = new Set")
        ]
        self.assertIn('warehouse = "snowflake";', pull_request_branch)
        self.assertIn('scope = "core";', pull_request_branch)
        self.assertIn("checkoutRef = context.sha;", pull_request_branch)
        self.assertIn("mergeSha = context.sha;", pull_request_branch)
        self.assertNotIn("projectVersion", pull_request_branch)
        self.assertNotIn("all-packages", pull_request_branch)

        supported_request_match = re.search(
            r"const supportedRequests = new Set\(\[(.*?)\]\);",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(supported_request_match)
        self.assertEqual(
            set(re.findall(r'"([^"]+)"', supported_request_match.group(1))),
            {"snowflake:core", "all:all-packages"},
        )
        self.assertEqual(self.packages.strip(), "packages:\n  - local: ../")

    def test_manual_release_dispatch_validates_an_internal_version_pr(self):
        resolver = self.all_warehouses_workflow
        self.assertIn('context.ref !== "refs/heads/main"', resolver)
        self.assertIn('pr.state !== "open"', resolver)
        self.assertIn('pr.base.ref !== "main"', resolver)
        self.assertIn("headRepository !== baseRepository", resolver)
        self.assertIn("pr.mergeable !== true", resolver)
        self.assertIn('ref: `refs/pull/${prNumber}/merge`', resolver)
        self.assertIn("parentShas[0] !== pr.base.sha", resolver)
        self.assertIn("parentShas[1] !== pr.head.sha", resolver)
        self.assertIn("projectVersion(pr.base.sha)", resolver)
        self.assertIn("projectVersion(mergeCommit.sha)", resolver)
        self.assertIn("function parseSemver(version)", resolver)
        self.assertIn("function compareSemverPrecedence(", resolver)
        self.assertIn(
            "compareSemverPrecedence(releaseVersion, baseVersion) <= 0",
            resolver,
        )

        self.assertGreaterEqual(resolver.count("await getPullRequest()"), 2)
        self.assertGreaterEqual(resolver.count("await getTestMerge("), 2)
        self.assertIn("currentMerge.sha !== mergeCommit.sha", resolver)

        self.assertIn("warehouse: all", resolver)
        self.assertIn("scope: all-packages", resolver)
        self.assertIn(
            "source_lock: ${{ needs.resolve.outputs.source_lock }}", resolver
        )
        self.assertIn("publish_status: true", resolver)

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
            r"const releasePackages = \[(.*?)\];\n\s*async function getPullRequest",
            self.all_warehouses_workflow,
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

    def test_release_sources_are_resolved_once_and_shared_by_every_job(self):
        resolver = self.all_warehouses_workflow
        build = self.workflow[self.workflow.index("\n  build:") :]

        self.assertIn("releasePackages.map(async (item) =>", resolver)
        self.assertIn("github.rest.repos.getCommit({", resolver)
        self.assertIn("repo: item.repository,", resolver)
        self.assertIn('ref: "main"', resolver)
        self.assertIn("revision: commit.sha.toLowerCase()", resolver)
        self.assertIn('branch: "main"', resolver)
        self.assertIn("standalone_packages: standalonePackages", resolver)
        self.assertIn("revision: mergeCommit.sha.toLowerCase()", resolver)
        self.assertIn(
            'core.setOutput("source_lock", JSON.stringify(sourceLock));', resolver
        )

        self.assertNotIn("repo: item.repository,", build)
        self.assertNotIn("releasePackages.map(async (item) =>", build)
        self.assertIn(
            'packages = source_lock.get("standalone_packages")', build
        )
        self.assertIn(
            'if not isinstance(packages, list) or len(packages) != 8:', build
        )
        self.assertIn('exact_sha = re.compile(r"^[0-9a-f]{40}$")', build)
        self.assertIn('env_path = Path(os.environ["GITHUB_ENV"])', build)
        self.assertIn('integration_dir / "packages.release.yml"', build)
        self.assertIn('integration_dir / "ci-source-lock.json"', build)

        self.assertIn("const expectedStandalonePackages = [", self.workflow)
        self.assertIn("expected_packages = {", build)
        self.assertIn("actual_packages = {", build)
        for repository, dbt_package, env_var in RELEASE_PACKAGES:
            with self.subTest(repository=repository):
                self.assertIn(f"/{repository}`", self.workflow)
                self.assertIn(f'"{dbt_package}"', self.workflow)
                self.assertIn(f'"{env_var}"', self.workflow)
                self.assertIn(f'/{repository}"', build)
                self.assertIn(f'"{dbt_package}"', build)
                self.assertIn(f'"{env_var}"', build)

    def test_release_preflight_requires_payloads_but_no_receipt(self):
        preflight = self.workflow[
            self.workflow.index("\n  preflight_assets:") : self.workflow.index(
                "\n  build:"
            )
        ]
        self.assertIn("needs.resolve.outputs.scope == 'all-packages'", preflight)
        self.assertIn(
            'source_lock.get("release_version") != package_version', preflight
        )
        self.assertIn('Path("data_assets.yml")', preflight)
        self.assertIn('r"^    path: (\\S+)$"', preflight)
        self.assertIn("declared_path_keys != len(asset_paths)", preflight)
        self.assertIn("declared_seed_keys != len(canonical_seeds)", preflight)
        self.assertIn("len(canonical_seeds) != len(asset_paths)", preflight)
        self.assertIn(
            "release preflight refuses to skip unrecognized YAML declarations",
            preflight,
        )
        for provider_url in (
            "https://tuva-public-resources.s3.amazonaws.com",
            "https://storage.googleapis.com/tuva-public-resources",
            "https://tuvapublicresources.blob.core.windows.net/",
        ):
            self.assertIn(provider_url, preflight)
        self.assertIn("/tuva-core/{version_component}/", preflight)
        self.assertIn("/_release.json", preflight)
        self.assertIn("if status != 404:", preflight)
        self.assertIn("- preflight_assets", self.workflow)
        self.assertIn(
            "needs.preflight_assets.result == 'success' || "
            "needs.preflight_assets.result == 'skipped'",
            self.workflow,
        )

    def test_both_paths_use_small_synthetic_data_without_parity(self):
        self.assertEqual(self.workflow.count('"use_synthetic_data": true'), 1)
        self.assertEqual(self.workflow.count('"synthetic_data_size": "small"'), 1)
        self.assertEqual(self.workflow.count('"parity_enabled": false'), 1)
        self.assertIn("strategy:\n      fail-fast: false", self.workflow)
        self.assertEqual(self.workflow.count("dbt build --full-refresh"), 5)
        self.assertNotIn('"synthetic_data_size": "large"', self.workflow)

    def test_ci_actions_are_immutable_and_release_evidence_is_retained(self):
        workflows = (
            self.workflow,
            self.all_warehouses_workflow,
            self.external_workflow,
        )
        action_uses = sum((extract_action_uses(text) for text in workflows), [])
        local_uses = [use for use in action_uses if use.startswith("./")]
        external_uses = [use for use in action_uses if not use.startswith("./")]

        self.assertEqual(
            local_uses,
            ["./.github/workflows/ci.yml", "./.github/workflows/ci.yml"],
        )
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
            r"- name: Upload all-warehouse evidence(.*?)retention-days: 90",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(artifact_match)
        artifact_block = artifact_match.group(1)
        self.assertIn(
            "if: always() && env.CI_SCOPE == 'all-packages'", artifact_block
        )
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
            r"- name: Prepare all-warehouse evidence(.*?)"
            r"\n\s*- name: Upload all-warehouse evidence",
            self.workflow,
            re.DOTALL,
        )
        self.assertIsNotNone(prepare_match)
        prepare_block = prepare_match.group(1)
        self.assertIn(
            '"github_run_attempt": os.environ["GITHUB_RUN_ATTEMPT"]',
            prepare_block,
        )
        self.assertIn('target_dir / "manifest.json"', prepare_block)
        self.assertIn('target_dir / "run_results.json"', prepare_block)
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

    def test_status_contexts_are_distinct_and_stale_safe(self):
        self.assertIn('"Tuva CI / Snowflake"', self.workflow)
        self.assertIn('"Tuva CI / All Warehouses"', self.workflow)
        self.assertNotIn("Tuva CI / Required", self.workflow)
        self.assertIn(
            "const statusRefs = [process.env.HEAD_SHA, process.env.MERGE_SHA]",
            self.workflow,
        )
        self.assertIn(
            "const statusRefs = [expectedHead, expectedMerge];", self.workflow
        )
        self.assertIn("status.context === statusContext", self.workflow)
        self.assertIn("currentMerge.sha === expectedMerge", self.workflow)
        self.assertIn(
            "changed before CI could publish pending status", self.workflow
        )
        self.assertIn("newerRunOwnsStatus", self.workflow)
        self.assertIn(
            "JSON.parse(sourceLock).standalone_packages", self.workflow
        )
        self.assertIn(
            'ref: "main"',
            self.workflow[self.workflow.index("\n  finalize_status:") :],
        )
        self.assertIn(
            "A standalone package main changed; rerun required", self.workflow
        )
        self.assertIn("All-warehouse release CI passed", self.workflow)
        self.assertIn("Snowflake Core build passed", self.workflow)

    def test_every_run_gets_isolated_cross_package_schemas(self):
        self.assertIn("${schemaBase}_r${runId}_a${runAttempt}", self.workflow)
        self.assertIn(
            "TUVA_CI_SCHEMA_PREFIX: "
            "${{ needs.resolve.outputs.schema_prefix }}",
            self.workflow,
        )

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

    def test_external_dispatch_is_core_snowflake_and_rejects_version_changes(self):
        self.assertRegex(self.external_workflow, r"(?m)^  workflow_dispatch:$")
        self.assertIn("projectVersion(pr.base.sha)", self.external_workflow)
        self.assertIn("projectVersion(mergeCommit.sha)", self.external_workflow)
        rejection = "if (baseVersion !== mergeVersion) {"
        self.assertIn(rejection, self.external_workflow)
        self.assertIn(
            "Version-changing pull requests must use an internal branch",
            self.external_workflow,
        )
        self.assertLess(
            self.external_workflow.index(rejection),
            self.external_workflow.index('core.setOutput("pr_number"'),
        )
        self.assertIn("warehouse: snowflake", self.external_workflow)
        self.assertIn("scope: core", self.external_workflow)
        self.assertIn('source_lock: ""', self.external_workflow)
        self.assertNotIn("warehouse: all", self.external_workflow)


if __name__ == "__main__":
    unittest.main()
