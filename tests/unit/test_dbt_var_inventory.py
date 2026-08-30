#!/usr/bin/env python3

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROOT_PROJECT_FILE = ROOT / "dbt_project.yml"
PROJECT_FILE = ROOT / "integration_tests" / "dbt_project.yml"
SOURCE_SUFFIXES = {".sql", ".yml", ".yaml"}
TEXT_SUFFIXES = SOURCE_SUFFIXES | {".md"}
EXCLUDED_PARTS = {".git", "dbt_packages", "logs", "target"}


def repository_files(suffixes):
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in suffixes:
            continue
        if EXCLUDED_PARTS.intersection(path.relative_to(ROOT).parts):
            continue
        yield path


def executable_var_names():
    names = set()
    literal_var = re.compile(
        r"(?<![A-Za-z0-9_])var\(\s*['\"]([^'\"]+)['\"]"
    )
    boolean_var = re.compile(
        r"tuva_boolean_var\(\s*['\"]([^'\"]+)['\"]"
    )

    for path in repository_files(SOURCE_SUFFIXES):
        text = path.read_text()
        names.update(literal_var.findall(text))
        names.update(boolean_var.findall(text))
    return names


def documented_core_var_names():
    project_text = PROJECT_FILE.read_text()
    start_marker = "  ## Tuva Core variables\n"
    end_marker = "  ## Standalone package variables\n"
    if start_marker not in project_text or end_marker not in project_text:
        raise AssertionError("Tuva Core variable inventory markers are missing")

    core_block = project_text.split(start_marker, 1)[1].split(end_marker, 1)[0]
    return set(
        re.findall(
            r"^  (?:# )?([A-Za-z_][A-Za-z0-9_]*):",
            core_block,
            re.MULTILINE,
        )
    )


def declared_root_var_names():
    project_text = ROOT_PROJECT_FILE.read_text()
    vars_block = project_text.split("\nvars:\n", 1)[1].split("\nmodel-paths:", 1)[0]
    return set(
        re.findall(
            r"^  ([A-Za-z_][A-Za-z0-9_]*):",
            vars_block,
            re.MULTILINE,
        )
    )


class DbtVarInventoryTest(unittest.TestCase):
    def test_every_executable_core_var_is_documented(self):
        self.assertEqual(documented_core_var_names(), executable_var_names())

    def test_every_root_declaration_has_an_executable_lookup(self):
        self.assertLessEqual(declared_root_var_names(), executable_var_names())

    def test_removed_vars_stay_absent(self):
        removed_vars = (
            "use_" + "synthetic_data",
            "expected_" + "terminology_bucket",
            "expected_" + "core_bucket",
            "expected_" + "package_bucket",
            "tuva_seeds_s3_" + "bucket",
            "tuva_seeds_s3_" + "key_prefix",
        )
        this_file = Path(__file__).resolve()

        for path in repository_files(TEXT_SUFFIXES):
            if path.resolve() == this_file:
                continue
            text = path.read_text()
            for var_name in removed_vars:
                self.assertNotIn(var_name, text, str(path.relative_to(ROOT)))


if __name__ == "__main__":
    unittest.main()
