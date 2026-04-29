import importlib.util
import io
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout


MODULE_PATH = pathlib.Path(__file__).resolve().with_name("check_metadata_description_length.py")
SPEC = importlib.util.spec_from_file_location("check_metadata_description_length", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class CheckMetadataDescriptionLengthTests(unittest.TestCase):
    def write_schema(self, root: pathlib.Path, relative_path: str, contents: str) -> pathlib.Path:
        file_path = root / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
        return file_path

    def test_scan_schema_file_detects_literal_and_folded_descriptions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = pathlib.Path(temp_dir)
            schema_path = self.write_schema(
                repo_root,
                "models/example.yml",
                """
                version: 2
                models:
                  - name: example_model
                    columns:
                      - name: long_literal
                        description: |
                          first line
                          second line
                      - name: folded_text
                        description: >-
                          alpha
                          beta
                """,
            )

            descriptions = MODULE.scan_schema_file(schema_path)
            by_name = {description.column_name: description for description in descriptions}

            self.assertEqual(by_name["long_literal"].resource_name, "example_model")
            self.assertEqual(by_name["long_literal"].description, "first line\nsecond line\n")
            self.assertEqual(by_name["folded_text"].description, "alpha beta")

    def test_main_reports_violations_and_honors_limit_flag(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = pathlib.Path(temp_dir)
            self.write_schema(
                repo_root,
                "seeds/example.yml",
                """
                version: 2
                seeds:
                  - name: example_seed
                    columns:
                      - name: short_column
                        description: short text
                      - name: long_column
                        description: >
                          this description is
                          definitely too long
                """,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = MODULE.main(["--repo-root", str(repo_root), "--limit", "12"])

            output = stdout.getvalue()
            self.assertEqual(exit_code, 1)
            self.assertIn("seeds/example.yml", output)
            self.assertIn("seed=example_seed", output)
            self.assertIn("column=long_column", output)
            self.assertIn("overflow=", output)

    def test_script_exits_with_message_when_pyyaml_is_missing(self):
        harness = textwrap.dedent(
            f"""
            import builtins
            import runpy

            original_import = builtins.__import__

            def fake_import(name, *args, **kwargs):
                if name == "yaml":
                    raise ImportError("missing yaml")
                return original_import(name, *args, **kwargs)

            builtins.__import__ = fake_import
            runpy.run_path({str(MODULE_PATH)!r}, run_name="__main__")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", harness],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PyYAML is required", result.stderr)


if __name__ == "__main__":
    unittest.main()
