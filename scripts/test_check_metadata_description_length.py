import os
import pathlib
import shutil
import subprocess
import tempfile
import textwrap
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().with_name("check_metadata_description_length.sh")


class CheckMetadataDescriptionLengthTests(unittest.TestCase):
    def setUp(self):
        self.yq_bin = os.environ.get("YQ_BIN") or shutil.which("yq")
        self.jq_bin = os.environ.get("JQ_BIN", "jq")

        if not self.yq_bin:
            self.skipTest("yq is not available")

    def write_schema(self, root: pathlib.Path, relative_path: str, contents: str) -> None:
        file_path = root / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")

    def run_script(self, repo_root: pathlib.Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["YQ_BIN"] = self.yq_bin
        env["JQ_BIN"] = self.jq_bin
        return subprocess.run(
            [str(SCRIPT_PATH), "--repo-root", str(repo_root), *extra_args],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

    def test_passes_when_descriptions_are_within_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = pathlib.Path(temp_dir)
            self.write_schema(
                repo_root,
                "models/example.yml",
                """
                version: 2
                models:
                  - name: example_model
                    columns:
                      - name: member_id
                        description: short text
                """,
            )

            result = self.run_script(repo_root, "--limit", "20")

            self.assertEqual(result.returncode, 0)
            self.assertIn("No descriptions exceed 20 characters", result.stdout)

    def test_reports_overflowing_descriptions(self):
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

            result = self.run_script(repo_root, "--limit", "12")

            self.assertEqual(result.returncode, 1)
            self.assertIn("seeds/example.yml", result.stdout)
            self.assertIn("seed=example_seed", result.stdout)
            self.assertIn("column=long_column", result.stdout)
            self.assertIn("overflow=", result.stdout)


if __name__ == "__main__":
    unittest.main()
