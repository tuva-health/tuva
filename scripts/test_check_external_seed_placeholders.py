import importlib.util
import pathlib
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().with_name("check_external_seed_placeholders.py")
SPEC = importlib.util.spec_from_file_location("check_external_seed_placeholders", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ExternalSeedPlaceholderTests(unittest.TestCase):
    def write_seed(self, content: str) -> pathlib.Path:
        repo_root = pathlib.Path(self.temporary_directory.name)
        seed_path = repo_root / "seeds" / "example.csv"
        seed_path.parent.mkdir(parents=True)
        seed_path.write_text(content, encoding="utf-8", newline="")
        return repo_root

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_rejects_header_only_seed(self):
        repo_root = self.write_seed("first,second\n")

        errors = MODULE.validate_seed_files(repo_root)

        self.assertEqual(len(errors), 1)
        self.assertIn("header-only", errors[0].message)

    def test_accepts_single_all_null_placeholder(self):
        repo_root = self.write_seed("first,second\n,\n")

        self.assertEqual(MODULE.validate_seed_files(repo_root), [])

    def test_rejects_wrong_width_placeholder(self):
        repo_root = self.write_seed("first,second\n\n")

        errors = MODULE.validate_seed_files(repo_root)

        self.assertEqual(len(errors), 1)
        self.assertIn("has 0 fields; expected 2", errors[0].message)

    def test_fix_appends_placeholder_without_rewriting_header(self):
        repo_root = self.write_seed("first,second\r\n")

        updated_paths = MODULE.add_missing_placeholders(repo_root)

        self.assertEqual([path.name for path in updated_paths], ["example.csv"])
        self.assertEqual((repo_root / "seeds" / "example.csv").read_bytes(), b"first,second\r\n,\n")

    def test_fix_writes_a_quoted_null_for_single_column_seed(self):
        repo_root = self.write_seed("only_column\n")

        MODULE.add_missing_placeholders(repo_root)

        self.assertEqual((repo_root / "seeds" / "example.csv").read_bytes(), b'only_column\n""\n')


if __name__ == "__main__":
    unittest.main()
