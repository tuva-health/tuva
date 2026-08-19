#!/usr/bin/env python3

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DuckDbLocalAssetRootTest(unittest.TestCase):
    def test_local_staging_root_is_explicit_and_s3_remains_the_default(self):
        macro = (
            ROOT / "macros" / "cross_database_utils" / "load_seed.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("var('tuva_seed_duckdb_storage_root', '')", macro)
        self.assertIn("set seed_path = 's3://' ~ uri", macro)
        self.assertIn("set seed_path = local_storage_root ~ root_separator ~ uri", macro)
        self.assertIn("read_csv('{{ seed_path", macro)


if __name__ == "__main__":
    unittest.main()
