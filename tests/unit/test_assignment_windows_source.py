#!/usr/bin/env python3

import csv
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE = (
    ROOT
    / "data_assets"
    / "sources"
    / "1.0.0"
    / "cms_provider_attribution__assignment_windows.csv"
)
HEADER = (
    "performance_year",
    "service_year",
    "assignment_methodology",
    "expanded_window_start",
    "window_start",
    "window_end",
)
EXPECTED_2026_ROWS = {
    ("2026", "2026", "retrospective", "1/1/2025", "1/1/2026", "12/31/2026"),
    ("2026", "2025", "retrospective", "1/1/2024", "1/1/2025", "12/31/2025"),
    ("2026", "2024", "retrospective", "1/1/2023", "1/1/2024", "12/31/2024"),
    ("2026", "2023", "retrospective", "1/1/2022", "1/1/2023", "12/31/2023"),
    ("2026", "2026", "prospective", "10/1/2023", "10/1/2024", "9/30/2025"),
    ("2026", "2025", "prospective", "10/1/2022", "10/1/2023", "9/30/2024"),
    ("2026", "2024", "prospective", "10/1/2021", "10/1/2022", "9/30/2023"),
    ("2026", "2023", "prospective", "10/1/2020", "10/1/2021", "9/30/2022"),
}


class AssignmentWindowsSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with SOURCE.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            cls.header = tuple(next(reader))
            cls.rows = [tuple(row) for row in reader]

    def test_schema_and_complete_1_0_0_inventory(self):
        self.assertEqual(self.header, HEADER)
        self.assertEqual(len(self.rows), 24)
        self.assertEqual({row[0] for row in self.rows}, {"2024", "2025", "2026"})
        self.assertTrue(all(len(row) == len(HEADER) and all(row) for row in self.rows))

    def test_natural_keys_are_unique(self):
        keys = {(row[0], row[1], row[2]) for row in self.rows}
        self.assertEqual(len(keys), len(self.rows))

    def test_2026_rows_match_cms_version_14(self):
        actual = {row for row in self.rows if row[0] == "2026"}
        self.assertEqual(actual, EXPECTED_2026_ROWS)


if __name__ == "__main__":
    unittest.main()
