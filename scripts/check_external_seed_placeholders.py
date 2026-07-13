#!/usr/bin/env python3
"""Validate that package seed CSVs cannot be skipped as header-only files."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ValidationError:
    path: Path
    message: str


def seed_paths(repo_root: Path) -> list[Path]:
    return sorted((repo_root / "seeds").rglob("*.csv"))


def read_rows(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def validate_seed_files(repo_root: Path) -> list[ValidationError]:
    errors: list[ValidationError] = []

    for path in seed_paths(repo_root):
        rows = read_rows(path)
        relative_path = path.relative_to(repo_root)

        if not rows or not rows[0]:
            errors.append(ValidationError(relative_path, "missing CSV header"))
            continue

        if len(rows) == 1:
            errors.append(
                ValidationError(
                    relative_path,
                    "header-only CSVs are skipped by dbt; add an all-null placeholder row",
                )
            )
            continue

        for row_number, row in enumerate(rows[1:], start=2):
            if not any(row):
                if len(row) != len(rows[0]):
                    errors.append(
                        ValidationError(
                            relative_path,
                            f"all-null row {row_number} has {len(row)} fields; expected {len(rows[0])}",
                        )
                    )
                if len(rows) != 2:
                    errors.append(
                        ValidationError(
                            relative_path,
                            "all-null placeholder rows must be the only data row",
                        )
                    )

    return errors


def placeholder_row(header: list[str]) -> str:
    return '""' if len(header) == 1 else "," * (len(header) - 1)


def add_missing_placeholders(repo_root: Path) -> list[Path]:
    updated_paths: list[Path] = []

    for path in seed_paths(repo_root):
        rows = read_rows(path)
        if not rows or not rows[0]:
            continue

        contents = path.read_bytes()
        newline = b"\n"
        if len(rows) == 1:
            separator = b"" if contents.endswith((b"\n", b"\r")) else newline
            with path.open("ab") as handle:
                handle.write(separator + placeholder_row(rows[0]).encode("utf-8") + newline)
        elif len(rows) == 2 and not rows[1] and len(rows[0]) == 1:
            if not contents.endswith(newline + newline):
                raise ValueError(f"cannot repair malformed placeholder row in {path}")
            path.write_bytes(contents[: -len(newline)] + b'""' + newline)
        elif len(rows) == 2 and not any(rows[1]) and contents.endswith(b"\r\n"):
            path.write_bytes(contents[:-2] + newline)
        else:
            continue
        updated_paths.append(path)

    return updated_paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--fix", action="store_true", help="append placeholders to header-only package seed CSVs")
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    if args.fix:
        for path in add_missing_placeholders(repo_root):
            print(f"added placeholder: {path.relative_to(repo_root)}")

    errors = validate_seed_files(repo_root)
    if errors:
        for error in errors:
            print(f"{error.path}: {error.message}", file=sys.stderr)
        return 1

    print(f"validated {len(seed_paths(repo_root))} package seed CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
