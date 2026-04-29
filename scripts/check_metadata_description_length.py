#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised in runtime environments
    raise SystemExit("PyYAML is required. Install it with `pip install pyyaml`.") from exc


DEFAULT_LIMIT = 1024
DEFAULT_SEARCH_ROOTS = ("models", "seeds", "snapshots")
SUPPORTED_SECTION_KEYS = ("models", "seeds", "snapshots")


@dataclass(frozen=True)
class ColumnDescription:
    file_path: Path
    resource_type: str
    resource_name: str | None
    column_name: str | None
    description: str

    @property
    def length(self) -> int:
        return len(self.description)


@dataclass(frozen=True)
class DescriptionViolation:
    description: ColumnDescription
    limit: int

    @property
    def overflow(self) -> int:
        return self.description.length - self.limit


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail when dbt schema YAML column descriptions exceed a configured character limit."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum allowed description length. Defaults to {DEFAULT_LIMIT}.",
    )
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        default=[],
        help=(
            "Directory to scan. May be provided more than once. "
            f"Defaults to: {', '.join(DEFAULT_SEARCH_ROOTS)}."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root to scan from. Defaults to the current directory.",
    )
    return parser.parse_args(argv)


def iter_schema_files(repo_root: Path, roots: Iterable[str]) -> Iterator[Path]:
    seen: set[Path] = set()
    for root in roots:
        search_root = (repo_root / root).resolve()
        if not search_root.exists():
            continue
        for pattern in ("*.yml", "*.yaml"):
            for path in sorted(search_root.rglob(pattern)):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                yield resolved


def _load_yaml(file_path: Path) -> dict[str, Any]:
    with file_path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        return {}
    return data


def _iter_column_descriptions(
    file_path: Path,
    resource_type: str,
    resources: Any,
) -> Iterator[ColumnDescription]:
    if not isinstance(resources, list):
        return

    for resource in resources:
        if not isinstance(resource, dict):
            continue

        resource_name = resource.get("name")
        if resource_name is not None:
            resource_name = str(resource_name)

        columns = resource.get("columns")
        if not isinstance(columns, list):
            continue

        for column in columns:
            if not isinstance(column, dict):
                continue

            description = column.get("description")
            if description is None:
                continue

            column_name = column.get("name")
            if column_name is not None:
                column_name = str(column_name)

            yield ColumnDescription(
                file_path=file_path,
                resource_type=resource_type,
                resource_name=resource_name,
                column_name=column_name,
                description=str(description),
            )


def scan_schema_file(file_path: Path) -> list[ColumnDescription]:
    document = _load_yaml(file_path)
    descriptions: list[ColumnDescription] = []

    for section_key in SUPPORTED_SECTION_KEYS:
        descriptions.extend(
            _iter_column_descriptions(
                file_path=file_path,
                resource_type=section_key[:-1],
                resources=document.get(section_key),
            )
        )

    return descriptions


def scan_repository(repo_root: Path, roots: Iterable[str]) -> list[ColumnDescription]:
    descriptions: list[ColumnDescription] = []
    for file_path in iter_schema_files(repo_root, roots):
        descriptions.extend(scan_schema_file(file_path))
    return descriptions


def find_violations(descriptions: Iterable[ColumnDescription], limit: int) -> list[DescriptionViolation]:
    return [
        DescriptionViolation(description=description, limit=limit)
        for description in descriptions
        if description.length > limit
    ]


def format_violation(violation: DescriptionViolation, repo_root: Path) -> str:
    description = violation.description
    try:
        display_path = description.file_path.relative_to(repo_root)
    except ValueError:
        display_path = description.file_path

    resource_name = description.resource_name or "<unknown>"
    column_name = description.column_name or "<unknown>"
    return (
        f"{display_path}: "
        f"{description.resource_type}={resource_name} "
        f"column={column_name} "
        f"length={description.length} "
        f"overflow={violation.overflow}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    repo_root = Path(args.repo_root).resolve()
    roots = tuple(args.roots or DEFAULT_SEARCH_ROOTS)

    descriptions = scan_repository(repo_root, roots)
    violations = find_violations(descriptions, args.limit)

    if not violations:
        print(
            f"Checked {len(descriptions)} column descriptions under {', '.join(roots)}. "
            f"No descriptions exceed {args.limit} characters."
        )
        return 0

    print(f"Found {len(violations)} column description(s) longer than {args.limit} characters:")
    for violation in violations:
        print(format_violation(violation, repo_root))

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
