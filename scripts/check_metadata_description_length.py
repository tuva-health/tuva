#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence


DEFAULT_LIMIT = 1024
DEFAULT_SEARCH_ROOTS = ("models", "seeds", "snapshots")
SUPPORTED_SECTION_KEYS = {"models", "seeds", "snapshots"}


@dataclass(frozen=True)
class ColumnDescription:
    file_path: Path
    resource_type: str
    resource_name: str | None
    column_name: str | None
    description: str
    line_number: int

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


def count_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def strip_inline_comment(value: str) -> str:
    in_single = False
    in_double = False

    for index, char in enumerate(value):
        if char == "'" and not in_double:
            if in_single and index + 1 < len(value) and value[index + 1] == "'":
                continue
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            escaped = index > 0 and value[index - 1] == "\\"
            if not escaped:
                in_double = not in_double
            continue
        if char == "#" and not in_single and not in_double:
            if index == 0 or value[index - 1].isspace():
                return value[:index].rstrip()

    return value.rstrip()


def decode_double_quoted(value: str) -> str:
    escapes = {
        '"': '"',
        "\\": "\\",
        "/": "/",
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
    }

    decoded: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char != "\\" or index + 1 >= len(value):
            decoded.append(char)
            index += 1
            continue

        next_char = value[index + 1]
        decoded.append(escapes.get(next_char, next_char))
        index += 2

    return "".join(decoded)


def parse_inline_scalar(raw_value: str) -> str:
    value = strip_inline_comment(raw_value).strip()
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return decode_double_quoted(value[1:-1])
    return value


def fold_block_lines(lines: Sequence[str]) -> str:
    parts: list[str] = []
    previous_blank = True

    for raw_line in lines:
        if raw_line.strip() == "":
            parts.append("\n")
            previous_blank = True
            continue

        line = raw_line.strip()
        if not parts:
            parts.append(line)
        elif previous_blank:
            parts.append(line)
        else:
            parts.append(f" {line}")
        previous_blank = False

    return "".join(parts)


def parse_block_scalar(lines: Sequence[str], start_index: int, key_indent: int, indicator: str) -> tuple[str, int]:
    block_lines: list[str] = []
    index = start_index + 1

    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        indent = count_indent(line)
        if stripped and indent <= key_indent:
            break
        block_lines.append(line)
        index += 1

    nonblank_indents = [count_indent(line) for line in block_lines if line.strip()]
    block_indent = min(nonblank_indents) if nonblank_indents else key_indent + 1
    normalized_lines = [
        "" if line.strip() == "" else line[block_indent:]
        for line in block_lines
    ]

    style = indicator[0]
    chomp = indicator[1:] if len(indicator) > 1 else ""

    if style == "|":
        value = "\n".join(normalized_lines)
    else:
        value = fold_block_lines(normalized_lines)

    if normalized_lines and chomp != "-":
        value += "\n"

    return value, index


def parse_name_from_list_item(stripped_line: str) -> str | None:
    if not stripped_line.startswith("-"):
        return None

    remainder = stripped_line[1:].strip()
    if not remainder:
        return None
    if not remainder.startswith("name:"):
        return None

    return parse_inline_scalar(remainder.split(":", 1)[1])


def parse_key_value(stripped_line: str) -> tuple[str, str] | None:
    if ":" not in stripped_line or stripped_line.startswith("-"):
        return None
    key, value = stripped_line.split(":", 1)
    return key.strip(), value


def scan_schema_file(file_path: Path) -> list[ColumnDescription]:
    lines = file_path.read_text(encoding="utf-8").splitlines()
    descriptions: list[ColumnDescription] = []

    current_section: str | None = None
    section_indent: int | None = None
    current_resource_name: str | None = None
    resource_item_indent: int | None = None
    columns_indent: int | None = None
    current_column_name: str | None = None
    column_item_indent: int | None = None

    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        indent = count_indent(line)

        if current_column_name is not None and stripped and not stripped.startswith("#") and indent <= column_item_indent:
            current_column_name = None
            column_item_indent = None

        if columns_indent is not None and stripped and not stripped.startswith("#") and indent <= columns_indent:
            columns_indent = None
            current_column_name = None
            column_item_indent = None

        if resource_item_indent is not None and stripped and not stripped.startswith("#") and indent <= resource_item_indent:
            current_resource_name = None
            resource_item_indent = None
            columns_indent = None
            current_column_name = None
            column_item_indent = None

        key_value = parse_key_value(stripped)
        if key_value and key_value[0] in SUPPORTED_SECTION_KEYS and indent == 0:
            current_section = key_value[0]
            section_indent = indent
            current_resource_name = None
            resource_item_indent = None
            columns_indent = None
            current_column_name = None
            column_item_indent = None
            index += 1
            continue

        if current_section is None or section_indent is None:
            index += 1
            continue

        resource_list_indent = section_indent + 2
        if indent == resource_list_indent and stripped.startswith("-"):
            resource_item_indent = indent
            current_resource_name = parse_name_from_list_item(stripped)
            columns_indent = None
            current_column_name = None
            column_item_indent = None
            index += 1
            continue

        if resource_item_indent is not None and indent == resource_item_indent + 2 and key_value:
            key, raw_value = key_value
            if key == "name":
                current_resource_name = parse_inline_scalar(raw_value)
                index += 1
                continue
            if key == "columns":
                columns_indent = indent
                current_column_name = None
                column_item_indent = None
                index += 1
                continue

        if columns_indent is not None and indent == columns_indent + 2 and stripped.startswith("-"):
            column_item_indent = indent
            current_column_name = parse_name_from_list_item(stripped)
            index += 1
            continue

        if column_item_indent is not None and indent == column_item_indent + 2 and key_value:
            key, raw_value = key_value
            if key == "name":
                current_column_name = parse_inline_scalar(raw_value)
                index += 1
                continue
            if key == "description":
                indicator = raw_value.strip()
                if indicator.startswith(("|", ">")):
                    description, next_index = parse_block_scalar(lines, index, indent, indicator)
                else:
                    description = parse_inline_scalar(raw_value)
                    next_index = index + 1

                descriptions.append(
                    ColumnDescription(
                        file_path=file_path,
                        resource_type=current_section[:-1] if current_section.endswith("s") else current_section,
                        resource_name=current_resource_name,
                        column_name=current_column_name,
                        description=description,
                        line_number=index + 1,
                    )
                )
                index = next_index
                continue

        index += 1

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
        f"{display_path}:{description.line_number}: "
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
