#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shlex
import sys
import uuid
from dataclasses import dataclass


WAREHOUSES = ["snowflake", "bigquery", "databricks", "fabric", "redshift", "duckdb", "clickhouse"]
WAREHOUSE_SET = set(WAREHOUSES)
COLLABORATOR_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}
MAINTAINER_ASSOCIATIONS = {"OWNER", "MEMBER"}
VALID_SUBCOMMANDS = {"build", "compile", "debug", "run", "seed", "test"}
MULTI_VALUE_FLAGS = {"--exclude", "--select", "-s"}
SINGLE_VALUE_FLAGS = {"--selector", "--vars"}
BOOLEAN_FLAGS = {"--empty", "--fail-fast", "--full-refresh"}


class ValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedRequest:
    command_tokens: list[str]
    targets: list[str]
    source: str

    @property
    def all_targets(self) -> bool:
        return self.targets == WAREHOUSES

    @property
    def targets_csv(self) -> str:
        return "all" if self.all_targets else ",".join(self.targets)

    @property
    def targets_display(self) -> str:
        return "all" if self.all_targets else ", ".join(self.targets)


@dataclass(frozen=True)
class ValidatedCommand:
    command_tokens: list[str]
    subcommand: str
    requires_seed_baseline: bool
    refreshes_seeds: bool

    @property
    def command_display(self) -> str:
        return shlex.join(self.command_tokens)


def _write_outputs(values: dict[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as handle:
            for key, value in values.items():
                delimiter = f"EOF_{uuid.uuid4().hex}"
                handle.write(f"{key}<<{delimiter}\n{value}\n{delimiter}\n")
    else:
        print(json.dumps(values, indent=2, sort_keys=True))


def _trimmed_env(name: str) -> str:
    return os.environ.get(name, "").strip()


def _split_shell_words(raw: str) -> list[str]:
    try:
        return shlex.split(raw)
    except ValueError as exc:
        raise ValidationError(f"Unable to parse command: {exc}") from exc


def _normalize_targets(tokens: list[str]) -> list[str]:
    if not tokens:
        return WAREHOUSES.copy()

    lowered = [token.lower() for token in tokens]
    if "all" in lowered and len(lowered) > 1:
        raise ValidationError("Do not combine `all` with specific warehouses in a `/ci` command.")
    if lowered == ["all"]:
        return WAREHOUSES.copy()

    normalized: list[str] = []
    for token in lowered:
        if token not in WAREHOUSE_SET:
            supported = ", ".join(WAREHOUSES)
            raise ValidationError(f"Unsupported warehouse `{token}`. Supported warehouses: {supported}.")
        if token not in normalized:
            normalized.append(token)
    return normalized


def _normalize_targets_csv(raw: str) -> list[str]:
    trimmed = raw.strip().lower()
    if not trimmed or trimmed == "all":
        return WAREHOUSES.copy()
    return _normalize_targets([part.strip() for part in trimmed.split(",") if part.strip()])


def validate_dbt_command(command_tokens: list[str]) -> ValidatedCommand:
    if not command_tokens:
        raise ValidationError("Missing dbt command. Example: `dbt run --select tag:tuva_demo`.")
    if command_tokens[0].lower() != "dbt":
        raise ValidationError("CI commands must start with `dbt`.")
    if len(command_tokens) < 2:
        raise ValidationError("Missing dbt subcommand. Example: `dbt run`.")

    subcommand = command_tokens[1].lower()
    if subcommand not in VALID_SUBCOMMANDS:
        supported = ", ".join(sorted(VALID_SUBCOMMANDS))
        raise ValidationError(f"Unsupported dbt subcommand `{subcommand}`. Supported subcommands: {supported}.")

    normalized = ["dbt", subcommand]
    idx = 2
    while idx < len(command_tokens):
        token = command_tokens[idx]
        lowered = token.lower()

        if lowered in BOOLEAN_FLAGS:
            normalized.append(lowered)
            idx += 1
            continue

        value_flag = next((flag for flag in MULTI_VALUE_FLAGS if lowered == flag), None)
        if value_flag is not None:
            value_idx = idx + 1
            values: list[str] = []
            while value_idx < len(command_tokens) and not command_tokens[value_idx].startswith("-"):
                values.append(command_tokens[value_idx])
                value_idx += 1
            if not values:
                raise ValidationError(f"Flag `{token}` requires a value.")
            normalized.append(value_flag)
            normalized.extend(values)
            idx = value_idx
            continue

        value_flag = next((flag for flag in SINGLE_VALUE_FLAGS if lowered == flag), None)
        if value_flag is not None:
            if idx + 1 >= len(command_tokens):
                raise ValidationError(f"Flag `{token}` requires a value.")
            normalized.extend([value_flag, command_tokens[idx + 1]])
            idx += 2
            continue

        value_flag = next((flag for flag in MULTI_VALUE_FLAGS | SINGLE_VALUE_FLAGS if lowered.startswith(flag + "=")), None)
        if value_flag is not None:
            normalized.append(lowered if lowered.startswith("-s=") else token)
            idx += 1
            continue

        raise ValidationError(
            "Unsupported dbt argument "
            f"`{token}`. Allowed flags: {', '.join(sorted(BOOLEAN_FLAGS | MULTI_VALUE_FLAGS | SINGLE_VALUE_FLAGS))}."
        )

    return ValidatedCommand(
        command_tokens=normalized,
        subcommand=subcommand,
        requires_seed_baseline=subcommand in {"run", "test"},
        refreshes_seeds=subcommand in {"build", "seed"},
    )


def parse_comment_body(body: str) -> ParsedRequest:
    stripped = body.strip()
    if not stripped.startswith("/ci"):
        raise ValidationError("CI commands must start with `/ci`.")

    tokens = _split_shell_words(stripped)
    if not tokens or tokens[0] != "/ci":
        raise ValidationError("CI commands must start with `/ci`.")

    remainder = tokens[1:]
    if not remainder:
        return ParsedRequest(command_tokens=["dbt", "run"], targets=WAREHOUSES.copy(), source="default")

    if "dbt" in remainder:
        dbt_index = remainder.index("dbt")
        target_tokens = remainder[:dbt_index]
        command_tokens = remainder[dbt_index:]
        source = "explicit"
    else:
        target_tokens = remainder
        command_tokens = ["dbt", "run"]
        source = "default"

    return ParsedRequest(
        command_tokens=command_tokens,
        targets=_normalize_targets(target_tokens),
        source=source,
    )


def resolve_dispatch_inputs(
    dbt_command: str,
    targets_csv: str,
    operation: str,
    target: str,
) -> ParsedRequest:
    if dbt_command.strip():
        command_tokens = _split_shell_words(dbt_command)
        return ParsedRequest(
            command_tokens=command_tokens,
            targets=_normalize_targets_csv(targets_csv),
            source="explicit",
        )

    lowered_operation = operation.strip().lower()
    lowered_target = target.strip().lower() or "snowflake"
    if lowered_operation not in {"run", "build"}:
        raise ValidationError(f"Invalid workflow_dispatch input operation `{operation}`.")
    if lowered_target != "all" and lowered_target not in WAREHOUSE_SET:
        raise ValidationError(f"Invalid workflow_dispatch input target `{target}`.")

    return ParsedRequest(
        command_tokens=["dbt", "build", "--full-refresh"] if lowered_operation == "build" else ["dbt", "run"],
        targets=WAREHOUSES.copy() if lowered_target == "all" else [lowered_target],
        source="legacy",
    )


def _authorize_request(author_association: str, parsed: ParsedRequest, command: ValidatedCommand) -> None:
    association = author_association.strip().upper()
    if association not in COLLABORATOR_ASSOCIATIONS:
        raise ValidationError("You are not authorized to run CI commands on this repository.")
    if parsed.all_targets and command.subcommand in {"build", "seed"} and association not in MAINTAINER_ASSOCIATIONS:
        raise ValidationError(
            "Only repository maintainers can run all-warehouse build or seed commands."
        )


def _emit_parsed_request(parsed: ParsedRequest, validated: ValidatedCommand) -> None:
    _write_outputs(
        {
            "allowed": "true",
            "dbt_command": validated.command_display,
            "dbt_command_json": json.dumps(validated.command_tokens),
            "command_label": f"{parsed.targets_display}: {validated.command_display}",
            "requires_seed_baseline": str(validated.requires_seed_baseline).lower(),
            "refreshes_seeds": str(validated.refreshes_seeds).lower(),
            "source": parsed.source,
            "subcommand": validated.subcommand,
            "targets_csv": parsed.targets_csv,
            "targets_json": json.dumps(parsed.targets),
        }
    )


def _emit_failure(message: str) -> int:
    _write_outputs({"allowed": "false", "message": message})
    return 1


def run_parse_comment() -> int:
    body = _trimmed_env("CI_COMMENT_BODY")
    author_association = _trimmed_env("CI_AUTHOR_ASSOCIATION")

    try:
        parsed = parse_comment_body(body)
        validated = validate_dbt_command(parsed.command_tokens)
        _authorize_request(author_association, parsed, validated)
    except ValidationError as exc:
        _write_outputs(
            {
                "allowed": "false",
                "message": (
                    f"{exc} Examples: `/ci`, `/ci snowflake dbt run`, "
                    "`/ci dbt seed --select tag:tuva_demo`."
                ),
            }
        )
        return 0

    _emit_parsed_request(parsed, validated)
    return 0


def run_resolve_dispatch() -> int:
    try:
        parsed = resolve_dispatch_inputs(
            dbt_command=_trimmed_env("CI_DBT_COMMAND"),
            targets_csv=_trimmed_env("CI_TARGETS_CSV"),
            operation=_trimmed_env("CI_OPERATION"),
            target=_trimmed_env("CI_TARGET"),
        )
        validated = validate_dbt_command(parsed.command_tokens)
    except ValidationError as exc:
        return _emit_failure(str(exc))

    _emit_parsed_request(parsed, validated)
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[1] not in {"parse-comment", "resolve-dispatch"}:
        sys.stderr.write("Usage: parse_ci_command.py <parse-comment|resolve-dispatch>\n")
        return 2

    if argv[1] == "parse-comment":
        return run_parse_comment()
    return run_resolve_dispatch()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
