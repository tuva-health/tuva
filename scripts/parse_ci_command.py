#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shlex
import sys
import uuid
from dataclasses import dataclass


WAREHOUSES = ["snowflake", "bigquery", "databricks", "fabric", "redshift", "duckdb"]
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
    command_sequences: list[list[str]]
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


@dataclass(frozen=True)
class ValidatedCommandSequence:
    commands: list[ValidatedCommand]
    requires_seed_baseline: bool
    refreshes_seeds: bool

    @property
    def command_display(self) -> str:
        return " -> ".join(command.command_display for command in self.commands)

    @property
    def dispatch_command(self) -> str:
        return " ".join(command.command_display for command in self.commands)

    @property
    def first_subcommand(self) -> str:
        return self.commands[0].subcommand


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


def _split_command_sequences(command_tokens: list[str]) -> list[list[str]]:
    if not command_tokens:
        raise ValidationError("Missing dbt command. Example: `dbt run --select tag:tuva_demo`.")
    if command_tokens[0].lower() != "dbt":
        raise ValidationError("CI commands must start with `dbt`.")

    sequences: list[list[str]] = []
    current: list[str] = []
    for token in command_tokens:
        if token.lower() == "dbt":
            if current:
                sequences.append(current)
            current = ["dbt"]
            continue
        current.append(token)

    if current:
        sequences.append(current)

    return sequences


def _parse_legacy_alias(tokens: list[str]) -> ParsedRequest | None:
    if not tokens:
        return None

    alias = tokens[0].strip().lower()
    remainder = tokens[1:]
    if alias in {"run", "build"}:
        operation = alias
        targets = WAREHOUSES.copy()
    elif alias.startswith("run-") or alias.startswith("build-"):
        operation, _, target = alias.partition("-")
        if not target:
            raise ValidationError(
                "Missing warehouse in legacy CI alias. Example: `/ci run-snowflake`."
            )
        targets = _normalize_targets([target])
    else:
        return None

    base_command = ["dbt", operation]
    if operation == "build":
        base_command.append("--full-refresh")

    return ParsedRequest(
        command_sequences=[base_command + remainder],
        targets=targets,
        source="alias",
    )


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
        refreshes_seeds=subcommand in {"seed", "build"},
    )


def validate_dbt_sequence(command_sequences: list[list[str]]) -> ValidatedCommandSequence:
    validated_commands = [validate_dbt_command(command_tokens) for command_tokens in command_sequences]

    requires_seed_baseline = False
    refreshes_seeds = False
    seen_seed_refresh = False
    for command in validated_commands:
        if command.requires_seed_baseline and not seen_seed_refresh:
            requires_seed_baseline = True
        if command.refreshes_seeds:
            refreshes_seeds = True
            seen_seed_refresh = True

    return ValidatedCommandSequence(
        commands=validated_commands,
        requires_seed_baseline=requires_seed_baseline,
        refreshes_seeds=refreshes_seeds,
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
        return ParsedRequest(command_sequences=[["dbt", "run"]], targets=WAREHOUSES.copy(), source="default")

    alias_request = _parse_legacy_alias(remainder)
    if alias_request is not None:
        return alias_request

    if any(token.lower() == "dbt" for token in remainder):
        dbt_index = next(index for index, token in enumerate(remainder) if token.lower() == "dbt")
        target_tokens = remainder[:dbt_index]
        return ParsedRequest(
            command_sequences=_split_command_sequences(remainder[dbt_index:]),
            targets=_normalize_targets(target_tokens),
            source="explicit",
        )

    return ParsedRequest(
        command_sequences=[["dbt", "run"]],
        targets=_normalize_targets(remainder),
        source="default",
    )


def resolve_dispatch_inputs(
    dbt_command: str,
    targets_csv: str,
    operation: str,
    target: str,
) -> ParsedRequest:
    if dbt_command.strip():
        return ParsedRequest(
            command_sequences=_split_command_sequences(_split_shell_words(dbt_command)),
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
        command_sequences=[
            ["dbt", "build", "--full-refresh"] if lowered_operation == "build" else ["dbt", "run"]
        ],
        targets=WAREHOUSES.copy() if lowered_target == "all" else [lowered_target],
        source="legacy",
    )


def _authorize_request(
    author_association: str,
    parsed: ParsedRequest,
    command_sequence: ValidatedCommandSequence,
) -> None:
    association = author_association.strip().upper()
    if association not in COLLABORATOR_ASSOCIATIONS:
        raise ValidationError("You are not authorized to run CI commands on this repository.")
    if parsed.all_targets and command_sequence.refreshes_seeds and association not in MAINTAINER_ASSOCIATIONS:
        raise ValidationError("Only repository maintainers can run all-warehouse build or seed commands.")


def _emit_parsed_request(parsed: ParsedRequest, validated: ValidatedCommandSequence) -> None:
    normalized_sequences = [command.command_tokens for command in validated.commands]
    _write_outputs(
        {
            "allowed": "true",
            "dbt_command": validated.dispatch_command,
            "dbt_command_json": json.dumps(validated.commands[0].command_tokens),
            "dbt_commands_json": json.dumps(normalized_sequences),
            "command_label": f"{parsed.targets_display}: {validated.command_display}",
            "requires_seed_baseline": str(validated.requires_seed_baseline).lower(),
            "refreshes_seeds": str(validated.refreshes_seeds).lower(),
            "source": parsed.source,
            "subcommand": validated.first_subcommand,
            "first_subcommand": validated.first_subcommand,
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
        validated = validate_dbt_sequence(parsed.command_sequences)
        _authorize_request(author_association, parsed, validated)
    except ValidationError as exc:
        _write_outputs(
            {
                "allowed": "false",
                "message": (
                    f"{exc} Examples: `/ci`, `/ci snowflake dbt run`, "
                    "`/ci snowflake fabric dbt seed dbt run`, `/ci run-snowflake`."
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
        validated = validate_dbt_sequence(parsed.command_sequences)
    except ValidationError as exc:
        return _emit_failure(
            f"{exc} Examples: `dbt run`, `dbt seed dbt run`, `dbt seed --select tag:tuva_demo dbt run`."
        )

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
