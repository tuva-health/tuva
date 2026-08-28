#!/usr/bin/env python3
"""Flag raw SQL in dbt models that does not compile on every warehouse Tuva supports.

Rules live in scripts/portability_rules.yml, in five kinds:

  forbidden             a function that must never appear as raw SQL in a model.
  forbidden_keywords    the same, for a bare word rather than a call.
  forbidden_syntax      a non-portable operator, named here and patterned below.
  forbidden_references  a macro namespace that must not be called.
  wrap_required         a function whose spelling is owned by a macro in
                        macros/cross_database_utils/, and which is therefore
                        allowed inside that directory and nowhere else.

A model file is read as two complementary views that both keep their original
line and column positions: the SQL outside Jinja, and the Jinja itself. The
SQL-shaped rules are matched against the SQL view, so a call already routed
through a macro is not a violation. `forbidden_references` is matched against
the Jinja view instead, and against schema files whole, because that is where
a namespaced macro or generic test actually appears.

Files under macros/cross_database_utils/ are the dialect layer: per-adapter
spellings are supposed to live there, so the SQL-shaped rules are waived.
`forbidden_references` still applies, since nothing in that layer has any
business calling another package's macros.

A single occurrence can be waived with a same-line pragma::

    select nvl(a, b) as x  -- tuva-lint: allow(nvl)

Known violations are recorded in scripts/portability_baseline.txt as
``<relpath>:<rule>:<count>`` lines. The file is a ratchet: anything above its
count fails with the file, line and suggestion, and anything *below* it fails
too, asking for ``--update-baseline`` so the recorded debt comes down in the
same change and cannot grow back later.

Exit status is 0 when clean, 1 when there are violations above baseline, and 2
when the rules or baseline file cannot be read. Standard library only, so every
repo can carry a copy without a virtualenv.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = ROOT / "scripts" / "portability_rules.yml"
BASELINE_PATH = ROOT / "scripts" / "portability_baseline.txt"
DIALECT_LAYER_DIR = Path("macros") / "cross_database_utils"
DEFAULT_SCAN_DIRS = ("models", "macros", "tests", "seeds", "snapshots", "analyses")
# Installed packages and build output are never ours to lint.
EXCLUDED_DIR_NAMES = frozenset({"dbt_packages", "dbt_internal_packages", "target", "logs"})
SQL_SUFFIXES = (".sql",)
SCHEMA_SUFFIXES = (".yml", ".yaml")

PRAGMA_PATTERN = re.compile(r"tuva-lint:\s*allow\(([^)]*)\)", re.IGNORECASE)

# Operators cannot be expressed as a bare name in the rules file, so the rules
# file names them and the pattern lives here.
SYNTAX_PATTERNS = {
    "double_colon_cast": re.compile(r"::\s*[A-Za-z_]"),
    "pipe_concat": re.compile(r"\|\|"),
}

MAPPING_SECTIONS = (
    "forbidden",
    "forbidden_keywords",
    "forbidden_syntax",
    "forbidden_references",
)
SEQUENCE_SECTIONS = ("wrap_required",)

JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)
JINJA_TAG = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
LINE_COMMENT = re.compile(r"--[^\n]*")


class RulesError(Exception):
    """Raised when a rules or baseline file cannot be read as written."""


class Rules:
    """The parsed rules file."""

    __slots__ = MAPPING_SECTIONS + SEQUENCE_SECTIONS

    def __init__(self, forbidden, forbidden_keywords, forbidden_syntax,
                 forbidden_references, wrap_required):
        self.forbidden = forbidden
        self.forbidden_keywords = forbidden_keywords
        self.forbidden_syntax = forbidden_syntax
        self.forbidden_references = forbidden_references
        self.wrap_required = wrap_required


class Violation:
    """One rule match that is not allowed at this location."""

    __slots__ = ("relpath", "line", "rule", "count", "suggestion", "kind")

    def __init__(self, relpath, line, rule, count, suggestion, kind):
        self.relpath = relpath
        self.line = line
        self.rule = rule
        self.count = count
        self.suggestion = suggestion
        self.kind = kind

    @property
    def key(self):
        return (self.relpath, self.rule)

    def describe(self):
        occurrences = f" ({self.count} occurrences)" if self.count > 1 else ""
        return (
            f"{self.relpath}:{self.line}: {self.rule}{occurrences} "
            f"-- {self.suggestion}"
        )


def _strip_yaml_comment(line):
    """Drop a trailing ``#`` comment, ignoring ``#`` inside a quoted scalar."""
    quote = None
    for index, char in enumerate(line):
        if quote is not None:
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
            continue
        if char == "#":
            return line[:index]
    return line


def _unquote(value):
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def _parse_flow_sequence(text, path):
    start = text.find("[")
    end = text.find("]")
    if start == -1 or end == -1 or end < start:
        raise RulesError(f"{path}: wrap_required must be a [a, b, c] sequence")
    items = []
    for item in text[start + 1 : end].split(","):
        item = _unquote(item.strip())
        if not item:
            continue
        items.append(item.lower())
    if not items:
        raise RulesError(f"{path}: wrap_required must name at least one entry")
    return items


def load_rules(path=RULES_PATH):
    """Read the rules file.

    A hand-rolled reader for the small fixed shape of this file, so the linter
    stays standard-library only. Anything unexpected raises rather than being
    silently skipped: a rule that quietly fails to load is worse than no linter
    at all.
    """
    mappings = {section: {} for section in MAPPING_SECTIONS}
    wrap_required = []
    seen_sections = set()
    seen_names = {}
    section = None
    pending = None

    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = _strip_yaml_comment(raw_line)
        if pending is not None:
            pending += " " + line.strip()
            if "]" in pending:
                wrap_required = _parse_flow_sequence(pending, path)
                for name in wrap_required:
                    _claim_name(seen_names, name, "wrap_required", path, number)
                pending = None
                section = None
            continue
        if not line.strip():
            continue

        if not line[:1].isspace():
            key, separator, remainder = line.partition(":")
            key = key.strip()
            if not separator or key not in MAPPING_SECTIONS + SEQUENCE_SECTIONS:
                raise RulesError(
                    f"{path}:{number}: unknown top-level key {line.strip()!r}"
                )
            if key in seen_sections:
                raise RulesError(f"{path}:{number}: duplicate section {key!r}")
            seen_sections.add(key)
            section = key
            remainder = remainder.strip()
            if remainder:
                if section not in SEQUENCE_SECTIONS:
                    raise RulesError(
                        f"{path}:{number}: {key!r} takes a nested mapping"
                    )
                pending = remainder
                if "]" in pending:
                    wrap_required = _parse_flow_sequence(pending, path)
                    for name in wrap_required:
                        _claim_name(seen_names, name, "wrap_required", path, number)
                    pending = None
                    section = None
            continue

        if section in SEQUENCE_SECTIONS:
            pending = line.strip()
            if "]" in pending:
                wrap_required = _parse_flow_sequence(pending, path)
                for name in wrap_required:
                    _claim_name(seen_names, name, "wrap_required", path, number)
                pending = None
                section = None
            continue

        if section in MAPPING_SECTIONS:
            name, separator, suggestion = line.strip().partition(":")
            name = name.strip().lower()
            suggestion = _unquote(suggestion.strip())
            if not separator or not name or not suggestion:
                raise RulesError(f"{path}:{number}: expected 'name: \"suggestion\"'")
            _claim_name(seen_names, name, section, path, number)
            mappings[section][name] = suggestion
            continue

        raise RulesError(f"{path}:{number}: value outside any section")

    if pending is not None:
        raise RulesError(f"{path}: unterminated wrap_required sequence")

    unknown_syntax = set(mappings["forbidden_syntax"]) - set(SYNTAX_PATTERNS)
    if unknown_syntax:
        raise RulesError(
            f"{path}: forbidden_syntax names without a pattern in the linter: "
            f"{sorted(unknown_syntax)}"
        )
    for section in MAPPING_SECTIONS + SEQUENCE_SECTIONS:
        if section not in seen_sections:
            raise RulesError(f"{path}: missing section {section!r}")
    if not any(mappings[section] for section in MAPPING_SECTIONS):
        raise RulesError(f"{path}: no rules defined")
    if not wrap_required:
        raise RulesError(f"{path}: no wrap_required rules defined")

    return Rules(
        mappings["forbidden"],
        mappings["forbidden_keywords"],
        mappings["forbidden_syntax"],
        mappings["forbidden_references"],
        wrap_required,
    )


def _claim_name(seen_names, name, section, path, number):
    if name in seen_names:
        raise RulesError(
            f"{path}:{number}: {name!r} is already declared in "
            f"{seen_names[name]!r}"
        )
    seen_names[name] = section


def _blank(segment):
    return re.sub(r"[^\n]", " ", segment)


def split_sql_and_jinja(text):
    """Split a model into its SQL and Jinja views.

    Both views are the same length as the input, with the other view's regions
    blanked out, so line and column positions are preserved in each. Jinja
    comments and SQL comments belong to neither view.
    """
    text = JINJA_COMMENT.sub(lambda match: _blank(match.group(0)), text)

    jinja_characters = ["\n" if char == "\n" else " " for char in text]

    def hide_jinja(match):
        start = match.start()
        for offset, char in enumerate(match.group(0)):
            jinja_characters[start + offset] = char
        return _blank(match.group(0))

    sql = JINJA_TAG.sub(hide_jinja, text)
    sql = BLOCK_COMMENT.sub(lambda match: _blank(match.group(0)), sql)
    sql = LINE_COMMENT.sub(lambda match: _blank(match.group(0)), sql)
    return sql, "".join(jinja_characters)


def strip_non_sql(text):
    """The SQL view alone, with Jinja and comments blanked."""
    return split_sql_and_jinja(text)[0]


def allowed_by_pragma(line):
    """Rule names waived by ``-- tuva-lint: allow(...)`` on this line."""
    allowed = set()
    for group in PRAGMA_PATTERN.findall(line):
        for name in group.split(","):
            name = name.strip().lower()
            if name:
                allowed.add(name)
    return allowed


def _call_pattern(name):
    return re.compile(rf"\b{re.escape(name)}\s*\(", re.IGNORECASE)


def _word_pattern(name):
    return re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)


def _reference_pattern(name):
    return re.compile(rf"\b{re.escape(name)}\s*\.", re.IGNORECASE)


def is_dialect_layer(relpath):
    """True for files inside a macros/cross_database_utils/ directory.

    That directory is where per-adapter spellings are supposed to live, so the
    SQL-shaped rules are waived there. Reference rules are not: nothing in the
    dialect layer has any business calling another package's macros.
    """
    parts = Path(relpath).parent.parts
    wanted = DIALECT_LAYER_DIR.parts
    return any(
        parts[index : index + len(wanted)] == wanted
        for index in range(len(parts) - len(wanted) + 1)
    )


def _collect(view_lines, source_lines, checks, relpath):
    violations = []
    for index, view_line in enumerate(view_lines):
        if not view_line.strip():
            continue
        waived = allowed_by_pragma(source_lines[index])
        for name, pattern, suggestion, kind in checks:
            if name in waived:
                continue
            count = len(pattern.findall(view_line))
            if count:
                violations.append(
                    Violation(relpath, index + 1, name, count, suggestion, kind)
                )
    return violations


def scan_text(text, relpath, rules, dialect_layer=False):
    """Find every disallowed match in one model file's contents."""
    source_lines = text.splitlines()
    sql, jinja = split_sql_and_jinja(text)
    sql_lines = sql.splitlines()
    jinja_lines = jinja.splitlines()
    for lines in (sql_lines, jinja_lines):
        lines += [""] * (len(source_lines) - len(lines))

    sql_checks = []
    if not dialect_layer:
        sql_checks += [
            (name, _call_pattern(name), suggestion, "forbidden")
            for name, suggestion in rules.forbidden.items()
        ]
        sql_checks += [
            (name, _word_pattern(name), suggestion, "forbidden_keywords")
            for name, suggestion in rules.forbidden_keywords.items()
        ]
        sql_checks += [
            (name, SYNTAX_PATTERNS[name], suggestion, "forbidden_syntax")
            for name, suggestion in rules.forbidden_syntax.items()
        ]
        suggestion = "call the the_tuva_project wrapper in macros/cross_database_utils/"
        sql_checks += [
            (name, _call_pattern(name), suggestion, "wrap_required")
            for name in rules.wrap_required
        ]

    jinja_checks = [
        (name, _reference_pattern(name), suggestion, "forbidden_references")
        for name, suggestion in rules.forbidden_references.items()
    ]

    return (
        _collect(sql_lines, source_lines, sql_checks, relpath)
        + _collect(jinja_lines, source_lines, jinja_checks, relpath)
    )


def scan_schema_text(text, relpath, rules):
    """Find disallowed namespace references in a schema file.

    Generic tests are referenced by namespace in schema files rather than in
    Jinja, so the whole file is the reference view.
    """
    source_lines = text.splitlines()
    checks = [
        (name, _reference_pattern(name), suggestion, "forbidden_references")
        for name, suggestion in rules.forbidden_references.items()
    ]
    return _collect(source_lines, source_lines, checks, relpath)


def _relpath(path, root):
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def scan_paths(directories, rules, root=ROOT, required=True):
    """Scan every model and schema file under each directory, in a stable order.

    With ``required`` false a directory that does not exist is skipped, which is
    what the defaults need: not every package has a snapshots or analyses path.
    """
    violations = []
    for directory in directories:
        directory = Path(directory)
        if not directory.exists():
            if required:
                raise FileNotFoundError(f"no such directory: {directory}")
            continue
        candidates = sorted(
            path
            for path in directory.rglob("*")
            if path.is_file()
            and path.suffix.lower() in SQL_SUFFIXES + SCHEMA_SUFFIXES
            and EXCLUDED_DIR_NAMES.isdisjoint(path.parts)
        )
        for path in candidates:
            relpath = _relpath(path, root)
            text = path.read_text(encoding="utf-8")
            if path.suffix.lower() in SQL_SUFFIXES:
                violations.extend(
                    scan_text(text, relpath, rules, is_dialect_layer(relpath))
                )
            else:
                violations.extend(scan_schema_text(text, relpath, rules))
    return violations


def load_baseline(path=BASELINE_PATH):
    """Read ``<relpath>:<rule>:<count>`` lines into a count per file and rule."""
    baseline = Counter()
    if not path.exists():
        return baseline
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        relpath, separator, remainder = line.rpartition(":")
        if not separator:
            raise RulesError(f"{path}:{number}: expected '<relpath>:<rule>:<count>'")
        relpath, _, rule = relpath.rpartition(":")
        if not relpath or not rule:
            raise RulesError(f"{path}:{number}: expected '<relpath>:<rule>:<count>'")
        try:
            count = int(remainder)
        except ValueError:
            raise RulesError(f"{path}:{number}: count must be an integer") from None
        baseline[(relpath, rule.lower())] += count
    return baseline


def format_baseline(counts):
    header = (
        "# Known non-portable SQL, written by scripts/portability_lint.py\n"
        "# --update-baseline. Format: <relpath>:<rule>:<count>. A file and\n"
        "# rule pair at or under its count passes; anything above fails.\n"
        "# This file only shrinks -- never raise a count to land a change.\n"
    )
    body = "".join(
        f"{relpath}:{rule}:{count}\n"
        for (relpath, rule), count in sorted(counts.items())
    )
    return header + body


def _print_report(violations, baseline, stream):
    if not violations:
        print("portability lint: no non-portable SQL found", file=stream)
        return
    by_file = defaultdict(list)
    for violation in violations:
        by_file[violation.relpath].append(violation)
    print("portability lint report", file=stream)
    for relpath in sorted(by_file):
        print(f"  {relpath}", file=stream)
        for violation in sorted(by_file[relpath], key=lambda item: item.line):
            print(f"    {violation.describe()}", file=stream)
    counts = Counter()
    for violation in violations:
        counts[violation.key] += violation.count
    baselined = sum(min(count, baseline.get(key, 0)) for key, count in counts.items())
    total = sum(counts.values())
    print(
        f"  {total} occurrence(s) across {len(by_file)} file(s); "
        f"{baselined} at or under baseline",
        file=stream,
    )


def main(argv=None, stdout=None, stderr=None):
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--models-dir",
        action="append",
        dest="models_dirs",
        metavar="DIR",
        help=(
            "directory to scan for model and schema files; repeatable, "
            f"defaults to {' '.join(DEFAULT_SCAN_DIRS)}"
        ),
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="print every occurrence, including ones at or under baseline",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="rewrite the baseline from what is in the tree right now",
    )
    args = parser.parse_args(argv)

    explicit = bool(args.models_dirs)
    directories = [
        ROOT / directory for directory in (args.models_dirs or DEFAULT_SCAN_DIRS)
    ]

    try:
        rules = load_rules()
        violations = scan_paths(directories, rules, required=explicit)
    except (RulesError, FileNotFoundError) as error:
        print(f"portability lint: {error}", file=stderr)
        return 2

    counts = Counter()
    for violation in violations:
        counts[violation.key] += violation.count

    if args.update_baseline:
        BASELINE_PATH.write_text(format_baseline(counts), encoding="utf-8")
        print(
            f"portability lint: wrote {len(counts)} baseline entr(y|ies) to "
            f"{_relpath(BASELINE_PATH, ROOT)}",
            file=stdout,
        )
        return 0

    try:
        baseline = load_baseline()
    except RulesError as error:
        print(f"portability lint: {error}", file=stderr)
        return 2

    if args.report:
        _print_report(violations, baseline, stdout)
        # Keep the report ahead of the failure list when the two streams are
        # redirected to the same place.
        stdout.flush()

    failing_keys = {key for key, count in counts.items() if count > baseline.get(key, 0)}
    # The baseline is a ratchet, not a ceiling. When the tree is cleaner than
    # the baseline says, the baseline has to come down in the same change,
    # otherwise the debt it records can silently grow back later.
    stale_keys = {
        key for key, count in baseline.items() if counts.get(key, 0) < count
    }

    if not failing_keys and not stale_keys:
        if not args.report:
            print("portability lint: clean", file=stdout)
        return 0

    if failing_keys:
        print("portability lint: non-portable SQL above baseline", file=stderr)
        for violation in sorted(violations, key=lambda item: (item.relpath, item.line)):
            if violation.key in failing_keys:
                print(f"  {violation.describe()}", file=stderr)
        print(
            "Route the call through a the_tuva_project macro, or waive one "
            "occurrence with a same-line '-- tuva-lint: allow(<rule>)' pragma.",
            file=stderr,
        )

    if stale_keys:
        if failing_keys:
            print("", file=stderr)
        print("portability lint: the baseline is above what the tree contains", file=stderr)
        for relpath, rule in sorted(stale_keys):
            was = baseline[(relpath, rule)]
            now = counts.get((relpath, rule), 0)
            print(f"  {relpath}:{rule}: baseline {was}, tree {now}", file=stderr)
        print(
            "Run 'python3 scripts/portability_lint.py --update-baseline' and "
            "commit the result so the debt cannot grow back.",
            file=stderr,
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
