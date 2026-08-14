#!/usr/bin/env python3
"""Enforce Tuva Core's model structure and naming rules.

Reads the model files directly, so it needs no warehouse, no credentials and no
manifest.

    python3 scripts/check_model_conventions.py          # check, exit 1 on any violation
    python3 scripts/check_model_conventions.py --list   # print the rule table

ADDING A RULE
-------------
Write one function and decorate it. Nothing else in this file changes, and no
existing rule is touched:

    @model_rule("name-suffix", "published models do not end in _v1")
    def check_no_version_suffix(m):
        if m.tier == "final" and m.name.endswith("_v1"):
            yield m.problem("published model names carry no version suffix")

Three decorators, chosen by what the rule needs to see:

    @model_rule     one .sql model      -> m.name, m.module, m.tier, m.subject,
                                           m.text, m.config_args
    @yaml_rule      one .yml file       -> y.text, y.line_of(offset)
    @project_rule   every model at once -> cross-file checks such as uniqueness

A rule is a generator that yields Problem objects; yielding nothing means it
passed. Rules never raise, never print and never exit -- main() does that. Each
rule is independently testable: build a context, call the function, assert on
what it yields. See scripts/test_check_model_conventions.py.

Vocabulary and structure live in the CONFIG block below, so adding a module or
renaming a term is a one-line data edit rather than a code change.

THE RULES
---------
`--list` prints the current set. In summary:

1. Model name is  [<stage>_]<module>__<subject>
     stg_  staging      int_  intermediate      (no marker)  final / published
2. <module> is from the closed set and equals the folder the model sits in.
   `claims_preprocessing` is a grouping folder, so its modules live one deeper.
3. The stage marker agrees with the tier folder. Modules with no tier folders
   are exempt.
4. <subject> uses one canonical term per concept -- no `asc`, `snf`, `psych`,
   `prof`, `ptotst`.
5. Relation names are DERIVED, never hand-written:
     stg_<module>__<subject>  ->  staging.<module>__<subject>
     int_<module>__<subject>  ->  intermediate.<module>__<subject>
     <module>__<subject>      ->  <layer>.<subject>
   macros/generate_alias_name.sql does this, so no model may set `alias`, and
   `schema:` in YAML must point at that file's anchor.
6. YAML owns model config. A SQL config() beats YAML for scalars and *merges*
   for tags, so declaring a key in both places either silently overrides the
   YAML or produces duplicates. Only `enabled` may live in a SQL config(),
   because it is var-driven logic.
7. YAML anchors are declared in the top-level `anchors:` block, never on a model
   entry. YAML resolves an anchor only if it appears before every `<<:` that
   merges it, so `config: &shared` hung off a model makes the file depend on
   that model continuing to sort first.
"""
from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# CONFIG -- the vocabulary. Edit these, not the rule bodies.
# --------------------------------------------------------------------------

MODELS = "models"

MODULES = {
    "input_layer", "normalized", "encounter", "service_category", "enrollment",
    "provider_attribution", "core", "data_quality", "metadata",
}

# Layers whose folder name is the module. `claims_preprocessing` is a grouping
# folder, so its modules live one level deeper.
GROUPING_FOLDERS = {"claims_preprocessing"}

# Modules with no tier folders: nothing to check the stage marker against.
FLAT_MODULES = {"input_layer", "metadata", "enrollment"}

TIER_MARKER = {"staging": "stg_", "intermediate": "int_", "final": ""}

# One term per concept.
BANNED_VOCAB = {
    "asc": "ambulatory_surgery_center",
    "snf": "skilled_nursing",
    "psych": "psychiatric",
    "prof": "professional",
    "ptotst": "therapy",
    "ambulatory_surgery": "ambulatory_surgery_center",
    "physical_therapy": "therapy",
}

# Keys that belong in YAML, never in a SQL config(). `enabled` is deliberately
# absent -- it may stay, because it is var-driven logic.
SQL_CONFIG_KEYS = {
    "alias": "the relation name is derived by macros/generate_alias_name.sql",
    "schema": "it defeats the file's schema anchor",
    "tags": "SQL and YAML tags merge, producing duplicates",
    "materialized": "it silently overrides the YAML config",
}

# --------------------------------------------------------------------------
# Patterns
# --------------------------------------------------------------------------

# Matched only INSIDE a config() call, so macro kwargs such as
# `select_extension_columns(..., alias='x')` are not flagged. An earlier version
# anchored on `^[ \t]*key[ \t]*=`, which silently missed the single-line form
# `{{ config(materialized='table') }}` -- a real violation that shipped.
SQL_CONFIG = {key: re.compile(r"\b%s[ \t]*=" % key) for key in SQL_CONFIG_KEYS}
CONFIG_CALL = re.compile(r"\bconfig[ \t]*\(")

YAML_ALIAS = re.compile(r"^[ \t]*alias:", re.M)
YAML_SCHEMA_NOT_ANCHOR = re.compile(r"^[ \t]*schema:(?![ \t]*\*)", re.M)
# An anchor definition in anchor position -- `- &name`, `key: &name`, `- &name |`.
# Not a `&` inside prose, which is why the line must consist of nothing else.
YAML_ANCHOR_DEF = re.compile(
    r"^[ \t]*(?:-[ \t]+)?(?:[\w.]+:[ \t]*)?&([A-Za-z_][\w-]*)(?:[ \t]*[|>][-+]?)?[ \t]*$", re.M
)
YAML_MODELS_KEY = re.compile(r"^models:[ \t]*$", re.M)


def token(word: str) -> re.Pattern:
    """Match `word` as an underscore-delimited token (\\b does not fire at '_')."""
    return re.compile(r"(?<![a-z])" + word + r"(?![a-z])")


def config_call_bodies(text: str):
    """Yield the argument text of every config( ... ) call, paren-balanced."""
    for m in CONFIG_CALL.finditer(text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
            i += 1
        yield text[m.end():i - 1]


# --------------------------------------------------------------------------
# Rule plumbing
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Problem:
    path: str
    message: str
    rule: str = ""
    line: int | None = None

    def render(self) -> str:
        where = f"{self.path}:{self.line}" if self.line else self.path
        return f"{where}  [{self.rule}] {self.message}"


@dataclass
class Rule:
    rule_id: str
    summary: str
    fn: object
    kind: str


RULES: list[Rule] = []


def _register(kind):
    def make(rule_id, summary):
        def deco(fn):
            RULES.append(Rule(rule_id, summary, fn, kind))
            return fn
        return deco
    return make


model_rule = _register("model")
yaml_rule = _register("yaml")
project_rule = _register("project")


@dataclass
class Model:
    """One .sql model file, with everything the rules need precomputed."""
    relpath: str
    name: str
    text: str
    module: str | None
    subject: str | None
    expected_module: str | None
    tier: str | None
    stage_marker: str
    config_args: str = ""

    def problem(self, message: str, line: int | None = None) -> Problem:
        return Problem(self.relpath, message, line=line)


@dataclass
class Yaml:
    relpath: str
    text: str

    def problem(self, message: str, line: int | None = None) -> Problem:
        return Problem(self.relpath, message, line=line)

    def line_of(self, offset: int) -> int:
        return self.text[:offset].count("\n") + 1


@dataclass
class Project:
    models: list = field(default_factory=list)


def build_model(relpath: str, text: str) -> Model:
    """Derive everything the model rules need from a path and its contents."""
    name = os.path.basename(relpath)[:-4]
    stage = name[:4] if name.startswith(("stg_", "int_")) else ""
    stem = name[len(stage):]
    module, subject = stem.split("__", 1) if "__" in stem else (None, None)

    parts = relpath.split(os.sep)
    layer = parts[1] if len(parts) > 1 else None
    if layer in GROUPING_FOLDERS:
        expected = parts[2] if len(parts) > 3 else None
        rest = parts[3:]
    else:
        expected = layer
        rest = parts[2:]
    tier = rest[0] if rest and rest[0] in TIER_MARKER else None

    return Model(
        relpath=relpath, name=name, text=text, module=module, subject=subject,
        expected_module=expected, tier=tier, stage_marker=stage,
        config_args="\n".join(config_call_bodies(text)),
    )


# --------------------------------------------------------------------------
# RULES -- add one by writing a decorated generator. Nothing else changes.
# --------------------------------------------------------------------------

@model_rule("name-shape", "model name is [stg_|int_]<module>__<subject>")
def check_name_shape(m: Model):
    if m.module is None:
        yield m.problem("no `<module>__<subject>` -- expected [stg_|int_]<module>__<subject>")


@model_rule("name-module", "<module> is in the closed set and equals its folder")
def check_name_module(m: Model):
    if m.module is None:
        return
    if m.module not in MODULES:
        yield m.problem(
            f"module `{m.module}` is not in the closed set ({', '.join(sorted(MODULES))})"
        )
    elif m.expected_module and m.module != m.expected_module:
        yield m.problem(
            f"module `{m.module}` does not match its folder `{m.expected_module}`"
        )


@model_rule("name-tier-marker", "the stage marker agrees with the tier folder")
def check_tier_marker(m: Model):
    if m.module is None or not m.tier or m.module in FLAT_MODULES:
        return
    want = TIER_MARKER[m.tier]
    if m.stage_marker != want:
        must = f"start with `{want}`" if want else "have no stage marker"
        got = f", but starts with `{m.stage_marker}`" if m.stage_marker else ""
        yield m.problem(f"in {m.tier}/ so it must {must}{got}")


@model_rule("name-vocabulary", "<subject> uses one canonical term per concept")
def check_vocabulary(m: Model):
    if not m.subject:
        return
    for bad, good in BANNED_VOCAB.items():
        for match in token(bad).finditer(m.subject):
            # `ambulatory_surgery` also matches inside `ambulatory_surgery_center`,
            # which is the approved term. Only flag when the approved term is not
            # what is actually written here.
            if m.subject.startswith(good, match.start()):
                continue
            yield m.problem(f"uses `{bad}` -- the standard term is `{good}`")
            return


@model_rule("sql-config-keys", "YAML owns model config; only `enabled` may sit in SQL")
def check_sql_config_keys(m: Model):
    for key, why in SQL_CONFIG_KEYS.items():
        if SQL_CONFIG[key].search(m.config_args):
            yield m.problem(f"sets `{key}` in SQL config() -- declare it in YAML instead: {why}")


@yaml_rule("yaml-no-alias", "relation names are derived, never hand-written")
def check_yaml_alias(y: Yaml):
    for match in YAML_ALIAS.finditer(y.text):
        yield y.problem(
            "hand-written `alias:` -- the relation name is derived by "
            "macros/generate_alias_name.sql",
            line=y.line_of(match.start()),
        )


@yaml_rule("yaml-schema-anchor", "`schema:` points at the file's anchor")
def check_yaml_schema_anchor(y: Yaml):
    for match in YAML_SCHEMA_NOT_ANCHOR.finditer(y.text):
        yield y.problem(
            "`schema:` must point at an anchor (*schema, *schema_staging, *schema_intermediate)",
            line=y.line_of(match.start()),
        )


@yaml_rule("yaml-anchor-placement", "anchors are declared in the top-level `anchors:` block")
def check_yaml_anchor_placement(y: Yaml):
    models_key = YAML_MODELS_KEY.search(y.text)
    if not models_key:
        return
    for match in YAML_ANCHOR_DEF.finditer(y.text, models_key.end()):
        yield y.problem(
            f"anchor `&{match.group(1)}` defined inside `models:` -- move it to the "
            f"top-level `anchors:` block. A YAML anchor must appear before every `<<:` "
            f"that merges it, so defining it on a model entry makes the file depend on "
            f"that model continuing to sort first",
            line=y.line_of(match.start()),
        )


@yaml_rule("yaml-file-name", "a model YAML is `_models.yml` or `<model_name>.yml`")
def check_yaml_file_name(y: Yaml):
    stem = os.path.basename(y.relpath)[: -len(".yml")]
    if stem == "_models":
        return
    # Published-output folders may instead carry one file per model, named for it.
    sibling = os.path.join(os.path.dirname(y.relpath), stem + ".sql")
    if not os.path.exists(sibling):
        yield y.problem(
            f"`{os.path.basename(y.relpath)}` is neither `_models.yml` nor named for a "
            f"model beside it -- one `_models.yml` per folder, or one `<model_name>.yml` "
            f"per model in published-output folders"
        )


@project_rule("unique-model-name", "model names are unique across the project")
def check_unique_names(p: Project):
    seen: dict = {}
    for m in sorted(p.models, key=lambda m: m.relpath):
        if m.name in seen:
            yield m.problem(f"duplicate model name, also at {seen[m.name]}")
        else:
            seen[m.name] = m.relpath


# --------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------

def collect(models_dir: str = MODELS):
    """Walk the tree once and build every rule context."""
    models, yamls = [], []
    for dirpath, _, filenames in os.walk(models_dir):
        for filename in sorted(filenames):
            path = os.path.join(dirpath, filename)
            relpath = os.path.relpath(path)
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
            if filename.endswith((".yml", ".yaml")):
                yamls.append(Yaml(relpath, text))
            elif filename.endswith(".sql"):
                models.append(build_model(relpath, text))
    return models, yamls


def run(models: list, yamls: list) -> list:
    """Run every registered rule and return the problems, stamped with rule ids."""
    project = Project(models)
    found = []
    for rule in RULES:
        targets = {"model": models, "yaml": yamls, "project": [project]}[rule.kind]
        for target in targets:
            for problem in rule.fn(target):
                found.append(
                    Problem(problem.path, problem.message, rule.rule_id, problem.line)
                )
    return found


def print_rules() -> int:
    width = max(len(r.rule_id) for r in RULES)
    for kind in ("model", "yaml", "project"):
        group = [r for r in RULES if r.kind == kind]
        if not group:
            continue
        print(f"\n{kind} rules ({len(group)}):")
        for r in group:
            print(f"  {r.rule_id:<{width}}  {r.summary}")
    print(f"\n{len(RULES)} rules total.")
    return 0


def main(argv: list) -> int:
    if "--list" in argv:
        return print_rules()

    if not os.path.isdir(MODELS):
        print("run from the repo root (no ./models directory)", file=sys.stderr)
        return 2

    models, yamls = collect()
    problems = run(models, yamls)

    if problems:
        print(f"{len(problems)} convention violation(s):\n")
        for p in sorted(problems, key=lambda p: (p.path, p.line or 0, p.rule)):
            print(f"  {p.render()}")
        print("\nRun --list to see every rule. They are documented at the top of this file.")
        return 1

    print(f"model conventions OK ({len(models)} models, {len(RULES)} rules)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
