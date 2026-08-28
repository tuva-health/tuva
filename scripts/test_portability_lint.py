#!/usr/bin/env python3
"""Unit tests for scripts/portability_lint.py. Standard library only."""

import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import portability_lint  # noqa: E402


SCRIPTS = Path(__file__).resolve().parent
RULES = SCRIPTS / "portability_rules.yml"


def rules():
    return portability_lint.load_rules(RULES)


def scan(text, relpath="models/example.sql", dialect_layer=False):
    return portability_lint.scan_text(
        textwrap.dedent(text), relpath, rules(), dialect_layer
    )


def scan_schema(text, relpath="models/example.yml"):
    return portability_lint.scan_schema_text(textwrap.dedent(text), relpath, rules())


def names(violations):
    return sorted(violation.rule for violation in violations)


class RulesFileTest(unittest.TestCase):
    def test_shipped_rules_cover_every_documented_kind(self):
        loaded = rules()

        for name in ("iff", "nvl", "datediff", "substr", "len", "array_agg"):
            self.assertIn(name, loaded.forbidden)
        self.assertEqual(loaded.forbidden["iff"], "use case when ... end")
        self.assertEqual(loaded.forbidden["nvl"], "use coalesce()")
        self.assertEqual(loaded.forbidden["charindex"], "use {{ dbt.position() }}")

        self.assertIn("qualify", loaded.forbidden_keywords)
        self.assertIn("ilike", loaded.forbidden_keywords)
        self.assertEqual(
            sorted(loaded.forbidden_syntax), ["double_colon_cast", "pipe_concat"]
        )
        self.assertEqual(sorted(loaded.forbidden_references), ["dbt_utils"])
        self.assertEqual(
            sorted(loaded.wrap_required),
            ["greatest", "least", "left", "right", "round", "trim"],
        )

    def test_every_forbidden_syntax_name_has_a_pattern_in_the_linter(self):
        for name in rules().forbidden_syntax:
            self.assertIn(name, portability_lint.SYNTAX_PATTERNS)

    def test_a_rules_file_it_cannot_read_raises_instead_of_parsing_to_nothing(self):
        complete = RULES.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rules.yml"

            path.write_text('forbidden:\n  iff: "use case"\n', encoding="utf-8")
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_rules(path)  # sections missing

            path.write_text('bogus:\n  iff: "x"\n', encoding="utf-8")
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_rules(path)

            path.write_text(
                complete + '\nforbidden_syntax:\n  made_up: "x"\n', encoding="utf-8"
            )
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_rules(path)  # duplicate section

            path.write_text(
                complete.replace(
                    '  double_colon_cast: "not in BigQuery or Fabric; '
                    'use cast(x as <type>)"',
                    '  made_up_operator: "x"',
                ),
                encoding="utf-8",
            )
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_rules(path)  # no pattern for that name

    def test_a_name_cannot_be_declared_in_two_sections(self):
        source = RULES.read_text(encoding="utf-8").replace(
            "  qualify:", "  iff:", 1
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rules.yml"
            path.write_text(source, encoding="utf-8")
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_rules(path)

    def test_a_quoted_hash_is_not_treated_as_a_yaml_comment(self):
        self.assertEqual(
            portability_lint._strip_yaml_comment('  iff: "a # b"  # tail'),
            '  iff: "a # b"  ',
        )


class MatchingTest(unittest.TestCase):
    def test_forbidden_and_wrap_required_calls_are_both_flagged(self):
        violations = scan("select nvl(a, b), iff(x, 1, 0), trim(c) from t")
        self.assertEqual(names(violations), ["iff", "nvl", "trim"])
        kinds = {violation.rule: violation.kind for violation in violations}
        self.assertEqual(kinds["nvl"], "forbidden")
        self.assertEqual(kinds["trim"], "wrap_required")

    def test_matching_is_case_insensitive_and_tolerates_space_before_the_paren(self):
        self.assertEqual(names(scan("select NVL (a, b) from t")), ["nvl"])
        self.assertEqual(names(scan("select IfF(x, 1, 0) from t")), ["iff"])

    def test_a_bare_word_without_a_call_is_not_a_match(self):
        self.assertEqual(scan("select a left join b on true"), [])
        self.assertEqual(scan("select nvl_total from t"), [])
        self.assertEqual(scan("select t.round_number from t"), [])

    def test_repeat_calls_on_one_line_are_counted(self):
        violations = scan("select trim(a), trim(b), trim(c) from t")
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].count, 3)
        self.assertEqual(violations[0].line, 1)

    def test_line_numbers_survive_stripping(self):
        violations = scan(
            """\
            select
                {{ dbt.dateadd('day', 1, 'x') }} as a
              , nvl(b, c) as b
            from t
            """
        )
        self.assertEqual([violation.line for violation in violations], [3])


class KeywordRuleTest(unittest.TestCase):
    def test_a_bare_keyword_is_flagged_without_a_call(self):
        violations = scan(
            """\
            select a
            from t
            qualify row_number() over (partition by a order by b) = 1
            """
        )
        self.assertEqual(names(violations), ["qualify"])
        self.assertEqual(violations[0].kind, "forbidden_keywords")

    def test_keywords_are_flagged_case_insensitively(self):
        self.assertEqual(names(scan("select a from t where b ILIKE 'x%'")), ["ilike"])

    def test_a_keyword_inside_a_word_is_not_a_match(self):
        self.assertEqual(scan("select qualifying_event from t"), [])
        self.assertEqual(scan("select current_date_flag from t"), [])

    def test_a_keyword_routed_through_jinja_is_not_flagged(self):
        self.assertEqual(scan("select {{ dbt.current_timestamp() }} as a"), [])


class SyntaxRuleTest(unittest.TestCase):
    def test_the_double_colon_cast_operator_is_flagged(self):
        violations = scan("select a::int as a from t")
        self.assertEqual(names(violations), ["double_colon_cast"])
        self.assertEqual(violations[0].kind, "forbidden_syntax")

    def test_the_concatenation_operator_is_flagged(self):
        self.assertEqual(names(scan("select a || b as ab from t")), ["pipe_concat"])

    def test_a_portable_cast_is_not_flagged(self):
        self.assertEqual(scan("select cast(a as date) as a from t"), [])

    def test_syntax_rules_respect_the_pragma(self):
        self.assertEqual(
            scan("select a::int -- tuva-lint: allow(double_colon_cast)"), []
        )


class ReferenceRuleTest(unittest.TestCase):
    def test_a_namespaced_macro_call_inside_jinja_is_flagged(self):
        violations = scan("select {{ dbt_utils.generate_surrogate_key(['a']) }} as k")
        self.assertEqual(names(violations), ["dbt_utils"])
        self.assertEqual(violations[0].kind, "forbidden_references")

    def test_a_namespaced_call_inside_a_statement_tag_is_flagged(self):
        self.assertEqual(
            names(scan("{% set columns = dbt_utils.get_column_values(ref('t'), 'c') %}")),
            ["dbt_utils"],
        )

    def test_the_replacement_namespace_is_not_flagged(self):
        self.assertEqual(
            scan("select {{ the_tuva_project.generate_surrogate_key(['a']) }} as k"), []
        )

    def test_a_namespaced_call_inside_a_jinja_comment_is_not_flagged(self):
        self.assertEqual(scan("{# {{ dbt_utils.pretty_time() }} #}\nselect 1"), [])

    def test_a_generic_test_reference_in_a_schema_file_is_flagged(self):
        violations = scan_schema(
            """\
            version: 2
            models:
              - name: example
                tests:
                  - dbt_utils.unique_combination_of_columns:
                      arguments:
                        combination_of_columns: [a, b]
            """
        )
        self.assertEqual(names(violations), ["dbt_utils"])
        self.assertEqual(violations[0].line, 5)

    def test_a_schema_file_without_the_namespace_is_clean(self):
        self.assertEqual(
            scan_schema("version: 2\nmodels:\n  - name: example\n    tests:\n      - not_null\n"),
            [],
        )


class StrippingTest(unittest.TestCase):
    def test_calls_routed_through_jinja_are_not_flagged(self):
        self.assertEqual(scan("select {{ dbt.datediff('a', 'b', 'day') }} from t"), [])
        self.assertEqual(
            scan("select {{ the_tuva_project.string_agg('a', \"','\") }} from t"), []
        )
        self.assertEqual(scan("{% set x = dbt.dateadd('day', 1, 'y') %}"), [])

    def test_multi_line_jinja_is_stripped_whole(self):
        self.assertEqual(
            scan(
                """\
                select {{ dbt.safe_cast(
                    'a',
                    api.Column.translate_type('string')
                ) }} as a
                from t
                """
            ),
            [],
        )

    def test_sql_and_jinja_comments_are_stripped(self):
        self.assertEqual(scan("-- nvl(a, b)\nselect 1"), [])
        self.assertEqual(scan("/* nvl(a, b)\n   iff(x, 1, 0) */\nselect 1"), [])
        self.assertEqual(scan("{# trim(a) #}\nselect 1"), [])

    def test_a_comment_after_real_sql_hides_only_the_comment(self):
        violations = scan("select nvl(a, b) from t -- iff(x, 1, 0)")
        self.assertEqual(names(violations), ["nvl"])

    def test_stripping_preserves_line_count_and_column_positions(self):
        text = "select {{ a }} /* x\ny */ trim(c)\n"
        stripped = portability_lint.strip_non_sql(text)
        self.assertEqual(len(stripped.splitlines()), len(text.splitlines()))
        self.assertEqual(stripped.index("trim("), text.index("trim("))

    def test_the_two_views_are_complementary_and_position_preserving(self):
        text = "select {{ dbt_utils.pretty_time() }}, trim(a) from t\n"
        sql, jinja = portability_lint.split_sql_and_jinja(text)

        self.assertEqual(len(sql), len(text))
        self.assertEqual(len(jinja), len(text))
        self.assertNotIn("dbt_utils", sql)
        self.assertIn("trim(", sql)
        self.assertIn("dbt_utils", jinja)
        self.assertNotIn("trim(", jinja)
        self.assertEqual(jinja.index("dbt_utils"), text.index("dbt_utils"))


class PragmaTest(unittest.TestCase):
    def test_a_same_line_pragma_waives_that_rule(self):
        self.assertEqual(scan("select nvl(a, b) -- tuva-lint: allow(nvl)"), [])

    def test_a_pragma_waives_only_the_rule_it_names(self):
        violations = scan("select nvl(a, b), iff(x, 1, 0) -- tuva-lint: allow(nvl)")
        self.assertEqual(names(violations), ["iff"])

    def test_a_pragma_does_not_leak_to_other_lines(self):
        violations = scan(
            """\
            select nvl(a, b) -- tuva-lint: allow(nvl)
              , nvl(c, d)
            from t
            """
        )
        self.assertEqual([violation.line for violation in violations], [2])

    def test_a_pragma_may_name_several_rules(self):
        self.assertEqual(
            scan("select nvl(a, b), trim(c) -- tuva-lint: allow(nvl, trim)"), []
        )

    def test_the_pragma_is_case_insensitive(self):
        self.assertEqual(scan("select NVL(a, b) -- TUVA-LINT: ALLOW(NVL)"), [])

    def test_a_pragma_waives_a_reference_on_the_same_line(self):
        self.assertEqual(
            scan("select {{ dbt_utils.pretty_time() }} -- tuva-lint: allow(dbt_utils)"),
            [],
        )


class DialectLayerScopeTest(unittest.TestCase):
    def test_wrap_required_calls_are_allowed_inside_the_dialect_layer(self):
        text = "select trim(a), least(b, c), round(d, 2) from t"
        self.assertEqual(
            names(scan(text, "macros/cross_database_utils/trim.sql", dialect_layer=True)),
            [],
        )
        self.assertEqual(
            names(scan(text, "models/example.sql", dialect_layer=False)),
            ["least", "round", "trim"],
        )

    def test_adapter_spellings_are_allowed_inside_the_dialect_layer(self):
        # That directory exists to hold them: try_cast on Snowflake, len on
        # Fabric, listagg behind string_agg, and so on.
        text = "select try_cast(a as date), len(b), c::int, d || e from t"
        self.assertEqual(
            names(
                scan(
                    text,
                    "macros/cross_database_utils/try_to_cast_date.sql",
                    dialect_layer=True,
                )
            ),
            [],
        )
        self.assertEqual(
            sorted(set(names(scan(text, "models/example.sql", dialect_layer=False)))),
            ["double_colon_cast", "len", "pipe_concat", "try_cast"],
        )

    def test_references_are_still_forbidden_inside_the_dialect_layer(self):
        violations = scan(
            "select {{ dbt_utils.generate_surrogate_key(['a']) }} as k",
            "macros/cross_database_utils/stable_id_hash.sql",
            dialect_layer=True,
        )
        self.assertEqual(names(violations), ["dbt_utils"])

    def test_the_dialect_layer_is_matched_by_path_segments(self):
        self.assertTrue(
            portability_lint.is_dialect_layer("macros/cross_database_utils/trim.sql")
        )
        self.assertTrue(
            portability_lint.is_dialect_layer(
                "dbt_packages/the_tuva_project/macros/cross_database_utils/trim.sql"
            )
        )
        self.assertFalse(portability_lint.is_dialect_layer("models/trim.sql"))
        self.assertFalse(
            portability_lint.is_dialect_layer("macros/cross_database_utils_extra/x.sql")
        )


class BaselineFormatTest(unittest.TestCase):
    def test_a_baseline_round_trips_through_write_and_read(self):
        counts = {("models/a.sql", "trim"): 3, ("models/b.sql", "nvl"): 1}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.txt"
            path.write_text(portability_lint.format_baseline(counts), encoding="utf-8")
            self.assertEqual(dict(portability_lint.load_baseline(path)), counts)

    def test_a_missing_baseline_reads_as_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "absent.txt"
            self.assertEqual(dict(portability_lint.load_baseline(path)), {})

    def test_a_malformed_baseline_line_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.txt"
            path.write_text("models/a.sql:trim:not-a-number\n", encoding="utf-8")
            with self.assertRaises(portability_lint.RulesError):
                portability_lint.load_baseline(path)


class CommandLineTest(unittest.TestCase):
    """Drive the real entry point inside a throwaway repo laid out like this one."""

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        (self.root / "scripts").mkdir()
        (self.root / "models").mkdir()
        (self.root / "macros" / "cross_database_utils").mkdir(parents=True)
        shutil.copy(SCRIPTS / "portability_lint.py", self.root / "scripts")
        shutil.copy(RULES, self.root / "scripts")
        self.addCleanup(self._directory.cleanup)

    def run_lint(self, *arguments):
        return subprocess.run(
            [sys.executable, str(self.root / "scripts" / "portability_lint.py"), *arguments],
            capture_output=True,
            text=True,
            check=False,
        )

    def write_model(self, name, text):
        (self.root / "models" / name).write_text(textwrap.dedent(text), encoding="utf-8")

    @property
    def baseline(self):
        return self.root / "scripts" / "portability_baseline.txt"

    def test_a_clean_tree_exits_zero(self):
        self.write_model("clean.sql", "select {{ dbt.current_timestamp() }} as a\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("clean", result.stdout)

    def test_a_violation_exits_one_and_names_file_line_and_suggestion(self):
        self.write_model("bad.sql", "select 1\nselect iff(1=1,'a','b')\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("models/bad.sql:2", result.stderr)
        self.assertIn("iff", result.stderr)
        self.assertIn("use case when ... end", result.stderr)

    def test_a_schema_file_is_scanned_alongside_models(self):
        self.write_model("clean.sql", "select 1\n")
        (self.root / "models" / "schema.yml").write_text(
            "version: 2\nmodels:\n  - name: clean\n    tests:\n"
            "      - dbt_utils.unique_combination_of_columns\n",
            encoding="utf-8",
        )
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("models/schema.yml:5", result.stderr)
        self.assertIn("dbt_utils", result.stderr)

    def test_update_baseline_then_lint_passes_at_the_recorded_count(self):
        self.write_model("bad.sql", "select nvl(a, b), nvl(c, d) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.assertIn("models/bad.sql:nvl:2", self.baseline.read_text())
        self.assertEqual(self.run_lint().returncode, 0)

    def test_one_more_occurrence_than_the_baseline_fails(self):
        self.write_model("bad.sql", "select nvl(a, b) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.write_model("bad.sql", "select nvl(a, b), nvl(c, d) from t\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("models/bad.sql:1", result.stderr)

    def test_a_baseline_for_one_rule_does_not_cover_another(self):
        self.write_model("bad.sql", "select nvl(a, b) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.write_model("bad.sql", "select nvl(a, b) from t qualify x = 1\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("qualify", result.stderr)

    def test_a_baseline_for_one_file_does_not_cover_another(self):
        self.write_model("bad.sql", "select nvl(a, b) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.write_model("other.sql", "select nvl(a, b) from t\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("models/other.sql", result.stderr)
        self.assertNotIn("models/bad.sql", result.stderr)

    def test_report_lists_occurrences_that_are_at_or_under_baseline(self):
        self.write_model("bad.sql", "select nvl(a, b) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        result = self.run_lint("--report")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("models/bad.sql:1", result.stdout)
        self.assertIn("1 at or under baseline", result.stdout)

    def test_models_dir_is_repeatable_and_scopes_the_wrapper_exemption(self):
        self.write_model("clean.sql", "select 1\n")
        (self.root / "macros" / "cross_database_utils" / "trim.sql").write_text(
            "{% macro default__trim(expression) %} trim( {{ expression }} ) {% endmacro %}\n",
            encoding="utf-8",
        )
        (self.root / "macros" / "other.sql").write_text(
            "{% macro helper() %} select trim(a) from t {% endmacro %}\n",
            encoding="utf-8",
        )
        result = self.run_lint("--models-dir", "models", "--models-dir", "macros")
        self.assertEqual(result.returncode, 1)
        self.assertIn("macros/other.sql", result.stderr)
        self.assertNotIn("cross_database_utils/trim.sql", result.stderr)

    def test_a_missing_models_dir_is_an_error_not_a_silent_pass(self):
        result = self.run_lint("--models-dir", "nope")
        self.assertEqual(result.returncode, 2)
        self.assertIn("no such directory", result.stderr)

    def test_absent_default_directories_are_skipped_rather_than_failing(self):
        # This throwaway repo has no seeds, snapshots or analyses path.
        self.write_model("clean.sql", "select 1\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_default_scan_covers_the_whole_project_not_just_models(self):
        self.write_model("clean.sql", "select 1\n")
        (self.root / "seeds").mkdir()
        (self.root / "seeds" / "seeds.yml").write_text(
            "version: 2\nseeds:\n  - name: s\n    tests:\n"
            "      - dbt_utils.unique_combination_of_columns\n",
            encoding="utf-8",
        )
        (self.root / "tests").mkdir()
        (self.root / "tests" / "singular.sql").write_text(
            "select substr(a, 1, 2) from t\n", encoding="utf-8"
        )
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("seeds/seeds.yml", result.stderr)
        self.assertIn("tests/singular.sql", result.stderr)

    def test_installed_packages_and_build_output_are_never_scanned(self):
        self.write_model("clean.sql", "select 1\n")
        for excluded in ("dbt_packages", "target"):
            path = self.root / "models" / excluded / "vendor.sql"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("select nvl(a, b), iff(c, 1, 0) from t\n", encoding="utf-8")
        result = self.run_lint()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_tree_cleaner_than_the_baseline_fails_as_stale(self):
        self.write_model("bad.sql", "select nvl(a, b), nvl(c, d) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.write_model("bad.sql", "select coalesce(a, b), nvl(c, d) from t\n")

        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("baseline is above what the tree contains", result.stderr)
        self.assertIn("baseline 2, tree 1", result.stderr)
        self.assertIn("--update-baseline", result.stderr)

    def test_the_ratchet_stops_a_fixed_violation_coming_back(self):
        self.write_model("bad.sql", "select nvl(a, b), nvl(c, d) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)

        # Fix both, and bring the baseline down in the same change.
        self.write_model("bad.sql", "select coalesce(a, b), coalesce(c, d) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        self.assertEqual(self.run_lint().returncode, 0)

        # Now the old code cannot return.
        self.write_model("bad.sql", "select nvl(a, b), nvl(c, d) from t\n")
        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("above baseline", result.stderr)

    def test_a_baseline_entry_for_a_deleted_file_is_reported_as_stale(self):
        self.write_model("bad.sql", "select nvl(a, b) from t\n")
        self.assertEqual(self.run_lint("--update-baseline").returncode, 0)
        (self.root / "models" / "bad.sql").unlink()

        result = self.run_lint()
        self.assertEqual(result.returncode, 1)
        self.assertIn("models/bad.sql:nvl: baseline 1, tree 0", result.stderr)


if __name__ == "__main__":
    unittest.main()
