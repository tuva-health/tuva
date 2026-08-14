"""Unit tests for check_model_conventions.py.

Every rule is tested in both directions: a case that must pass and a case that
must be caught. The checker had shipped three false negatives before these
existed -- a line-anchored `materialized` regex that missed the single-line
`{{ config(materialized='table') }}` form, a 900-character scan window that hid
a second config() block, and no anchor-placement rule at all. A convention
checker that reports OK while violations pass through is worse than no checker,
so the shapes that fooled it each carry a regression test, marked REGRESSION.

    python -m unittest scripts/test_check_model_conventions.py
"""
import importlib.util
import os
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().with_name("check_model_conventions.py")
SPEC = importlib.util.spec_from_file_location("check_model_conventions", MODULE_PATH)
C = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = C
SPEC.loader.exec_module(C)


ENABLED_ONLY = "{{ config(\n     enabled = var('claims_enabled', False) | as_bool\n   )\n}}\n"

CLEAN_YAML = """version: 2

anchors:
  - &schema |
    core
  - &shared_config
    schema: *schema
    materialized: table

models:
  - name: core__patient
    description: R&D notes -- see A & B, and the &c.
    config:
      <<: *shared_config
"""


class Base(unittest.TestCase):
    def model(self, relpath, text=""):
        return C.build_model(relpath, text)

    def yaml(self, text, relpath="models/core/final/_models.yml"):
        return C.Yaml(relpath, text)

    def assertFires(self, rule_fn, ctx, contains=None):
        problems = list(rule_fn(ctx))
        self.assertTrue(problems, f"{rule_fn.__name__} should have fired but did not")
        if contains:
            self.assertIn(contains, problems[0].message)
        return problems

    def assertQuiet(self, rule_fn, ctx):
        problems = list(rule_fn(ctx))
        self.assertFalse(
            problems, f"{rule_fn.__name__} fired unexpectedly: "
                      f"{[p.message for p in problems]}")


class BuildModelTests(Base):
    """The parsing every model rule depends on."""

    def test_parses_staging(self):
        m = self.model("models/normalized/staging/stg_normalized__medical_claim.sql")
        self.assertEqual(
            (m.module, m.subject, m.tier, m.stage_marker),
            ("normalized", "medical_claim", "staging", "stg_"))

    def test_parses_published(self):
        m = self.model("models/core/final/core__patient.sql")
        self.assertEqual(
            (m.module, m.subject, m.tier, m.stage_marker), ("core", "patient", "final", ""))

    def test_grouping_folder_module_is_one_level_deeper(self):
        m = self.model("models/claims_preprocessing/encounter/final/encounter__lab.sql")
        self.assertEqual((m.expected_module, m.tier), ("encounter", "final"))

    def test_group_folder_collapses_to_its_tier(self):
        m = self.model("models/claims_preprocessing/encounter/intermediate/dme/"
                       "int_encounter__dme_generate_id.sql")
        self.assertEqual((m.expected_module, m.tier), ("encounter", "intermediate"))


class NameShapeTests(Base):
    def test_ok(self):
        self.assertQuiet(C.check_name_shape, self.model("models/core/final/core__patient.sql"))

    def test_missing_double_underscore(self):
        self.assertFires(C.check_name_shape, self.model("models/core/final/patient.sql"))


class NameModuleTests(Base):
    def test_ok(self):
        self.assertQuiet(C.check_name_module, self.model("models/core/final/core__patient.sql"))

    def test_module_not_in_closed_set(self):
        self.assertFires(C.check_name_module,
                         self.model("models/core/final/warehouse__patient.sql"),
                         "is not in the closed set")

    def test_module_mismatches_folder(self):
        self.assertFires(C.check_name_module,
                         self.model("models/core/final/normalized__patient.sql"),
                         "does not match its folder")

    def test_skips_unparseable_name(self):
        """A name-shape failure must not also produce a spurious module complaint."""
        self.assertQuiet(C.check_name_module, self.model("models/core/final/patient.sql"))


class TierMarkerTests(Base):
    def test_ok_staging(self):
        self.assertQuiet(C.check_tier_marker,
                         self.model("models/normalized/staging/stg_normalized__medical_claim.sql"))

    def test_ok_final_has_no_marker(self):
        self.assertQuiet(C.check_tier_marker, self.model("models/core/final/core__patient.sql"))

    def test_wrong_prefix_in_staging(self):
        self.assertFires(C.check_tier_marker,
                         self.model("models/normalized/staging/int_normalized__medical_claim.sql"),
                         "must start with `stg_`")

    def test_final_must_not_have_marker(self):
        self.assertFires(C.check_tier_marker,
                         self.model("models/core/final/int_core__patient.sql"),
                         "have no stage marker")

    def test_flat_module_is_exempt(self):
        """enrollment has no tier folders, so there is nothing to agree with."""
        self.assertQuiet(C.check_tier_marker,
                         self.model("models/claims_preprocessing/enrollment/"
                                    "enrollment__member_month.sql"))


class VocabularyTests(Base):
    def test_ok(self):
        self.assertQuiet(C.check_vocabulary,
                         self.model("models/claims_preprocessing/encounter/final/"
                                    "encounter__ambulatory_surgery_center.sql"))

    def test_banned_abbreviation(self):
        self.assertFires(C.check_vocabulary,
                         self.model("models/claims_preprocessing/encounter/final/"
                                    "encounter__asc.sql"),
                         "the standard term is `ambulatory_surgery_center`")

    def test_approved_term_not_flagged_by_its_own_prefix(self):
        """`ambulatory_surgery` is banned but is a prefix of the approved term."""
        self.assertQuiet(C.check_vocabulary,
                         self.model("models/claims_preprocessing/service_category/intermediate/"
                                    "int_service_category__ambulatory_surgery_center_"
                                    "institutional.sql"))

    def test_token_boundary(self):
        """`prof` must not fire inside `professional`."""
        self.assertQuiet(C.check_vocabulary,
                         self.model("models/claims_preprocessing/service_category/intermediate/"
                                    "int_service_category__combined_professional.sql"))


class SqlConfigTests(Base):
    def test_enabled_is_allowed(self):
        self.assertQuiet(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", ENABLED_ONLY))

    def test_multiline_materialized_caught(self):
        text = "{{ config(\n     schema = 'core',\n     materialized = 'table'\n   )\n}}"
        self.assertFires(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", text))

    def test_single_line_materialized_caught(self):
        """REGRESSION: the old line-anchored regex missed this exact form."""
        text = ENABLED_ONLY + "\n{{ config(materialized='table') }}\n\nselect 1\n"
        self.assertFires(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", text),
                         "`materialized`")

    def test_second_config_block_far_below_caught(self):
        """REGRESSION: the old scan stopped at 900 characters."""
        text = ENABLED_ONLY + ("-- filler\n" * 400) + "{{ config(materialized='table') }}\n"
        self.assertGreater(len(text), 900)
        self.assertFires(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", text))

    def test_macro_kwarg_not_flagged(self):
        """select_extension_columns(..., alias='x') is a macro kwarg, not model config."""
        text = ENABLED_ONLY + "select {{ select_extension_columns(ref('x'), alias='med') }}\n"
        self.assertQuiet(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", text))

    def test_nested_call_before_key_still_caught(self):
        """A nested paren must not end the config() scan early."""
        text = ("{{ config(\n     enabled = var('claims_enabled', False) | as_bool,\n"
                "     materialized = 'table'\n   )\n}}")
        self.assertFires(C.check_sql_config_keys,
                         self.model("models/core/final/core__patient.sql", text))


class YamlTests(Base):
    def test_clean_file_passes_every_yaml_rule(self):
        y = self.yaml(CLEAN_YAML)
        for rule in (C.check_yaml_alias, C.check_yaml_schema_anchor,
                     C.check_yaml_anchor_placement):
            self.assertQuiet(rule, y)

    def test_alias_caught(self):
        self.assertFires(C.check_yaml_alias,
                         self.yaml("models:\n  - name: x\n    config:\n      alias: patient\n"))

    def test_schema_must_be_anchor(self):
        self.assertFires(C.check_yaml_schema_anchor,
                         self.yaml("models:\n  - name: x\n    config:\n      schema: core\n"))

    def test_schema_anchor_reference_ok(self):
        self.assertQuiet(C.check_yaml_schema_anchor,
                         self.yaml("models:\n  - name: x\n    config:\n      schema: *schema\n"))

    def test_anchor_on_model_entry_caught(self):
        """REGRESSION: normalized/final shipped this shape, one rename from breaking."""
        text = ("version: 2\n\nanchors:\n  - &schema |\n    core\n\nmodels:\n"
                "  - name: a__b\n    config: &shared\n      schema: *schema\n")
        self.assertFires(C.check_yaml_anchor_placement, self.yaml(text), "&shared")

    def test_anchor_in_anchors_block_allowed(self):
        self.assertQuiet(C.check_yaml_anchor_placement, self.yaml(CLEAN_YAML))

    def test_ampersand_in_prose_not_flagged(self):
        r"""A naive &\w+ search would flag every ampersand in a description."""
        text = ("version: 2\n\nmodels:\n  - name: a__b\n"
                "    description: Costs & charges, R&D, A & B, and the &c.\n")
        self.assertQuiet(C.check_yaml_anchor_placement, self.yaml(text))

    def test_anchor_reports_line_number(self):
        text = "version: 2\n\nmodels:\n  - name: a__b\n    config: &shared\n      x: 1\n"
        problems = self.assertFires(C.check_yaml_anchor_placement, self.yaml(text))
        self.assertEqual(problems[0].line, 5)


    def test_file_name_models_yml_ok(self):
        self.assertQuiet(C.check_yaml_file_name,
                         self.yaml(CLEAN_YAML, "models/core/final/_models.yml"))

    def test_file_name_arbitrary_name_caught(self):
        self.assertFires(C.check_yaml_file_name,
                         self.yaml(CLEAN_YAML, "models/core/cost_utilization_models.yml"),
                         "neither `_models.yml` nor named for a model beside it")

    def test_file_name_per_model_yml_ok_when_sql_sits_beside_it(self):
        """input_layer keeps one <model_name>.yml per model."""
        self.assertQuiet(
            C.check_yaml_file_name,
            self.yaml(CLEAN_YAML,
                      "models/input_layer/stg_input_layer__patient.yml"))


class ProjectRuleTests(Base):
    def test_duplicate_model_name_caught(self):
        p = C.Project([self.model("models/core/final/core__patient.sql"),
                       self.model("models/normalized/final/core__patient.sql")])
        self.assertFires(C.check_unique_names, p, "duplicate model name")

    def test_unique_model_names_pass(self):
        p = C.Project([self.model("models/core/final/core__patient.sql"),
                       self.model("models/core/final/core__condition.sql")])
        self.assertQuiet(C.check_unique_names, p)


class RegistryTests(Base):
    def test_every_rule_has_a_unique_id_and_summary(self):
        seen = set()
        for r in C.RULES:
            self.assertTrue(r.rule_id, f"{r.fn.__name__} has no rule id")
            self.assertTrue(r.summary, f"{r.rule_id} has no summary")
            self.assertNotIn(r.rule_id, seen, f"duplicate rule id {r.rule_id}")
            seen.add(r.rule_id)

    def test_every_rule_kind_is_known(self):
        self.assertLessEqual({r.kind for r in C.RULES}, {"model", "yaml", "project"})

    def test_run_stamps_problems_with_their_rule_id(self):
        problems = C.run([self.model("models/core/final/patient.sql")], [])
        self.assertTrue(problems)
        self.assertIn("name-shape", {p.rule for p in problems})

    def test_run_on_clean_input_finds_nothing(self):
        models = [self.model("models/core/final/core__patient.sql", ENABLED_ONLY)]
        self.assertEqual(C.run(models, [self.yaml(CLEAN_YAML)]), [])

    def test_problem_render_includes_line_when_known(self):
        self.assertEqual(C.Problem("f.yml", "msg", "some-rule", 12).render(),
                         "f.yml:12  [some-rule] msg")
        self.assertEqual(C.Problem("f.sql", "msg", "some-rule").render(),
                         "f.sql  [some-rule] msg")


class RepositoryTests(Base):
    """The real tree must stay clean, and collect() must actually find it."""

    def test_repository_passes_all_rules(self):
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cwd = os.getcwd()
        try:
            os.chdir(repo)
            models, yamls = C.collect()
            problems = C.run(models, yamls)
            self.assertFalse(problems, "\n".join(p.render() for p in problems))
            self.assertGreater(len(models), 300,
                               f"only found {len(models)} models -- collect() is broken")
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
