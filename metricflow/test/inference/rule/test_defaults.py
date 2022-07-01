from metricflow.inference.rule.base import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.defaults import RuleDefaults

from metricflow.inference.rule.rules import ColumnRegexMatcherRule


def test_any_identifier_regex():  # noqa: D
    assert RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.id")
    assert RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.tableid")
    assert RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.table_id")
    assert RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.othertable_id")
    assert not RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.whatever")


def test_any_identifier_regex_rule_factory():  # noqa: D
    rule = RuleDefaults.any_identifier_regex_rule()
    assert isinstance(rule, ColumnRegexMatcherRule)
    assert rule.pattern == RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN
    assert rule.confidence == InferenceSignalConfidence.HIGH
    assert rule.signal_type == InferenceSignalType.IDENTIFER


def test_primary_identifier_regex():  # noqa: D
    assert RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.id")
    assert RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.tableid")
    assert RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.table_id")
    assert not RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.othertable_id")
    assert not RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN.search("db.schema.table.whatever")


def test_primary_identifier_regex_rule_factory():  # noqa: D
    rule = RuleDefaults.primary_identifier_regex_rule()
    assert isinstance(rule, ColumnRegexMatcherRule)
    assert rule.pattern == RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN
    assert rule.confidence == InferenceSignalConfidence.FOR_SURE
    assert rule.signal_type == InferenceSignalType.PRIMARY_IDENTIFIER
