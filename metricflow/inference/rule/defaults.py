import re
from typing import List

from metricflow.inference.rule.base import InferenceRule, InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnRegexMatcherRule


class RuleDefaults:
    """Static factory class for sensible default rules."""

    # This is the default regex pattern that is used to determine if columns are identifiers.
    # It simply matches column names ending with "id", case insensitive.
    #
    # We searched for words ending with "id" just to assess the chance of this resulting in a
    # false positive. Our guess is most of those words would rarely, if ever, be used as column names.
    # Therefore, not adding a mandatory "_" before "id" would benefit the product by matching names
    # like "userid", despite the rare "squid", "mermaid" or "android" matches.
    #
    # See: https://www.thefreedictionary.com/words-that-end-in-id
    ANY_IDENTIFIER_REGEX_PATTERN = re.compile(r"id$", flags=re.IGNORECASE)

    # This is the default regex pattern that is used to determine if columns are primary identifiers.
    #
    # It is divided into two mutually exclusive parts "(A|B)":
    # - "A" is "(\.id$)", which only matches strings ending with ".id". Since we're compiling with
    #   re.IGNORECASE, it will also match "ID", "Id" and such. This will catch columns like
    #   "db.schema.table.id", where we assume a column simply named "id" must mean it is the primary
    #   identifier for its table.
    # - "B" is "([a-z0-9_]+)s?\.(.*)\2_?id$". Here's how it works:
    #    - Match the table name with "([a-z0-9_]+)", with the optional "s?" after the table name
    #      (catches plural table names).
    #    - The singular table name is saved onto the second capturing group.
    #    - "(.*)\2_?id" then checks if the name is prefixed by the table name and ends with "id",
    #      referencing the saved capturing group (table name) with "\2".
    #   In short, we assume that column names prefixed by the table name and ending with ID are probably
    #   primary identifiers for their tables. Examples:
    #    - "db.schema.customer.customer_id"
    #    - "db.schema.customers.customer_id"
    #    - "db.schema.customers.customerid"
    PRIMARY_IDENTIFIER_REGEX_PATTERN = re.compile(r"(\.id$)|([a-z0-9_]+)s?\.\2_?id$", flags=re.IGNORECASE)

    @staticmethod
    def default_ruleset() -> List[InferenceRule]:
        """Returns a sensible default set of inference rules."""
        return [
            RuleDefaults.primary_identifier_regex_rule(),
            RuleDefaults.any_identifier_regex_rule(),
        ]

    @staticmethod
    def primary_identifier_regex_rule() -> ColumnRegexMatcherRule:
        """A default for finding primary identifiers by their names based on regex matches.

        The returned rule will match columns such as `db.schema.mytable.mytable_id`,
        `db.schema.mytable.mytableid` and `db.schema.mytable.id`.

        It will always produce a PRIMARY_IDENTIFIER signal with FOR_SURE confidence.
        """
        return ColumnRegexMatcherRule(
            pattern=RuleDefaults.PRIMARY_IDENTIFIER_REGEX_PATTERN,
            signal_type=InferenceSignalType.PRIMARY_IDENTIFIER,
            confidence=InferenceSignalConfidence.FOR_SURE,
        )

    @staticmethod
    def any_identifier_regex_rule() -> ColumnRegexMatcherRule:
        """A default for finding identifiers of any type by their names based on regex matches.

        The returned rule will match columns such as `db.schema.mytable.id`,
        `db.schema.mytable.othertable_id` and `db.schema.mytable.othertableid`

        It will always produce an IDENTIFIER signal with HIGH confidence.
        """
        return ColumnRegexMatcherRule(
            pattern=RuleDefaults.ANY_IDENTIFIER_REGEX_PATTERN,
            signal_type=InferenceSignalType.IDENTIFER,
            confidence=InferenceSignalConfidence.HIGH,
        )
