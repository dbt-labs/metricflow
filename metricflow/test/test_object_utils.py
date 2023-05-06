import logging
import textwrap

from dbt_semantic_interfaces.pretty_print import pretty_format, pformat_big_objects
from metricflow.specs import DimensionSpec, EntityReference

logger = logging.getLogger(__name__)


def test_pretty_print() -> None:  # noqa: D
    assert pretty_format("foo") == "foo"
    assert pretty_format(1) == "1"
    assert pretty_format(0.1) == "0.1"
    assert pretty_format(["bar", "baz"]) == "['bar', 'baz']"
    assert pretty_format(("bar", "baz")) == "('bar', 'baz')"
    assert pretty_format({"foo": 1}) == "{'foo': 1}"


def test_pformat_big_objects() -> None:  # noqa: D
    dimension_spec = DimensionSpec(
        element_name="country_latest", entity_links=(EntityReference(element_name="listing"),)
    )

    assert pformat_big_objects(dimension_spec) == (
        textwrap.dedent(
            """\
            {'class': 'DimensionSpec',
             'element_name': 'country_latest',
             'entity_links': ({'class': 'EntityReference', 'element_name': 'listing'},)}
            """
        ).rstrip()
    )
    logger.error(f"Output:\n{pformat_big_objects(dimension_spec=dimension_spec)}")
    assert pformat_big_objects(dimension_spec=dimension_spec) == (
        textwrap.dedent(
            """\
            dimension_spec:
                {'class': 'DimensionSpec',
                 'element_name': 'country_latest',
                 'entity_links': ({'class': 'EntityReference', 'element_name': 'listing'},)}
            """
        ).rstrip()
    )
