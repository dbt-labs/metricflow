import logging
import textwrap

from metricflow.object_utils import pretty_format, pformat_big_objects
from metricflow.specs import DimensionSpec, IdentifierReference

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
        element_name="country_latest", identifier_links=(IdentifierReference(element_name="listing"),)
    )

    assert pformat_big_objects(dimension_spec) == (
        textwrap.dedent(
            """\
            {'class': 'DimensionSpec',
             'element_name': 'country_latest',
             'identifier_links': ({'class': 'IdentifierReference',
                                   'element_name': 'listing'},)}
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
                 'identifier_links': ({'class': 'IdentifierReference',
                                       'element_name': 'listing'},)}
            """
        ).rstrip()
    )
