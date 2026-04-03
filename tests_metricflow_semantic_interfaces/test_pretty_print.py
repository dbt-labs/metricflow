from __future__ import annotations

import logging
import textwrap

from metricflow_semantic_interfaces.pretty_print import pformat_big_objects, pretty_format
from metricflow_semantic_interfaces.test_utils import default_meta

logger = logging.getLogger(__name__)


def test_pretty_print() -> None:  # noqa: D103
    assert pretty_format("foo") == "foo"
    assert pretty_format(1) == "1"
    assert pretty_format(0.1) == "0.1"
    assert pretty_format(["bar", "baz"]) == "['bar', 'baz']"
    assert pretty_format(("bar", "baz")) == "('bar', 'baz')"
    assert pretty_format({"foo": 1}) == "{'foo': 1}"


def test_pformat_big_objects() -> None:  # noqa: D103
    meta = default_meta()
    assert pformat_big_objects(meta) == (
        textwrap.dedent(
            """\
                {'class': 'PydanticMetadata',
                 'repo_file_path': '/not/from/a/repo',
                 'file_slice': {'filename': 'not_from_file.py',
                                'content': 'N/A',
                                'start_line_number': 0,
                                'end_line_number': 0}}
            """
        ).rstrip()
    )
    logger.error(f"Output:\n{pformat_big_objects(meta=meta)}")
    assert pformat_big_objects(meta=meta) == (
        textwrap.dedent(
            """\
                meta:
                    {'class': 'PydanticMetadata',
                     'repo_file_path': '/not/from/a/repo',
                     'file_slice': {'filename': 'not_from_file.py',
                                    'content': 'N/A',
                                    'start_line_number': 0,
                                    'end_line_number': 0}}
            """
        ).rstrip()
    )
