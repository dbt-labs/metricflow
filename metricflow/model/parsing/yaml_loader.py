from __future__ import annotations

from io import StringIO
from typing import Dict, Iterator

import yaml

"""This is the name of the field the SafeLineLoader adds to the parsed yaml
   we retrieve line number and the file name from the context stored at this key"""
PARSING_CONTEXT_KEY = "__parsing_context__"


class ParsingContext:  # noqa: D
    def __init__(self, start_line: int, end_line: int, filename: str) -> None:  # noqa: D
        self.start_line = start_line
        self.end_line = end_line
        self.filename = filename

    def __str__(self) -> str:  # noqa: D
        return f"line: {self.start_line}, filename: {self.filename}"


class YamlConfigLoader:
    """Helper class for loading YAML config strings into an iterator of YAML output"""

    @staticmethod
    def load_all_with_context(name: str, contents: str) -> Iterator:
        """Wraps the yaml.load_all method and returns the resulting iterator with parsing context added to output

        This replaces any calls to yaml.load_all(loader=SafeLineLoader), which internally adds ParsingContext info.
        Note PyYAML reads the name property from the input stream IF that input stream is a file object, otherwise
        it replaces it with a constant. Therefore, we use as StringIO instance to pass the contents into PyYAML and
        set the value of the name property on the file object to the name parameter here.
        """
        with StringIO(initial_value=contents) as stream:
            stream.name = name
            for document in yaml.load_all(stream=stream, Loader=SafeLineLoader):
                yield document

    @staticmethod
    def load_all_without_context(contents: str) -> Iterator:
        """Convenience wrapper for yaml.load_all with the standard yaml.SafeLoader

        This method is here more for readability than anything else.
        """
        return yaml.load_all(stream=contents, Loader=yaml.SafeLoader)


class SafeLineLoader(yaml.SafeLoader):
    """Adds special field __parsing_context__ to all mappings.

    Credit: https://stackoverflow.com/questions/13319067/parsing-yaml-return-with-line-number
    """

    # we may also want to consider this parser https://yaml.readthedocs.io/en/latest/
    # which supports yaml1.2 and maintains round-trip parsing, for now we use
    # the more established road
    # https://stackoverflow.com/questions/55441300/how-can-i-get-the-parent-node-within-yaml-loader-add-contructor
    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> Dict:  # noqa: D
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        mapping[PARSING_CONTEXT_KEY] = ParsingContext(
            node.start_mark.line + 1, node.end_mark.line, node.start_mark.name
        )  # change to 1-indexed

        return mapping

    @staticmethod
    def is_valid_yaml_file_ending(filename: str) -> bool:
        """Checks if YAML file name ends with one of the supported suffixes"""
        return filename.endswith(".yaml") or filename.endswith(".yml")
