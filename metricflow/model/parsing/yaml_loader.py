from typing import Dict

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
