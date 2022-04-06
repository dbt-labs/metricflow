class SqlClientException(Exception):
    """Exception to represent errors related to the SqlClient"""

    def __init__(self) -> None:  # noqa: D
        super().__init__(
            "An error occurred when attempting to create/interact with the DB",
        )


class MetricFlowInitException(Exception):
    """Exception to represent errors related to the MetricFlow creation"""

    def __init__(self) -> None:  # noqa: D
        super().__init__(
            "An error occurred when attempting to create the MetricFlow engine",
        )


class ModelCreationException(Exception):
    """Exception to represent errors related to the building a model"""

    def __init__(self) -> None:  # noqa: D
        super().__init__(
            "An error occurred when attempting to build the semantic model",
        )
