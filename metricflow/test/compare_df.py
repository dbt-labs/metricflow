import math
import pandas as pd


def assert_dataframes_equal(
    actual: pd.DataFrame, expected: pd.DataFrame, sort_columns: bool = True, allow_empty: bool = False
) -> None:
    """Check that contents of DataFrames are the same.

    If sort_columns is set to false, value and column order needs to be the same.
    """
    if set(actual.columns) != set(expected.columns):
        raise ValueError(
            f"DataFrames do not contain the same columns. actual: {set(actual.columns)}, "
            f"expected: {set(expected.columns)}"
        )

    if not allow_empty and actual.shape[0] == 0 and expected.shape[0] == 0:
        raise AssertionError("Both dataframes have no rows; likely there is a mistake with the test")

    equal = False
    # dataframes may be of different shapes causing equality check to fail
    if expected.shape == actual.shape:
        if sort_columns:
            sort_by = list(sorted(actual.columns.tolist()))
            expected = expected.loc[:, sort_by].sort_values(sort_by).reset_index(drop=True)
            actual = actual.loc[:, sort_by].sort_values(sort_by).reset_index(drop=True)
        equal = True
        # Compare all of the elements of the dataframe one by one. If someone knows pandas magic, this can be improved.
        for c in range(expected.shape[0]):
            for r in range(actual.shape[1]):
                # Floats need to be compared with an epsilon due to rounding error.
                # A % difference might be better.

                # NaNs can't be compared for equality.
                if pd.isna(expected.iloc[c, r]) and pd.isna(actual.iloc[c, r]):
                    equal = True
                elif isinstance(expected.iloc[c, r], float) and isinstance(actual.iloc[c, r], float):
                    equal = math.isclose(expected.iloc[c, r], actual.iloc[c, r])
                elif expected.iloc[c, r] != actual.iloc[c, r]:
                    equal = False

    if not equal:
        raise ValueError(
            f"Dataframes not equal.\n"
            f"Expected:\n{expected.to_markdown(index=False)}"
            "\n---\n"
            f"Actual:\n{actual.to_markdown(index=False)}"
        )
