"""
Useful functions.
"""
from functools import reduce
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_dataframe(from_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split the input Dataframe in errors_df and ok_df.
    The first one contains the records that contains null fields, whereas the second one contains
    all the records without null values.

    Parameters
    ----------
    from_df: DataFrame
        Pyspark Dataframe.
    """
    errors_df = from_df.where(
        reduce(
            lambda col1, col2: col1 | col2,
            [col(col_name).isNull() for col_name in from_df.columns],
        )
    )

    ok_df = from_df.where(
        reduce(
            lambda col1, col2: col1 & col2,
            [col(col_name).isNotNull() for col_name in from_df.columns],
        )
    )

    return errors_df, ok_df
