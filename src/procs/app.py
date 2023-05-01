"""
An example stored procedure. __main__ provides an entrypoint for local development
and testing.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import col, DataFrame
from snowflake.snowpark.types import StringType
from util.local import add_import

def run(snowpark_session: Session) -> int:
    """
    A sample stored procedure which creates a small DataFrame, prints it to the
    console, and returns the number of rows in the table.
    """

    # Register UDF
    import udf
    # this works locally but fails in snow proc
    add_import(snowpark_session,udf)
    combine = snowpark_session.udf.register(
        udf.functions.combine, StringType(), input_types=[StringType(), StringType()]
    )

    schema = ["col_1", "col_2"]

    data = [
        ("Welcome to ", "Snowflake!"),
        ("Learn more: ", "https://www.snowflake.com/snowpark/"),
    ]

    df: DataFrame = snowpark_session.create_dataframe(data, schema)

    df2 = df.select(combine(col("col_1"), col("col_2")).as_("Hello world")).sort(
        "Hello world", ascending=False
    )

    df2.show()
    return df2.count()


if __name__ == "__main__":

    # This entrypoint is used for local development.
    import sys
    import logging
    logging.basicConfig(level=logging.INFO)
    from util.local import get_session

    print("Creating session...")
    session = get_session(True)

    print("Running stored proc...")
    run(session)
