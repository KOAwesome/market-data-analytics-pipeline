from pyspark.sql.functions import col


def check_not_null(df, columns, error_message):
    """
    Ensures specified columns contain no NULL values
    """
    condition = None
    for c in columns:
        expr = col(c).isNull()
        condition = expr if condition is None else condition | expr

    violations = df.filter(condition)

    if violations.limit(1).count() > 0:
        raise Exception(error_message)


def check_positive(df, column, error_message):
    """
    Ensures a numeric column is strictly positive
    """
    violations = df.filter(col(column) <= 0)

    if violations.limit(1).count() > 0:
        raise Exception(error_message)


def check_duplicates(df, key_columns, error_message):
    """
    Ensures uniqueness on business key
    """
    violations = (
        df.groupBy(*key_columns)
          .count()
          .filter(col("count") > 1)
    )

    if violations.limit(1).count() > 0:
        raise Exception(error_message)


def check_key_completeness(silver_df, bronze_df, key_columns, error_message):
    """
    Ensures Silver does not introduce new business keys
    """
    silver_keys = silver_df.select(*key_columns).distinct().count()
    bronze_keys = bronze_df.select(*key_columns).distinct().count()

    if silver_keys > bronze_keys:
        raise Exception(error_message)