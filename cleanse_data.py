from pyspark.sql import DataFrame

def remove_empty_strings(df: DataFrame, column: str) -> DataFrame:
    return df.filter(df[column] != '')