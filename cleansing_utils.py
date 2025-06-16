from pyspark.sql import Dataframe

def drop_duplicates(df: Dataframe, column: str) -> Dataframe:
    return df.dropDuplicates([column])
