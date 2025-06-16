import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
 
#sys.path.append('s3://dpk-glue-bucket/libraries')
from cleanse_data import remove_empty_strings
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
 
df = spark.read.csv('s3://dpk-glue-bucket/input/users.csv', header=True)
cleaned_df = remove_empty_strings(df, 'email')
cleaned_df.write.parquet('s3://dpk-glue-bucket/cleaned_users')