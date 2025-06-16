import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# sys.path.append('')
from cleansing_utils import drop_duplicates

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession 

df = spark.read.option('header',True).csv('s3://dpk-glue-bucket/input/employees.csv')
df.show()
cleand_df = drop_duplicates(df,'EMAIL')
cleand_df.show()