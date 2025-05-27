import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_date
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://football-project/collect/"]},
    format="csv",
    format_options={"withHeader": True}
)

df = datasource.toDF()
df = df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
df = df.dropna(subset=["HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"])
cleaned_dynamic_df = DynamicFrame.fromDF(df, glueContext, "cleaned_dynamic_df")

glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_df,
    connection_type="s3",
    connection_options={"path": "s3://football-project/translate/"},
    format="csv"
)
