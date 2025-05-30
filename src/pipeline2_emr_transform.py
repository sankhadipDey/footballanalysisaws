from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FootballETL").getOrCreate()
df = spark.read.csv("s3://football-project/translate/", header=True, inferSchema=True)
df = df.withColumn("GoalDiff", col("FTHG") - col("FTAG"))
df.write.mode("overwrite").parquet("s3://football-project/curated/match_parquet/")