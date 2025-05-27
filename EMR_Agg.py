from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder.appName("TeamStats").getOrCreate()
df = spark.read.parquet("s3://football-project/curated/match_parquet/")
team_stats = df.groupBy("HomeTeam").agg(
    count("*").alias("HomeMatches"),
    avg("FTHG").alias("AvgHomeGoals")
)
team_stats.write.mode("overwrite").parquet("s3://football-project/curated/team_stats/")
