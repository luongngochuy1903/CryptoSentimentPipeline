from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Loading to Gold zone").getOrCreate()

def write_from_news_silver_to_gold():
    # Loading News Silver to Gold
    fetch_max_id = spark.sql("SELECT MAX(id) as max_id from iceberg.gold.dim_news").collect()
    max_id = fetch_max_id[0]["max_id"] or 0
    silverNewsDf = spark.read.table("iceberg.silver.news")
    spec = Window.orderBy("published")

    goldNewsDf = silverNewsDf.withColumn("id", F.row_number().over(spec) + F.lit(max_id))
    goldNewsDf = goldNewsDf.withColumn("published", F.date_format("published", "yyyyMMdd"))
    goldTopicDf = spark.read.table("iceberg.silver.dim_topic")
    goldNewsDf = goldNewsDf.join(goldTopicDf, goldNewsDf["tag"] == goldTopicDf["topic"], how="left").select(goldNewsDf["*"], goldTopicDf["id_topic"])