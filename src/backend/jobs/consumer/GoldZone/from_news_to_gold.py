from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from load_to_gold_module import get_latest
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").config("spark.sql.shuffle.partitions", "12").getOrCreate()

# Loading News Silver to Gold
fetch_max_id = spark.sql("SELECT MAX(id) as max_id from iceberg.gold.dim_news").collect()
max_id = fetch_max_id[0]["max_id"] or 0

latest_partition = get_latest("silver.news")

silverNewsDf = spark.sql(f"""
    SELECT * FROM silver.news
        WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"] } AND hour = {latest_partition["hour"]}
"""
)
spec = Window.orderBy("published")

goldNewsDf = silverNewsDf.withColumn("id", F.row_number().over(spec) + F.lit(max_id))
goldNewsDf.printSchema()
goldNewsDf = goldNewsDf.withColumn("timestamp_id", F.date_format("published", "yyyyMMdd"))
goldTopicDf = spark.read.table("gold.dim_topic")
goldNewsDf = goldNewsDf.join(goldTopicDf, goldNewsDf["tag"] == goldTopicDf["topic"], how="left").select(goldNewsDf["*"], goldTopicDf["id_topic"])
goldNewsDf = goldNewsDf.drop("tag")
goldNewsDf.writeTo("gold.dim_news").append()