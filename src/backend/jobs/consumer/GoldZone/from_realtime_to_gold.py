from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from load_to_gold_module import get_latest
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").getOrCreate()

# Loading realtime Silver to Gold
fetch_max_id = spark.sql("SELECT MAX(realtime_id) as max_id from iceberg.gold.dim_realtime").collect()
max_id = fetch_max_id[0]["max_id"] or 0
latest_partition = get_latest("silver.realtime")

silverNewsDf = spark.sql(f"""
    SELECT * FROM silver.realtime
        WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"]} AND hour = {latest_partition["hour"]}
"""
)

spec = Window.orderBy("starttime")

goldRealtimeDf = silverNewsDf.withColumn("realtime_id", F.row_number().over(spec) + F.lit(max_id))
goldRealtimeDf = goldRealtimeDf.withColumn("starttime", F.date_format("starttime", "yyyyMMdd"))
goldRealtimeDf = goldRealtimeDf.withColumn("endtime", F.date_format("endtime", "yyyyMMdd"))
goldTopicDf = spark.read.table("iceberg.silver.dim_topic")
goldRealtimeDf = goldRealtimeDf.join(goldTopicDf, goldRealtimeDf["tag"] == goldTopicDf["topic"], how="left").select(goldRealtimeDf["*"], goldTopicDf["id_topic"])
goldCoinDf = spark.read.table("iceberg.silver.dim_coin")
goldRealtimeDf = goldRealtimeDf.join(goldCoinDf, goldRealtimeDf["name"] == goldCoinDf["name"], how="left").select(goldRealtimeDf["*"], goldCoinDf["coin_id"])
goldRealtimeDf.drop("symbol")
goldRealtimeDf.drop("name")
goldRealtimeDf.writeTo("iceberg.gold.dim_news").append()