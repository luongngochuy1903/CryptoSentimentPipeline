from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from load_to_gold_module import get_latest
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").config("spark.sql.shuffle.partitions", "12").getOrCreate()

# Loading realtime Silver to Gold
latest_partition = get_latest("silver.realtime")

silverRealtimeDf = spark.sql(f"""
    SELECT * FROM silver.realtime
        WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"]} AND hour = {latest_partition["hour"]}
"""
)

goldRealtimeDf = silverRealtimeDf.withColumn("starttime_id", F.date_format("starttime", "yyyyMMdd"))
goldRealtimeDf = goldRealtimeDf.withColumn("endtime_id", F.date_format("endtime", "yyyyMMdd"))
goldCoinDf = spark.read.table("gold.dim_coin")
goldRealtimeDf = goldRealtimeDf.join(goldCoinDf, goldRealtimeDf["name"] == goldCoinDf["name"], how="left").select(goldRealtimeDf["*"], goldCoinDf["coin_id"])
goldRealtimeDf= goldRealtimeDf.drop("symbol")
goldRealtimeDf = goldRealtimeDf.drop("name")
goldRealtimeDf = goldRealtimeDf.drop("tag")
goldRealtimeDf.writeTo("gold.dim_realtime").append()