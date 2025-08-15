from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from load_to_gold_module import get_latest
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").config("spark.sql.shuffle.partitions", "12").getOrCreate()

# Loading statistic Silver to Gold
fetch_max_id = spark.sql("SELECT MAX(id) as max_id from iceberg.gold.coin_sentiment_statistic").collect()
max_id = fetch_max_id[0]["max_id"] or 0
latest_partition = get_latest("silver.statistic")

silverStaDf = spark.sql(f"""
    SELECT * FROM silver.statistic
        WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"]} AND hour = {latest_partition["hour"]}
"""
)

silverSenDf = spark.sql(f"""
    SELECT * FROM silver.sentiment
        WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"]} AND hour = {latest_partition["hour"]}
"""
)

goldJoinDf = silverStaDf.join(silverSenDf, on="realtime_id", how="left")

goldJoinDf = goldJoinDf.withColumn("timestamp_id", F.date_format("endtime", "yyyyMMdd"))

goldCoinDf = spark.read.table("iceberg.silver.dim_coin")
goldJoinDf = goldJoinDf.join(goldCoinDf, F.substring(goldJoinDf["symbol"],1,3) == goldCoinDf["name"], how="left").select(goldJoinDf["*"], goldCoinDf["coin_id"])
goldJoinDf = goldJoinDf.drop("symbol")
goldJoinDf.writeTo("iceberg.gold.coin_sentiment_statistic").append()