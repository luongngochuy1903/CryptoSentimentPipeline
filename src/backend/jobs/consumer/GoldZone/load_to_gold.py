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
    goldTopicDf = spark.read.table("iceberg.gold.dim_topic")
    goldNewsDf = goldNewsDf.join(goldTopicDf, goldNewsDf["tag"] == goldTopicDf["topic"], how="left").select(goldNewsDf["*"], goldTopicDf["id_topic"])
    goldNewsDf.drop("tag")
    goldNewsDf.writeTo("iceberg.gold.dim_news").createOrReplace()

def write_from_comments_silver_to_gold():
    silverCommentsDf = spark.read.table("iceberg.silver.comments")
    goldCommentsDf = silverCommentsDf.withColumn("created_utc", F.date_format("published", "yyyyMMdd"))

    goldTopicDf = spark.read.table("iceberg.gold.dim_topic")
    goldCommentsDf = goldCommentsDf.join(goldTopicDf, goldCommentsDf["tag"] == goldTopicDf["topic"], how="left").select(goldCommentsDf["*"], goldTopicDf["id_topic"])
    goldCommentsDf.drop("tag")

    goldCoinDf = spark.read.table("iceberg.gold.dim_coin")
    goldCommentsDf = goldCommentsDf.join(goldCoinDf, goldCommentsDf["subreddit"] == goldCoinDf["name"], how="left").select(goldCommentsDf["*"], goldCoinDf["coin_id"])
    goldCommentsDf.drop("subreddit")

    goldAuthorDf = spark.read.table("iceberg.gold.author_credit")
    new_author = goldCommentsDf.filter(F.col("author").isNotNull()).select("author", "score").distinct()
    exist_author = goldAuthorDf.select("author", "score").distinct()
    new_author = new_author.subtract(exist_author)

    fetch_max_id = spark.sql("SELECT MAX(id_author) as max_id from iceberg.gold.author_credit").collect()
    max_id = fetch_max_id[0]["max_id"] or 0
    spec = Window.orderBy("author")
    new_author = new_author.withColumn("id_author", F.row_number().over(spec) + F.lit(max_id))
    goldAuthorDf = goldAuthorDf.unionByName(new_author)

    goldCommentsDf = goldCommentsDf.join(goldAuthorDf, goldCommentsDf["author"] == goldAuthorDf["author"], how='left').select(goldCommentsDf["*"], goldAuthorDf["id_author"])

    new_author.writeTo("iceberg.gold.author_credit").createOrReplace()
    goldCommentsDf.writeTo("iceberg.gold.dim_comments").createOrReplace()


def write_from_realtime_silver_to_gold():
    # Loading realtime Silver to Gold
    fetch_max_id = spark.sql("SELECT MAX(realtime_id) as max_id from iceberg.gold.dim_realtime").collect()
    max_id = fetch_max_id[0]["max_id"] or 0
    silverNewsDf = spark.read.table("iceberg.silver.realtime")
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
    goldRealtimeDf.writeTo("iceberg.gold.dim_news").createOrReplace()

    