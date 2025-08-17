from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from load_to_gold_module import get_latest
now = datetime.now()
spark = SparkSession.builder.appName("Loading to Gold zone").config("spark.sql.shuffle.partitions", "12").getOrCreate()
latest_partition = get_latest("silver.comments")

silverCommentsDf = spark.sql(f"""
        SELECT * FROM silver.comments
            WHERE year = {now.year} AND month = {now.month} AND day = {latest_partition["day"]} AND hour = {latest_partition["hour"]}
    """
    )

goldCommentsDf = silverCommentsDf.withColumn("created_utc", F.date_format("published", "yyyyMMdd"))

goldTopicDf = spark.read.table("gold.dim_topic")
goldCommentsDf = goldCommentsDf.join(goldTopicDf, goldCommentsDf["tag"] == goldTopicDf["topic"], how="left").select(goldCommentsDf["*"], goldTopicDf["id_topic"])
goldCommentsDf.drop("tag")

goldCoinDf = spark.read.table("gold.dim_coin")
goldCommentsDf = goldCommentsDf.join(goldCoinDf, goldCommentsDf["subreddit"] == goldCoinDf["name"], how="left").select(goldCommentsDf["*"], goldCoinDf["coin_id"])
goldCommentsDf.drop("subreddit")

goldAuthorDf = spark.read.table("gold.author_credit")
new_author = goldCommentsDf.filter(F.col("author").isNotNull()).select("author", "score").distinct()
exist_author = goldAuthorDf.select("author", "score").distinct()
new_author = new_author.subtract(exist_author)

fetch_max_id = spark.sql("SELECT MAX(id_author) as max_id from iceberg.gold.author_credit").collect()
max_id = fetch_max_id[0]["max_id"] or 0
spec = Window.orderBy("author")
new_author = new_author.withColumn("id_author", F.row_number().over(spec) + F.lit(max_id))
goldAuthorDf = goldAuthorDf.unionByName(new_author)

goldCommentsDf = goldCommentsDf.join(goldAuthorDf, goldCommentsDf["author"] == goldAuthorDf["author"], how='left').select(goldCommentsDf["*"], goldAuthorDf["id_author"])

new_author.writeTo("gold.author_credit").append()
goldCommentsDf.writeTo("gold.dim_comments").append()