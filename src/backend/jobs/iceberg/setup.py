from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("Setup iceberg table").getOrCreate()
# ------------------CREATE SILVER LAYER----------------------
spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.news (
            domain STRING,
            title STRING,
            url STRING,
            text STRING,
            published TIMESTAMP,
            author STRING,
            source STRING,
            tag STRING,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        LOCATION 's3a://silver/news/'
    """)

spark.sql("""

        CREATE TABLE IF NOT EXISTS silver.comments (
            subreddit STRING,
            title STRING,
            author STRING,
            score FLOAT,
            url STRING,
            created_utc TIMESTAMP,
            id STRING,
            self_text STRING,
            num_comments INT,
            tag STRING,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        LOCATION 's3a://silver/comments/'
    """)

spark.sql("""

        CREATE TABLE IF NOT EXISTS silver.realtime (
            symbol STRING,
            name STRING,
            interval STRING,
            starttime TIMESTAMP,
            endtime TIMESTAMP,
            volume FLOAT,
            quotevolume FLOAT,
            open FLOAT,
            close FLOAT,
            highest FLOAT,
            lowest FLOAT,
            tag STRING,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day, hour)
        LOCATION 's3a://silver/realtime/'
    """)
print("xong")
# Create table for ML table

# ------------------CREATE GOLD LAYER----------------------
spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold_statistic
        (
            id INT,
            coin_id STRING,
            realtime_id INT,
            timestamp_id STRING,
            RSI FLOAT,
            MACD FLOAT,
            bolling_band FLOAT,
            SMA FLOAT,
            EMA FLOAT,
            ATR FLOAT,
            year INT,
            month INT,
            day INT,
            hour INT,
            minutes INT
        )
        USING ICEBERG
        PARTITIONED BY (coin_id)
        LOCATION 's3a:///gold/statistic/'
          """)

spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold_sentiment
        (
            id INT,
            coin_id STRING,
            sentiment_signal STRING,
            timestamp_id STRING,
            RSI FLOAT,
            MACD FLOAT,
            bolling_band FLOAT,
            SMA FLOAT,
            EMA FLOAT,
            ATR FLOAT
        )
        USING ICEBERG
        PARTITIONED BY (coin_id)
        LOCATION 's3a:///gold/realtime/'
          """)

spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold_dim_time
        (
            timestamp_id STRING,
            year INT,
            month INT,
            day INT,
            hour INT,
            minutes INT,
            second INT,
            week INT,
            day_week STRING,
            datetime TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        LOCATION 's3a:///gold/dim_time/'
          """)


spark.sql("""

        CREATE TABLE IF NOT EXISTS iceberg.gold_dim_news (
            id INT,
            id_topic INT,
            domain STRING,
            title STRING,
            url STRING,
            text STRING,
            published TIMESTAMP,
            author STRING,
            source STRING,
            tag STRING,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (id_topic, year, month)
        LOCATION 's3a://gold/news/'
    """)

spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_dim_comments (
            coin_id INT,
            id_topic INT,
            timestamp_id STRING,
            title STRING,
            id_author STRING,
            score FLOAT,
            url STRING,
            id STRING,
            self_text STRING,
            num_comments INT,
            tag STRING,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (id_author, id_topic, year, month)
        LOCATION 's3a://gold/comments/'
    """)

spark.sql("""

        CREATE TABLE IF NOT EXISTS iceberg.gold_dim_realtime (
            realtime_id INT,
            coin_id INT,
            interval STRING,
            starttime STRING,
            endtime STRING,
            volume FLOAT,
            quotevolume FLOAT,
            open FLOAT,
            close FLOAT,
            highest FLOAT,
            lowest FLOAT,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day, hour)
        LOCATION 's3a://gold/realtime/'
    """)


spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_author_credit (
            id_author INT,
            author STRING,
            credit_score FLOAT
        )
        USING ICEBERG
        LOCATION 's3a://gold/author_credit/'
    """)

spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_dim_coin (
            coin_id INT,
            symbol STRING,
            name STRING
        )
        USING ICEBERG
        LOCATION 's3a://gold/dim_coin/'
    """)

spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_dim_topic (
            id_topic INT,
            topic STRING
        )
        USING ICEBERG
        LOCATION 's3a://gold/author_credit/'
    """)

# #-------------------Tạo dim_time-------------------
def create_dim_time():
    # Tạo list ngày
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2028, 12, 31)
    date_list = [(start_date + timedelta(days=i)).date() for i in range((end_date - start_date).days + 1)]

    # Tạo DataFrame
    df_date = spark.createDataFrame([(d,) for d in date_list], ["date"])

    # Tạo các cột thời gian
    dim_time = df_date.withColumn("date_id", date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("week_of_year", weekofyear("date")) \
        .withColumn("quarter", quarter("date")) \
        .withColumn("day_of_week", date_format("date", "EEEE")) \
        .withColumn("is_weekend", expr("dayofweek(date) IN (1,7)")) \
        .withColumn("month_name", date_format("date", "MMMM"))

    dim_time.writeTo("iceberg.gold.dim_time").createOrReplace()

#-----------------Tạo dim_coin---------------
def create_dim_coin():
    topic_symbol = [
    ("bitcoin", "BTC"),
    ("ethereum", "ETH"),
    ("bnb", "BNB"),
    ("xrp", "XRP"),
    ("solana", "SOL")
    ]
    df_topic = spark.createDataFrame(topic_symbol, ["name", "symbol"])
    spec = Window.orderBy("name")
    df_topic = df_topic.withColumn("coin_id", row_number().over(spec))

# #----------------Tạo dim_author--------------

print("xong")