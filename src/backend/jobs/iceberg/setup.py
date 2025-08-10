from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("Setup iceberg table").getOrCreate()
# -------------------Tạo dim_time-------------------
def create_dim_time():
    # Tạo list ngày
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2028, 12, 31)
    date_list = [(start_date + timedelta(days=i)).date() for i in range((end_date - start_date).days + 1)]

    df_date = spark.createDataFrame([(d,) for d in date_list], ["datetime"])

    dim_time = df_date.withColumn("timestamp_id", date_format("datetime", "yyyyMMdd")) \
        .withColumn("year", year("datetime")) \
        .withColumn("month", month("datetime")) \
        .withColumn("day", dayofmonth("datetime")) \
        .withColumn("week_of_year", weekofyear("datetime")) \
        .withColumn("quarter", quarter("datetime")) \
        .withColumn("day_of_week", date_format("datetime", "EEEE")) \
        .withColumn("is_weekend", expr("dayofweek(datetime) IN (1,7)"))

    dim_time.writeTo("gold.dim_time").append()

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
    df_topic.writeTo("gold.dim_coin").append()

#----------------Tạo dim_topic---------------
def create_dim_topic():
    topic = [
        ("economy",),
        ("coin",),
        ("politics",),
        ("comment",)
    ]
    df_topic = spark.createDataFrame(topic, ["topic"])
    spec = Window.orderBy("topic")
    df_topic = df_topic.withColumn("id_topic", row_number().over(spec))
    df_topic.writeTo("gold.dim_topic").append()
#----------------Tạo dim_author--------------

if __name__ == "__main__":
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
                day INT,
                hour INT
            )
            USING ICEBERG
            PARTITIONED BY (year, month, day, hour)
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
                day INT,
                hour INT
            )
            USING ICEBERG
            PARTITIONED BY (year, month, day, hour)
            LOCATION 's3a://silver/comments/'
        """)

    spark.sql("""

            CREATE TABLE IF NOT EXISTS silver.realtime (
                event_id INT,
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
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.technical (
        id INT,
        event_id INT,
        endtime TIMESTAMP,
        symbol STRING,
        sma20 DOUBLE,
        ema12 DOUBLE,
        rsi10 DOUBLE,
        macd DOUBLE,
        bb DOUBLE,
        atr DOUBLE,
        va_high DOUBLE,
        va_low DOUBLE,
        POC DOUBLE,
        year INT,
        month INT,
        day INT,
        hour INT
    )
    USING ICEBERG
    PARTITIONED BY (year, month, day, hour)
    LOCATION 's3a://silver/technical/';
    """
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.sentiment (
        id INT,
        event_id INT,
        endtime TIMESTAMP,
        RSI_sen STRING,
        MACD_sen STRING,
        EMA_sen STRING,
        bb_sen STRING,
        SMA_sen STRING,
        ATR_sen STRING,
        year INT,
        month INT,
        day INT,
        hour INT
    )
    USING ICEBERG
    PARTITIONED BY (year, month, day, hour)
    LOCATION 's3a://silver/sentiment/';
    """
    )
    # Create table for ML table

    # ------------------CREATE GOLD LAYER----------------------
    spark.sql("""CREATE TABLE IF NOT EXISTS gold.dim_coin_sentiment_statistic
            (
                id INT,
                coin_id STRING,
                realtime_id INT,
                timestamp_id STRING,
                rsi10 FLOAT,
                rsi_sen STRING,
                macd FLOAT,
                macd_sen STRING,
                bolling_band FLOAT,
                bb_sen STRING,
                sma20 FLOAT,
                sma_sen STRING,
                ema12 FLOAT,
                ema_sen STRING,
                atr FLOAT,
                atr_sen STRING,
                va_high FLOAT,
                va_low FLOAT,
                POC FLOAT,
                sentiment_signal STRING,
                year INT,
                month INT,
                day INT,
                hour INT,
            )
            USING ICEBERG
            PARTITIONED BY (coin_id)
            LOCATION 's3a://gold/statistic_sentiment/'
            """)

    spark.sql("""CREATE TABLE IF NOT EXISTS gold.dim_time
            (
                timestamp_id STRING,
                year INT,
                month INT,
                day INT,
                week_of_year INT,
                quarter INT,
                day_of_week STRING,
                is_weekend BOOLEAN,
                datetime TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (year, month)
            LOCATION 's3a://gold/dim_time/'
            """)


    spark.sql("""

            CREATE TABLE IF NOT EXISTS gold.dim_news (
                id INT,
                id_topic INT,
                domain STRING,
                title STRING,
                url STRING,
                text STRING,
                published TIMESTAMP,
                timestamp_id STRING,
                author STRING,
                source STRING,
                year INT,
                month INT,
                day INT,
                hour INT
            )
            USING ICEBERG
            PARTITIONED BY (id_topic, year, month)
            LOCATION 's3a://gold/news/'
        """)

    spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.dim_comments (
                coin_id INT,
                id_topic INT,
                created_utc TIMESTAMP,
                timestamp_id STRING,
                title STRING,
                id_author STRING,
                score FLOAT,
                url STRING,
                id STRING,
                self_text STRING,
                num_comments INT,
                year INT,
                month INT,
                day INT,
                hour INT
            )
            USING ICEBERG
            PARTITIONED BY (id_author, id_topic, year, month)
            LOCATION 's3a://gold/comments/'
        """)

    spark.sql("""

            CREATE TABLE IF NOT EXISTS gold.dim_realtime (
                realtime_id INT,
                coin_id INT,
                interval TIMESTAMP,
                starttime TIMESTAMP,
                starttime_id STRING,
                endtime STRING,
                endtime_id  STRING,
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
            CREATE TABLE IF NOT EXISTS gold.author_credit (
                id_author INT,
                author STRING,
                credit_score FLOAT
            )
            USING ICEBERG
            LOCATION 's3a://gold/author_credit/'
        """)

    spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.dim_coin (
                coin_id INT,
                symbol STRING,
                name STRING
            )
            USING ICEBERG
            LOCATION 's3a://gold/dim_coin/'
        """)

    spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.dim_topic (
                id_topic INT,
                topic STRING
            )
            USING ICEBERG
            LOCATION 's3a://gold/dim_topic/'
        """)
    
    create_dim_time()
    create_dim_coin()
    create_dim_topic()
    print("xong")