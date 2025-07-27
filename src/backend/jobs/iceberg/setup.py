from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Iceberg Init") \
    .getOrCreate()

spark.sql("USE iceberg.default;")
spark.sql("SHOW CATALOGS").show()

spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.silver")

spark.sql("""

        CREATE TABLE IF NOT EXISTS iceberg.silver.news (
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

        CREATE TABLE IF NOT EXISTS iceberg.silver.comments (
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

        CREATE TABLE IF NOT EXISTS iceberg.silver.realtime (
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
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day, hour)
        LOCATION 's3a://silver/realtime/'
    """)

spark.sql("CREATE DATABASE IF NOT EXIST iceberg.gold")
spark.sql("""CREATE TABLE IF NOT EXIST iceberg.gold.statistic
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

spark.sql("""CREATE TABLE IF NOT EXIST iceberg.gold.sentiment
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

spark.sql("""CREATE TABLE IF NOT EXIST iceberg.gold.dim_time
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

        CREATE TABLE IF NOT EXISTS iceberg.silver.news (
            id INT,
            id_topic INT,
            domain STRING,
            title STRING,
            url STRING,
            text STRING,
            published TIMESTAMP,
            author STRING,
            source STRING,
        )
        USING ICEBERG
        PARTITIONED BY (id_topic)
        LOCATION 's3a://gold/news/'
    """)

print("xong")