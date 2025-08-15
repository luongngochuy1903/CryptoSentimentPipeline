import logging, os, sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from module.modules import get_latest_partition_datetime
from pyspark.sql.functions import col, to_timestamp, cast
from producer.raw_to_kafka import SchemaRegistryObj

class StatisticSentimentQualityProducer(SchemaRegistryObj):
    def data_quality(self):
        self.logger.info("------------------STATISTIC AND SENTIMENT COIN DATA QUALITY------------------")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Data quality check") \
            .appName("Read from MinIO via s3a") \
            .config("spark.cores.max", "1") \
            .config("spark.executor.cores", "1") \
            .config("spark.executor.memory", "1G") \
            .getOrCreate()
        sta_latest = get_latest_partition_datetime("raw", "statistic")
        sen_latest = get_latest_partition_datetime("raw", "sentiment")
        stadf = spark.read.json(f"s3a://raw/statistic/{sta_latest.year}/{sta_latest.month:02}/{sta_latest.day:02}/{sta_latest.hour:02}")
        sendf = spark.read.json(f"s3a://raw/sentiment/{sen_latest.year}/{sen_latest.month:02}/{sen_latest.day:02}/{sen_latest.hour:02}")
        print(stadf.head(4))
        self.logger.info(stadf.schema.simpleString())
        self.logger.info(sendf.schema.simpleString())

        #changing name
        stadf = stadf.withColumnRenamed("event_id", "realtime_id")
        sendf = sendf.withColumnRenamed("event_id", "realtime_id")

        # #checking date
        # stadf = stadf.withColumn("endtime", to_timestamp(col("endtime"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        # sendf = sendf.withColumn("endtime", to_timestamp(col("endtime"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

        #Checking null
        stadf = stadf.dropna(how="any")
        sendf = sendf.dropna(how="any")

        #Data Types
        sta_col = [ "id", "realtime_id", "sma20", "ema12", "rsi10", "macd", "bb",
            "atr", "va_high", "va_low", "poc"
        ]

        for c in sta_col:
            stadf = stadf.withColumn(c, col(c).cast("double"))

        numeric_cols_sentiment = ["id", "realtime_id"]
        for c in numeric_cols_sentiment:
            sendf = sendf.withColumn(c, col(c).cast("double"))
        
        #Checking datatypes
        for column, dtype in stadf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")
        for column, dtype in sendf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")
        
        return stadf, sendf

def main():
    statistic_schema="""
    {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Technical",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "realtime_id": {"type": "integer"},
        "endtime": {"type": "string"},
        "symbol": {"type": "string"},
        "sma20": {"type": "number"},
        "ema12": {"type": "number"},
        "rsi10": {"type": "number"},
        "macd": {"type": "number"},
        "bb": {"type": "number"},
        "atr": {"type": "number"},
        "va_high": {"type": "number"},
        "va_low": {"type": "number"},
        "poc": {"type": "number"}
    },
    "required": [
        "id", "realtime_id", "endtime", "symbol",
        "sma20", "ema12", "rsi10", "macd", "bb",
        "atr", "va_high", "va_low", "poc"
    ]
    }
"""
    
    sentiment_schema="""
    {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Sentiment",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "realtime_id": {"type": "integer"},
        "endtime": {"type": "string"},
        "RSI_sen": {"type": "string"},
        "MACD_sen": {"type": "string"},
        "EMA_sen": {"type": "string"},
        "bb_sen": {"type": "string"},
        "SMA_sen": {"type": "string"},
        "ATR_sen": {"type": "string"}
    },
    "required": [
        "id", "realtime_id", "endtime", "RSI_sen", "MACD_sen", "EMA_sen", "bb_sen", "SMA_sen", "ATR_sen"]
    }
    """

    staobj = StatisticSentimentQualityProducer(statistic_schema, "statistic_raw", "statistic_data")
    stadf, sendf = staobj.data_quality()
    staobj.setCompatibility("BACKWARD")
    staobj.produces(stadf)

    senobj = StatisticSentimentQualityProducer(sentiment_schema, "sentiment_raw", "sentiment_data")
    senobj.setCompatibility("BACKWARD")
    senobj.produces(sendf)
    print("Xong")

if __name__ == "__main__":
    main()