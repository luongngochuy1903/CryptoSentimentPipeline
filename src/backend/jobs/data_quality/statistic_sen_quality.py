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
            .getOrCreate()
        sta_latest = get_latest_partition_datetime("raw", "statistic")
        sen_latest = get_latest_partition_datetime("raw", "sentiment")
        stadf = spark.read.json(f"s3a://raw/statistic/{sta_latest.year}/{sta_latest.month:02}/{sta_latest.day:02}/{sta_latest.hour:02}")
        sendf = spark.read.json(f"s3a://raw/sentiment/{sen_latest.year}/{sen_latest.month:02}/{sen_latest.day:02}/{sen_latest.hour:02}")
        self.logger.info(stadf.schema.simpleString())
        self.logger.info(sendf.schema.simpleString())

        #checking date
        stadf = stadf.withColumn("starttime", to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss"))
        stadf = stadf.withColumn("endtime", to_timestamp(col("endtime"), "yyyy-MM-dd HH:mm:ss"))

        sendf = sendf.withColumn("starttime", to_timestamp(col("starttime"), "yyyy-MM-dd HH:mm:ss"))
        sendf = sendf.withColumn("endtime", to_timestamp(col("endtime"), "yyyy-MM-dd HH:mm:ss"))

        #Checking null
        stadf = stadf.dropna(how="any")
        sendf = sendf.dropna(how="any")

        #Data Types
        sta_col = [ "id", "event_id", "sma20", "ema12", "rsi10", "macd", "bb",
            "atr", "va_high", "va_low", "POC"
        ]

        for c in sta_col:
            stadf = stadf.withColumn(c, col(c).cast("double"))

        numeric_cols_sentiment = ["id", "event_id"]
        for c in numeric_cols_sentiment:
            sendf = sendf.withColumn(c, col(c).cast("double"))
        
        #Checking datatypes
        for column, dtype in stadf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")
        for column, dtype in sendf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")
        
        return stadf, sendf

def main():
    statistic_schema={
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Technical",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "event_id": {"type": "integer"},
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
        "POC": {"type": "number"}
    },
    "required": [
        "id", "event_id", "endtime", "symbol",
        "sma20", "ema12", "rsi10", "macd", "bb",
        "atr", "va_high", "va_low", "POC"
    ]
}
    
    sentiment_schema={
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Sentiment",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "event_id": {"type": "integer"},
        "endtime": {"type": "string"},
        "RSI_sen": {"type": "string"},
        "MACD_sen": {"type": "string"},
        "EMA_sen": {"type": "string"},
        "bb_sen": {"type": "string"},
        "SMA_sen": {"type": "string"},
        "ATR_sen": {"type": "string"}
    },
    "required": [
        "id", "event_id", "endtime", "RSI_sen", "MACD_sen", "EMA_sen", "bb_sen", "SMA_sen", "ATR_sen"]
}

    staobj = StatisticSentimentQualityProducer(statistic_schema, "statistic", "statistic_data")
    stadf, sendf = staobj.data_quality()
    staobj.setCompatibility("BACKWARD")
    staobj.produces(stadf)

    senobj = StatisticSentimentQualityProducer(sentiment_schema, "sentiment", "sentiment_data")
    senobj.setCompatibility("BACKWARD")
    senobj.produces(sendf)
    print("Xong")

if __name__ == "__main__":
    main()