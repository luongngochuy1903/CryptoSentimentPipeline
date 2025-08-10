import logging, os, sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from module.modules import get_latest_partition_datetime
from producer.raw_to_kafka import SchemaRegistryObj

class NewsQualityProducer(SchemaRegistryObj):
    def data_quality(self):
        self.logger.info("------------------NEWS DATA QUALITY------------------")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Read from MinIO via s3a") \
            .config("spark.cores.max", "1") \
            .config("spark.executor.cores", "1") \
            .getOrCreate()
        latest = get_latest_partition_datetime("raw", "news")
        print(latest)
        newsdf = spark.read.json(f"s3a://raw/news/{latest.year}/{latest.month:02}/{latest.day:02}/{latest.hour:02}/")
        self.logger.info(newsdf.schema.simpleString())
        newsdf.printSchema()

        #Checking null
        from pyspark.sql.functions import col
        null_text = newsdf.filter((col("text").isNotNull()) & (col("text") != "") & (col("published").isNotNull()) & (col("published") != ""))
        num_of_null_text = null_text.count()
        self.logger.info(f"Text columns WHICH IS NULL:{newsdf.count() - num_of_null_text}")
        newsdf = null_text

        #Checking row count
        num_of_row = newsdf.count()
        self.logger.info(f"ROW COUNT:{num_of_row}")

        #Checking duplicated news
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        spec = Window.partitionBy("url").orderBy("title")
        newsdf_with_duplicated = newsdf.withColumn("duplicated_url", row_number().over(spec))
        record_duplicated = newsdf_with_duplicated.filter(col("duplicated_url") > 1).select(col('url'), col('title'))
        record_duplicated_select = record_duplicated.collect()
        num_record_duplicated = record_duplicated.count()
        self.logger.info(f"NUMBER of DUPLICATED NEWS: {num_record_duplicated}")
        self.logger.info("DUPLICATED NEWS:")
        for row in record_duplicated_select:
            self.logger.info(f"{str(row['url'])} - {str(row['title'])}")

        #Dropping duplicated url
        newsdf = newsdf_with_duplicated.filter(col("duplicated_url") == 1).drop("duplicated_url")

        #Checking distinct value
        for column in newsdf.columns:
            n_unique = newsdf.select(col(column)).distinct().count()
            self.logger.info(f"UNIQUE VALUE of {column}: {n_unique}")

        #checking date
        from pyspark.sql.functions import to_timestamp, to_date, min, max
            
        newsdf = newsdf.withColumn("published_ts", to_timestamp("published", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        newsdf.show(20)
        min_date_row = newsdf.agg(min(col("published_ts")).alias("min_date")).collect()[0]
        min_date = min_date_row["min_date"]
        self.logger.info(f"MIN DATE: {min_date}")

        max_date_row = newsdf.agg(max(col("published_ts")).alias("max_date")).collect()[0]
        max_date = max_date_row["max_date"]
        self.logger.info(f"MAX DATE: {max_date}")

        #Checking datatypes
        for column, dtype in newsdf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")

        #Eliminate old published from nearest scraped timestamp to now
        latest_update = get_latest_partition_datetime("silver", "news")

        today = datetime.today()
        print(today)
        if latest_update:
            newsdf = newsdf.filter((col('published_ts') > latest_update) & (col('published_ts') <= today))
        newsdf = newsdf.drop("published_ts")
        newsdf = newsdf.withColumnRenamed("authors", "author")
        sumcount = newsdf.count()
        self.logger.info(f"SUM OF RECORD after filtering TIMESTAMP: {sumcount}")

        # Build model reflecting which content is related to domain topics.

        return newsdf


def main():
    schema = """
        {
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"News",
            "type":"object",
            "properties": {
                "domain": {"type":"string"},
                "title": {"type":"string"},
                "url": {"type":"string"},
                "text": {"type":"string"},
                "published": { "type": "string", "format": "date-time"},
                "author": {"type":["string", "null"]},
                "source": {"type":"string"},
                "tag": {"type":"string"}
            },
            "required": ["domain","title", "url", "published", "text", "source", "tag"]
        }
        """

    obj = NewsQualityProducer(schema, "news_raw", "News")

    obj.setCompatibility("BACKWARD")
    df = obj.data_quality()
    obj.produces(df)
    print("Xong")

if __name__ == "__main__":
    main()