import logging, os, sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from module.modules import get_latest_partition_datetime
from producer.raw_to_kafka import SchemaRegistryObj

class CommentsQualityProducer(SchemaRegistryObj):
    def data_quality(self):
        self.logger.info("------------------COMMENTS DATA QUALITY------------------")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Read from MinIO via s3a") \
            .getOrCreate()
        latest = get_latest_partition_datetime("raw", "news")
        commentsdf = spark.read.json(f"s3a://raw/comments/{latest.year}/{latest.month}/{latest.day}/{latest.hour}/")
        self.logger.info(commentsdf.schema.simpleString())
        commentsdf.printSchema()

        #Checking null
        from pyspark.sql.functions import col, count
        num_of_null_title = commentsdf.filter((col("title").isNull()) | (col("title") == "")).count()
        self.logger.info(f"Title columns WHICH IS NULL:{num_of_null_title}")

        num_of_null_url = commentsdf.filter(col("url").isNull()).count()
        self.logger.info(f"Url columns WHICH IS NULL:{num_of_null_url}")

        num_of_null_id = commentsdf.filter(col("id").isNull()).count()
        self.logger.info(f"ID columns WHICH IS NULL:{num_of_null_id}")

        null_text = commentsdf.filter((col("text").isNotNull()) & (col("text") != ""))
        num_of_null_text = null_text.count()
        self.logger.info(f"Selftext columns WHICH IS NULL:{commentsdf.count() - num_of_null_text}")
        commentsdf = null_text

        num_of_null_score = commentsdf.filter(col("score").isNull()).count()
        self.logger.info(f"Score columns WHICH IS NULL:{num_of_null_score}")

        #Checking row count
        num_of_row = commentsdf.count()
        self.logger.info(f"ROW COUNT:{num_of_row}")

        #Checking duplicated news
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        spec = Window.partitionBy("id").orderBy("id")
        commentsdf_with_duplicated = commentsdf.withColumn("duplicated_id", row_number().over(spec))
        record_duplicated = commentsdf_with_duplicated.filter(col("duplicated_id") > 1).select(col('id'), col('title'))
        record_duplicated_select = record_duplicated.collect()
        num_record_duplicated = record_duplicated.count()
        self.logger.info(f"NUMBER of DUPLICATED COMMENTS: {num_record_duplicated}")
        self.logger.info("DUPLICATED COMMENTS:")
        for row in record_duplicated_select:
            self.logger.info(f"{str(row['id'])} - {str(row['title'])}")

        #Dropping duplicated url
        commentsdf = commentsdf_with_duplicated.filter(col("duplicated_id") == 1).drop("duplicated_id")

        #Checking distinct value
        for column in commentsdf.columns:
            n_unique = commentsdf.select(col(column)).distinct().count()
            self.logger.info(f"UNIQUE VALUE of {column}: {n_unique}")

        #Checking score, num_comments
        commentsdf = commentsdf.withColumn("score", col("score").cast("double"))
        commentsdf = commentsdf.withColumn("num_comments", col("num_comments").cast("int"))

        #checking date
        from pyspark.sql.functions import to_timestamp, to_date, min, max
            
        commentsdf = commentsdf.withColumn("created_utc", to_timestamp(col("created_utc")))
        commentsdf.show(20)
        min_date_row = commentsdf.agg(min(to_date(col("created_utc"))).alias("min_date")).collect()[0]
        min_date = min_date_row["min_date"]
        self.logger.info(f"MIN DATE: {min_date}")

        max_date_row = commentsdf.agg(max(to_date(col("created_utc"))).alias("max_date")).collect()[0]
        max_date = max_date_row["max_date"]
        self.logger.info(f"MAX DATE: {max_date}")

        #Checking datatypes
        for column, dtype in commentsdf.dtypes:
            self.logger.info(f"DATATYPE Column {column}: {dtype}")

        #Eliminate old published from nearest scraped timestamp to now
        latest_update = get_latest_partition_datetime("raw", "comments")
        today = datetime.today()
        print(today)

        commentsdf = commentsdf.filter((col('created_utc') > latest_update) & (col('created_utc') <= today))
        sumcount = commentsdf.count()
        self.logger.info(f"SUM OF RECORD after filtering TIMESTAMP: {sumcount}")

        # Build model reflecting which content is related to domain topics.

        return commentsdf

def main():
    schema = """
        {
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"News",
            "type":"object",
            "properties": {
                "subreddit": {"type":"string"},
                "title": {"type":"string"},
                "author": {"type":"string"},
                "score": {"type":"number"},
                "url": {"type":"string"},
                "created_utc": {"type":"string", "format":"date-time"},
                "id": {"type":"string"},
                "self_text": {"type":"string"},
                "num_comments": {"type":"integer"},
                "tag": {"type":"string"}
            },
            "required": ["subreddit","title", "author", "score", "url", "created_utc", "id", "self_text", "num_comments", "tag"]
        }
        """

    obj = CommentsQualityProducer(schema, "comments_raw", "Comments")
    obj.setCompatibility("BACKWARD")
    df = obj.data_quality()
    obj.produces(df)
    print("Xong")

if __name__ == "__main__":
    main()