from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Data quality check") \
    .appName("Read from MinIO via s3a") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1G") \
    .getOrCreate()

result_df = spark.sql("SELECT max(hour) FROM silver.news")
result_df.show()