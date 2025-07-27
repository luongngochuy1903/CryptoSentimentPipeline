from consumer.schema_registry_consumer import SchemaRegistryConsumer
from pyspark.sql import SparkSession

if __name__ == "__main__":
    comments_schema = """
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
    
    realtime_schema = """
        {
            "$schema":"http://json-schema.org/draft-07/schema#",
            "title":"News",
            "type":"object",
            "properties": {
                "symbol": {"type":"string"},
                "name": {"type":"string"},
                "interval": {"type":"string"},
                "starttime": {"type":"string", "format":"date-time"},
                "endtime": {"type":"string", "format":"date-time"},
                "volume": {"type":"number"},
                "quotevolume": {"type":"number"},
                "open": {"type":"number"},
                "close": {"type":"number"},
                "highest": {"type":"number"},
                "lowest": {"type":"number"},
                "tag": {"type":"string"}
            },
            "required": ["symbol", "name", "interval", "starttime", "endtime", "volume", "quotevolume", "open", "close", "highest", "lowest", "tag"]
        }
        """

    news_schema = """
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
    newsConsumer = SchemaRegistryConsumer("news_raw", news_schema, "NewsCon", "news-group-stage2")
    newsConsumer.polling("News-to-Silver", "news")

    commentsConsumer = SchemaRegistryConsumer("comments_raw", comments_schema, "CommentsCon", "comments-group-stage2")
    commentsConsumer.polling("Comments-to-Silver", "comments")

    newsConsumer = SchemaRegistryConsumer("realtime_raw", realtime_schema, "NewsCon", "realtime-group-stage2")
    newsConsumer.polling("Realtime-to-Silver", "realtime")