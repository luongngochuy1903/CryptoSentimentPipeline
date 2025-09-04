import sys, os
sys.path.append("/opt/spark_jobs/consumer")
from schema_registry_consumer import SchemaRegistryConsumer
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
        "created_utc": {"type":"number"},
        "id": {"type":"string"},
        "selftext": {"type":"string"},
        "num_comments": {"type":"integer"},
        "tag": {"type":"string"}
    },
    "required": ["subreddit","title", "author", "score", "url", "created_utc", "id", "selftext", "num_comments", "tag"]
}
"""

consumer = SchemaRegistryConsumer("comments_raw", comments_schema, "CommentsCon", "comments-group-stage2")
consumer.polling("Comments-to-Silver", "comments")

consumer = SchemaRegistryConsumer("news_raw", news_schema, "NewsCon", "news-group-stage2")
consumer.polling("News-to-Silver", "news")
