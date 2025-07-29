from consumer.schema_registry_consumer import SchemaRegistryConsumer

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

def run_comments_consumer():
    consumer = SchemaRegistryConsumer("comments_raw", comments_schema, "CommentsCon", "comments-group-stage2")
    consumer.polling("Comments-to-Silver", "comments")
