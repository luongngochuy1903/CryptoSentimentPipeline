from consumer.schema_registry_consumer import SchemaRegistryConsumer

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

def run_news_consumer():
    consumer = SchemaRegistryConsumer("news_raw", news_schema, "NewsCon", "news-group-stage2")
    consumer.polling("News-to-Silver", "news")
