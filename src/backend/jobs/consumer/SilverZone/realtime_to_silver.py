from consumer.schema_registry_consumer import SchemaRegistryConsumer

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

def run_realtime_consumer():
    consumer = SchemaRegistryConsumer("realtime_raw", realtime_schema, "NewsCon", "realtime-group-stage2")
    consumer.polling("Realtime-to-Silver", "realtime")
