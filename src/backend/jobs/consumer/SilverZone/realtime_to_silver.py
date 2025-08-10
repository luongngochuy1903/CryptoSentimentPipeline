from schema_registry_consumer import SchemaRegistryConsumer

realtime_schema = """
{
    "$schema":"http://json-schema.org/draft-07/schema#",
    "title":"News",
    "type":"object",
    "properties": {
        "realtime_id":{"type":"number"},
        "symbol": {"type":"string"},
        "name": {"type":"string"},
        "interval": {"type":"string"},
        "starttime": {"type":"string"},
        "endtime": {"type":"string"},
        "volume": {"type":"number"},
        "quotevolume": {"type":"number"},
        "open": {"type":"number"},
        "close": {"type":"number"},
        "highest": {"type":"number"},
        "lowest": {"type":"number"},
        "tag": {"type":"string"}
    },
    "required": ["realtime_id", "symbol", "name", "interval", "starttime", "endtime", "volume", "quotevolume", "open", "close", "highest", "lowest", "tag"]
}
"""

consumer = SchemaRegistryConsumer("realtime_raw", realtime_schema, "RealtimeCon", "realtime-group-stage2")
consumer.polling("Realtime-to-Silver", "realtime")
