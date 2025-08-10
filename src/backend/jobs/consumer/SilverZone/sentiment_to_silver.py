from schema_registry_consumer import SchemaRegistryConsumer

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

consumer = SchemaRegistryConsumer("sentiment_raw", sentiment_schema, "SenCon", "sentiment-group-stage2")
consumer.polling("Sentiment-to-Silver", "sentiment")