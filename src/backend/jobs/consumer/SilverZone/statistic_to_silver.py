from schema_registry_consumer import SchemaRegistryConsumer

statistic_schema={
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Technical",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "event_id": {"type": "integer"},
        "endtime": {"type": "string"},
        "symbol": {"type": "string"},
        "sma20": {"type": "number"},
        "ema12": {"type": "number"},
        "rsi10": {"type": "number"},
        "macd": {"type": "number"},
        "bb": {"type": "number"},
        "atr": {"type": "number"},
        "va_high": {"type": "number"},
        "va_low": {"type": "number"},
        "POC": {"type": "number"}
    },
    "required": [
        "id", "event_id", "endtime", "symbol",
        "sma20", "ema12", "rsi10", "macd", "bb",
        "atr", "va_high", "va_low", "POC"
    ]
}

consumer = SchemaRegistryConsumer("statistic_raw", statistic_schema, "StaCon", "statistic-group-stage2")
consumer.polling("Statistic-to-Silver", "statistic")