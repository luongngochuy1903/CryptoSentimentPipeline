from schema_registry_consumer import SchemaRegistryConsumer

sentiment_schema="""
    {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Sentiment",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "realtime_id": {"type": "integer"},
        "endtime": {"type": "string"},
        "rsi_sen": {"type": "string"},
        "macd_sen": {"type": "string"},
        "ema_sen": {"type": "string"},
        "bb_sen": {"type": "string"},
        "sma_sen": {"type": "string"},
        "atr_sen": {"type": "string"}
    },
    "required": [
        "id", "realtime_id", "endtime", "rsi_sen", "macd_sen", "ema_sen", "bb_sen", "sma_sen", "atr_sen"]
    }
    """
statistic_schema="""
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Technical",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "realtime_id": {"type": "integer"},
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
        "poc": {"type": "number"}
    },
    "required": [
        "id", "realtime_id", "endtime", "symbol",
        "sma20", "ema12", "rsi10", "macd", "bb",
        "atr", "va_high", "va_low", "poc"
    ]
}
    """
consumer = SchemaRegistryConsumer("statistic_raw", statistic_schema, "StaCon", "statistic-group-stage2")
consumer.polling("Statistic-to-Silver", "technical")


consumer = SchemaRegistryConsumer("sentiment_raw", sentiment_schema, "SenCon", "sentiment-group-stage2")
consumer.polling("Sentiment-to-Silver", "sentiment")