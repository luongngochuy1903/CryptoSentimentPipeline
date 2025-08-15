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
        "RSI_sen": {"type": "string"},
        "MACD_sen": {"type": "string"},
        "EMA_sen": {"type": "string"},
        "bb_sen": {"type": "string"},
        "SMA_sen": {"type": "string"},
        "ATR_sen": {"type": "string"}
    },
    "required": [
        "id", "realtime_id", "endtime", "RSI_sen", "MACD_sen", "EMA_sen", "bb_sen", "SMA_sen", "ATR_sen"]
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
consumer.polling("Statistic-to-Silver", "statistic")


consumer = SchemaRegistryConsumer("sentiment_raw", sentiment_schema, "SenCon", "sentiment-group-stage2")
consumer.polling("Sentiment-to-Silver", "sentiment")