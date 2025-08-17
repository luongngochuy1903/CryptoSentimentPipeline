query_get_coin_data="""
        WITH latest_events AS (
            SELECT symbol, endtime, close FROM event
                WHERE symbol = %s
                ORDER BY endtime DESC
                LIMIT 350
            ),

            price_24h AS (
                SELECT close
                    FROM event 
                    WHERE symbol = %s
                    AND endtime = (SELECT endtime FROM latest_events ORDER BY endtime DESC LIMIT 1) - interval '24 hours'
            )
        SELECT e.close, e.endtime, ROUND((e.close - p.close), 2) AS price_diff,
                ROUND(((e.close - p.close) / p.close) * 100, 2) AS percent_change
                FROM latest_events e
                LEFT JOIN price_24h p ON true
        """
#-------------------------------
query_get_raw_statistic="""
        SELECT * FROM event
                WHERE symbol = %s
                ORDER BY endtime DESC
                LIMIT 26
        """
query_load_back_to_db = """
        INSERT INTO technical(event_id, endtime, symbol, sma20, ema12, rsi10, macd, bb, atr, va_high, va_low, poc)
        VALUES %s
"""
#----------------------------------------------
query_get_24h_min_max = """
                WITH new_event AS (
        SELECT *
        FROM event
        WHERE symbol = %s
        ORDER BY endtime DESC
        LIMIT 1
        ),
        upsert AS (
        INSERT INTO event_max_cache(symbol, max_value, max_timestamp)
        SELECT ne.symbol, ne.close, ne.endtime
        FROM new_event ne
        WHERE NOT EXISTS (
                SELECT 1 FROM event_max_cache ec WHERE ec.symbol = ne.symbol
        )
        RETURNING symbol, max_value, max_timestamp
        ),
        update_cache AS (
        UPDATE event_max_cache ec
        SET max_value = CASE
                WHEN EXTRACT(EPOCH FROM (ne.endtime - ec.max_timestamp)) >= 86400
                THEN ne.close
                WHEN ne.close > ec.max_value
                THEN ne.close
                ELSE ec.max_value
        END,
        max_timestamp = CASE
                WHEN EXTRACT(EPOCH FROM (ne.endtime - ec.max_timestamp)) >= 86400
                THEN ne.endtime
                WHEN ne.close > ec.max_value
                THEN ne.endtime
                ELSE ec.max_timestamp
        END
        FROM new_event ne
        WHERE ec.symbol = ne.symbol
        AND NOT EXISTS (SELECT 1 FROM upsert u WHERE u.symbol = ec.symbol)
        RETURNING ec.symbol, ec.max_value, ec.max_timestamp
        )
        SELECT * FROM upsert
        UNION ALL
        SELECT * FROM update_cache;

    """

#----------------------------------------------
query_get_news = """
        SELECT title, published, url from news
"""
#----------------------------------------------
query_load_back_to_db_sen = """
        INSERT INTO sentiment(event_id, endtime, rsi_sen, macd_sen, ema_sen, bb_sen, sma_sen, atr_sen)
        VALUES %s
"""