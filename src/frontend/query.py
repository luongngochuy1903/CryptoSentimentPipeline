query_get_coin_data="""
        WITH latest_events AS (
            SELECT * FROM event
                WHERE symbol = %s
                ORDER BY endtime DESC
                LIMIT 200
            ),

            price_24h AS (
                SELECT close
                    FROM event 
                    WHERE symbol = %s
                    AND endtime = (SELECT endtime FROM latest_events LIMIT 1) - interval '24 hours'
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
        INSERT INTO technical(event_id, symbol, sma20, ema12, rsi_10, macd, bb, atr, vah, val, POC)
        VALUES %s
"""
#----------------------------------------------
query_get_24h_min_max = """
        WITH new_event AS (
    SELECT *
        FROM event
        WHERE symbol = %s 
        ORDER BY endtime
        LIMIT 1
        ),
        upsert_max AS (
        INSERT INTO event_max_cache (symbol, max_value, check_count_max)
                SELECT symbol, close, 1
                        FROM new_event ne
                        WHERE NOT EXISTS (
                                SELECT 1 FROM event_max_cache ec2 WHERE ec2.symbol = ne.symbol
                )
                RETURNING event_max_cache.symbol, event_max_cache.max_value
        ),

        update_max AS (
        UPDATE event_max_cache ec
        SET max_value = CASE
                WHEN ne.close > ec.max_value THEN ne.close
                WHEN ec.check_count_max >=86400 THEN ne.close
                ELSE ec.max_value
        END,
            check_count_max = CASE
                WHEN ne.close > ec.max_value THEN 1
                WHEN ec.check_count_max >=86400 THEN 1
                ELSE ec.check_count_max + 1
        END
                FROM new_event ne
                        WHERE ne.symbol = ec.symbol AND NOT EXISTS (
                        SELECT 1 from upsert_max um WHERE um.symbol = ec.symbol 
                        )
                RETURNING ec.symbol, ec.max_value
        )

        SELECT * FROM upsert_max 
        UNION ALL 
        SELECT * FROM update_max
    """

#----------------------------------------------
query_get_news = """
        SELECT title, published, url from news
"""