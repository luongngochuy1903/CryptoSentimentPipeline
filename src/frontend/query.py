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
# ...existing code...
from pathlib import Path
import glob, re
import pandas as pd
from datetime import datetime, timedelta, timezone as dt_tz

_TICKER_TO_SYMBOL = {"BTC":"BTCUSDT","ETH":"ETHUSDT","BNB":"BNBUSDT","SOL":"SOLUSDT","XRP":"XRPUSDT"}

def _fallback_texts_repo_root(symbol: str) -> pd.DataFrame:
    root = Path(__file__).resolve().parents[2]
    files = []
    for pat in ["*_news_gnews.jsonl", "rss_*_filtered_*.jsonl", "reddit_crypto_cmt.jsonl", "coin_news_debug.json"]:
        files.extend([str(p) for p in root.glob(pat)])
    frames = []
    for f in files:
        try:
            if f.endswith(".jsonl"):
                df = pd.read_json(f, lines=True)
            else:
                df = pd.read_json(f)
        except Exception:
            continue
        # text
        if "text" in df.columns:
            txt = df["text"].astype(str)
        elif "title" in df.columns and "description" in df.columns:
            txt = df["title"].astype(str) + " " + df["description"].astype(str)
        elif "content" in df.columns:
            txt = df["content"].astype(str)
        elif "body" in df.columns:
            txt = df["body"].astype(str)
        else:
            continue
        # ts
        for col in ["created_at","published","publishedAt","published_at","time","timestamp","date","created_utc"]:
            if col in df.columns:
                ts = pd.to_datetime(df[col], errors="coerce", utc=True)
                break
        else:
            continue
        # sym by column, filename, then text
        sym = None
        for col in ["symbol","pair","coin","ticker"]:
            if col in df.columns:
                sym = df[col].astype(str).str.upper().replace(_TICKER_TO_SYMBOL); break
        if sym is None:
            name = Path(f).name.lower()
            file_sym = None
            if "bitcoin" in name or "_btc" in name: file_sym = "BTCUSDT"
            elif "ethereum" in name or "_eth" in name: file_sym = "ETHUSDT"
            elif "solana" in name or "_sol" in name: file_sym = "SOLUSDT"
            elif "bnb" in name: file_sym = "BNBUSDT"
            elif "xrp" in name: file_sym = "XRPUSDT"
            if file_sym:
                sym = pd.Series([file_sym] * len(df))
        if sym is None:
            def infer(s):
                s = str(s).upper()
                for k, v in _TICKER_TO_SYMBOL.items():
                    if re.search(rf"(\$|\b){k}\b", s):
                        return v
                return None
            sym = txt.apply(infer)
        frames.append(pd.DataFrame({"text": txt, "created_at": ts, "symbol": sym}))
    if not frames:
        return pd.DataFrame(columns=["text","created_at","symbol"])
    return pd.concat(frames, ignore_index=True)

def query_get_texts_for_coin_24h(symbol: str, silver_base: str | None = None) -> pd.DataFrame:
    # ...existing code that reads silver/news and comments...
    df = pd.concat([read_folder("news"), read_folder("comments")], ignore_index=True)
    df = df.dropna(subset=["text","created_at","symbol"])
    end = datetime.now(dt_tz.utc); start = end - timedelta(days=1)
    df = df[(df["created_at"] >= start) & (df["created_at"] <= end) & (df["symbol"] == symbol)]

    if df.empty:
        alt_df = _fallback_texts_repo_root(symbol)
        alt_df = alt_df.dropna(subset=["text","created_at","symbol"])
        alt_df = alt_df[(alt_df["created_at"] >= start) & (alt_df["created_at"] <= end) & (alt_df["symbol"] == symbol)]
        return alt_df.reset_index(drop=True)

    return df.reset_index(drop=True)