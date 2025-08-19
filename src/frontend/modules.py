import streamlit as st
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
import os
from pathlib import Path
import numpy as np
import altair as alt
import pandas as pd
import sys, os
sys.path.append("/app")
from utils.constants import POSTGRES_USERNAME, POSTGRES_PASSWORD

def run_pool():
    return SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host="postgres",
        database="backend",
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        port="5432"
    )

def label_detach(df, query):
    pool = run_pool()
    conn = pool.getconn()
    conn.autocommit = True
    cursor = None
    def label_rsi(rsi):
        if rsi > 80:
            return "Extremely overbought"
        elif rsi > 70:
            return "Overbought"
        elif rsi >= 40:
            return "Neutral"
        elif rsi >= 20:
            return "Oversold"
        else:
            return "Extremely oversold"

    def label_macd(macd):
        if macd > 2:
            return "Strong bullish crossover"
        elif macd > 0:
            return "Mild bullish crossover"
        elif macd == 0:
            return "Neutral"
        elif macd >= -2:
            return "Mild bearish crossover"
        else:
            return "Strong bearish crossover"

    def label_ema(ema):
        if ema > 2:
            return "Sharp upward momentum"
        elif ema > 0:
            return "Upward momentum"
        elif ema == 0:
            return "Neutral momentum"
        elif ema >= -2:
            return "Downward momentum"
        else:
            return "Sharp downward momentum"

    def label_bb(bb):
        if bb > 2:
            return "Above upper band"
        elif bb > 1:
            return "Near upper band"
        elif bb >= -1:
            return "Mid-band"
        elif bb >= -2:
            return "Near lower band"
        else:
            return "Below lower band"

    def label_sma(sma):
        if sma > 2:
            return "Strongly above SMA"
        elif sma > 0:
            return "Moderately above SMA"
        elif sma == 0:
            return "Around SMA"
        elif sma >= -2:
            return "Moderately below SMA"
        else:
            return "Strongly below SMA"

    def label_atr(atr):
        if atr > 5:
            return "Very high volatility"
        elif atr > 3:
            return "High volatility"
        elif atr > 1:
            return "Moderate volatility"
        elif atr > 0:
            return "Low volatility"
        else:
            return "Very low volatility"

    try:
        sentiment_df = pd.DataFrame()
        sentiment_df["event_id"] = df["event_id"]
        sentiment_df["endtime"] = df["endtime"]

        sentiment_df["rsi_sen"] = df["rsi10"].apply(label_rsi)
        sentiment_df["macd_sen"] = df["macd"].apply(label_macd)
        sentiment_df["ema_sen"] = df["ema12"].apply(label_ema)
        sentiment_df["bb_sen"] = df["bb"].apply(label_bb)
        sentiment_df["sma_sen"] = df["sma20"].apply(label_sma)
        sentiment_df["atr_sen"] = df["atr"].apply(label_atr)

        values = sentiment_df.values.tolist()
        latest_row = sentiment_df.iloc[[-1]]
        cursor = conn.cursor()
        execute_values(cursor, query, values)

        return latest_row
    except Exception as e:
        print(f"Raise error when calculate sentiment: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        conn.commit()
        pool.putconn(conn)

def calculate_technical(symbol, query, query_load):
    pool = run_pool()
    conn = pool.getconn()
    conn.autocommit = True
    cursor = None
    try:
        df = pd.read_sql(query, conn, params=(symbol,))
        df = df.sort_values("endtime", ascending=True).reset_index(drop=True)
        df_statistic = pd.DataFrame(index=df.index)

        high = df["highest"]
        low = df["lowest"]
        close = df["close"]

        # ATR 14
        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df_statistic["atr"] = tr.rolling(window=14, min_periods=1).mean()

        # SMA 20
        df_statistic["sma20"] = close.rolling(window=20, min_periods=1).mean()

        # EMA 12
        df_statistic["ema12"] = close.ewm(span=12, adjust=False).mean()

        # MACD
        ema_26 = close.ewm(span=26, adjust=False).mean()
        df_statistic["macd"] = df_statistic["ema12"] - ema_26

        # Bollinger Bands (BB)
        std_20 = close.rolling(window=20, min_periods=1).std()
        df_statistic["bb_upper"] = df_statistic["sma20"] + (2 * std_20)
        df_statistic["bb_lower"] = df_statistic["sma20"] - (2 * std_20)
        df_statistic["bb_middle"] = df_statistic["sma20"]
        df_statistic["bb"] = ((df_statistic["bb_upper"] - df_statistic["bb_lower"]) /
                              df_statistic["bb_middle"]) * 100

        # RSI 10
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=10, min_periods=1).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=10, min_periods=1).mean()
        loss = loss.replace(0, 1e-10)
        rs = gain / loss
        df_statistic["rsi10"] = 100 - (100 / (1 + rs))

        # Volume Profile - POC, VAH, VAL
        df["price_bin"] = pd.cut(close, bins=24)
        grouped = df.groupby("price_bin", observed=True)["volume"].sum().reset_index()
        grouped = grouped.dropna(subset=["price_bin"])
        grouped = grouped.sort_values("volume", ascending=False).reset_index(drop=True)

        total_volume = grouped["volume"].sum()
        grouped["cumulative"] = grouped["volume"].cumsum()
        grouped["cumulative_pct"] = grouped["cumulative"] / total_volume

        poc_bin = grouped.iloc[0]["price_bin"] if not grouped.empty else None
        vah_bin = grouped[grouped["cumulative_pct"] <= 0.7]["price_bin"].max() if not grouped.empty else None
        val_bin = grouped[grouped["cumulative_pct"] <= 0.3]["price_bin"].min() if not grouped.empty else None

        poc_price = poc_bin.mid if isinstance(poc_bin, pd.Interval) else 0
        vah_price = vah_bin.mid if isinstance(vah_bin, pd.Interval) else 0
        val_price = val_bin.mid if isinstance(val_bin, pd.Interval) else 0

        df_statistic["poc"] = poc_price
        df_statistic["va_high"] = vah_price
        df_statistic["va_low"] = val_price

        latest = df_statistic.iloc[-1]
        print(f"ATR: {latest['atr']}")
        print(f"SMA 20: {latest['sma20']}")
        print(f"EMA 12: {latest['ema12']}")
        print(f"MACD: {latest['macd']}")
        print(f"Bollinger Bands Width %: {latest['bb']}")
        print(f"RSI 10: {latest['rsi10']}")
        print(f"POC: {latest['poc']}, VAH: {latest['va_high']}, VAL: {latest['va_low']}")

        df_statistic["event_id"] = df["event_id"]
        df_statistic["symbol"] = df["symbol"]
        df_statistic["endtime"] = df["endtime"]

        # Handle null
        df_statistic = df_statistic.fillna(0)

        order_col = ["event_id", "endtime", "symbol",
                     "sma20", "ema12", "rsi10", "macd", "bb", "atr",
                     "va_high", "va_low", "poc"]
        latest_row = df_statistic.iloc[[-1]]
        values = latest_row[order_col].values.tolist()
        cursor = conn.cursor()
        execute_values(cursor, query_load, values)

        return latest_row

    except Exception as e:
        print(f"Raise error when calculate statistic: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        conn.commit()
        pool.putconn(conn)


def fetch_coin_price(symbol, query):
    pool = run_pool()
    conn = pool.getconn()
    conn.autocommit = True
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol, symbol,))
    finally:
        pool.putconn(conn, close=True)
    return df

def fetch_min_max(symbol, query):
    pool = run_pool()
    conn = pool.getconn()
    conn.autocommit = True
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol, ))
        return df
    finally:
        pool.putconn(conn)

def fetch_news(symbol, query):
    pool = run_pool()
    conn = pool.getconn()
    conn.autocommit = True
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol,))
        return df
    finally:
        pool.putconn(conn)

def draw_chart(df, coin_symbol):
    df = df.sort_values("endtime")
    min_price = df["close"].min() - 10
    max_price = df["close"].max() + 10
    padding = (max_price - min_price)

    # Hover
    hover = alt.selection_single(
        fields=["endtime"], nearest=True,
        on="mouseover", empty="none", clear="mouseout"
    )

    # Chart
    base = alt.Chart(df).encode(
        x=alt.X("endtime:T", title="Thời gian", axis=alt.Axis(format="%H:%M:%S")),
        y=alt.Y("close:Q", title="Giá", scale=alt.Scale(domain=[min_price, max_price]))
    )

    # Line
    line = base.mark_line().encode(
        tooltip=[
            alt.Tooltip("endtime:T", title="Thời gian"),
            alt.Tooltip("close:Q", title="Giá")
        ]
    )

    # Point
    points = base.mark_point(size=50, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    ).add_selection(hover)

    # hover rule
    rule = base.mark_rule(color="gray").encode(
        opacity=alt.condition(hover, alt.value(0.3), alt.value(0))
    ).transform_filter(hover)

    return (line + points + rule).properties(
        width=1000,
        height=500,
        title=f"📈 Giá {coin_symbol.upper()} theo thời gian"
    )

def _softmax(x):
    x = np.asarray(x, dtype=np.float64)
    x = x - x.max(axis=-1, keepdims=True)
    e = np.exp(x)
    return e / e.sum(axis=-1, keepdims=True)

def _load_local_hf_model(model_dir: str):
    try:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        tok = AutoTokenizer.from_pretrained(model_dir)
        mdl = AutoModelForSequenceClassification.from_pretrained(model_dir)
        return tok, mdl
    except Exception:
        return None, None

def _predict_scores_local(texts: list[str], model_dir: str):
    tok, mdl = _load_local_hf_model(model_dir)
    if tok is None or not texts:
        return None
    try:
        import torch
        mdl.eval()
        scores = []
        with torch.no_grad():
            for i in range(0, len(texts), 32):
                batch = texts[i:i+32]
                inp = tok(batch, padding=True, truncation=True, max_length=128, return_tensors="pt")
                out = mdl(**inp)
                prob = _softmax(out.logits.numpy())
                # Map to [-100,100] as (p_pos - p_neg)*100, assuming id 2=POS, 0=NEG
                s = (prob[:, 2] - prob[:, 0]) * 100.0
                scores.extend(s.tolist())
        return np.asarray(scores, dtype=float)
    except Exception:
        return None

def _predict_scores_fallback(texts: list[str]):
    if not texts:
        return None
    try:
        from transformers import pipeline
        clf = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
        preds = clf(texts, truncation=True)
        out = []
        for p in preds:
            label = str(p.get("label","")).lower()
            conf = float(p.get("score", 0.5))
            if "pos" in label or "2" in label:
                out.append(+100.0 * conf)
            elif "neg" in label or "0" in label:
                out.append(-100.0 * conf)
            else:
                out.append(0.0)
        return np.asarray(out, dtype=float)
    except Exception:
        return None

def _momentum_from_price(price_df: pd.DataFrame) -> pd.DataFrame:
    df = price_df.copy()
    df["endtime"] = pd.to_datetime(df["endtime"], utc=True)
    df = df.sort_values("endtime")
    ret = df["close"].pct_change().fillna(0.0)
    z = (ret.rolling(12, min_periods=6).mean() - ret.rolling(96, min_periods=24).mean())
    std = ret.rolling(96, min_periods=24).std().replace(0, np.nan)
    z = (z / std).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-2, 2)
    return pd.DataFrame({
        "endtime": df["endtime"],
        "sentiment": z * 10.0,   # for chart [-20,20]
        "score": z * 50.0        # for gauge [-100,100]
    })

def get_sentiment_series(symbol: str, price_df: pd.DataFrame, text_df: pd.DataFrame | None) -> pd.DataFrame:
    """
    Returns ['endtime','sentiment','score'] aligned to price_df.
    Uses local fine‑tuned model if available, else public model, else momentum fallback.
    """
    if text_df is None or text_df.empty:
        return _momentum_from_price(price_df)

    texts = text_df["text"].astype(str).tolist()
    # try local model at models/sentiment_distilbert
    model_dir = Path(__file__).resolve().parents[2] / "models" / "sentiment_distilbert"
    scores = _predict_scores_local(texts, str(model_dir)) if model_dir.exists() else None
    if scores is None:
        scores = _predict_scores_fallback(texts)
    if scores is None:
        return _momentum_from_price(price_df)

    df = text_df[["created_at"]].copy()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["score"] = scores
    # aggregate to price timeline
    idx = pd.to_datetime(price_df["endtime"], utc=True)
    agg = (df.set_index("created_at").resample("5min")["score"].mean()
             .reindex(idx).interpolate(limit_direction="both").fillna(0.0).clip(-100, 100))
    out = pd.DataFrame({"endtime": idx, "score": agg.values})
    out["sentiment"] = (out["score"] / 5.0).clip(-20, 20)
    return out

def draw_price_sentiment_chart(price_df: pd.DataFrame, sent_df: pd.DataFrame, symbol: str):
    df = price_df.copy()
    df["endtime"] = pd.to_datetime(df["endtime"], utc=True)
    df = df.sort_values("endtime")
    s = sent_df.copy()
    s["endtime"] = pd.to_datetime(s["endtime"], utc=True)
    s = s.sort_values("endtime")

    base = alt.Chart(df).encode(x=alt.X("endtime:T", axis=alt.Axis(title=None, format="%H:%M")))
    price_line = base.mark_line(color="#22c55e", strokeWidth=2).encode(
        y=alt.Y("close:Q", axis=alt.Axis(title="Price"), scale=alt.Scale(zero=False))
    )
    sent_line = alt.Chart(s).mark_line(color="#d1a31b", strokeWidth=2).encode(
        x="endtime:T",
        y=alt.Y("sentiment:Q", axis=alt.Axis(title="Sentiment"), scale=alt.Scale(domain=[-20, 20])),
        tooltip=[alt.Tooltip("sentiment:Q", format=".2f", title="Sentiment")]
    )
    zero_rule = alt.Chart(pd.DataFrame({"y":[0]})).mark_rule(color="#94a3b8").encode(y="y")
    return alt.layer(price_line, sent_line, zero_rule).resolve_scale(y="independent").properties(height=360)

def compute_overall_sentiment(sent_df: pd.DataFrame) -> tuple[float, str]:
    if sent_df is None or sent_df.empty:
        return 0.0, "Neutral"
    s = float(sent_df["score"].iloc[-1])
    if s <= -60: lab = "Very Bearish"
    elif s <= -20: lab = "Bearish"
    elif s < 20: lab = "Neutral"
    elif s < 60: lab = "Bullish"
    else: lab = "Very Bullish"
    return s, lab