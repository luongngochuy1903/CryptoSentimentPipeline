import streamlit as st
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
import altair as alt
import pandas as pd

@st.cache_resource
def run_pool():
    return SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host="postgres",
        database="backend",
        user="airflow",
        password="airflow",
        port="5432"
    )

def calculate_technical(symbol, query, query_load):
    pool = run_pool()
    conn = pool.getconn()
    cursor = None
    try:
        df = pd.read_sql(query, conn, params=(symbol,))

        df_statistic = pd.DataFrame()
        df = df.sort_values("endtime", ascending=True).reset_index(drop=True)

        df_statistic = pd.DataFrame(index=df.index)

        # ATR 14
        high = df["highest"]
        low = df["lowest"]
        close = df["close"]

        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df_statistic["atr"] = tr.rolling(window=14, min_periods=1).mean()

        # SMA 20
        df_statistic["sma_20"] = close.rolling(window=20, min_periods=1).mean()

        # EMA 12
        df_statistic["ema_12"] = close.ewm(span=12, adjust=False).mean()

        # EMA 26 & MACD
        ema_26 = close.ewm(span=26, adjust=False).mean()
        df_statistic["macd"] = df_statistic["ema_12"] - ema_26

        # Bollinger Bands (BB)
        std_20 = close.rolling(window=20, min_periods=1).std()
        df_statistic["bb_upper"] = df_statistic["sma_20"] + (2 * std_20)
        df_statistic["bb_lower"] = df_statistic["sma_20"] - (2 * std_20)
        df_statistic["bb_middle"] = df_statistic["sma_20"]
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
        grouped = grouped.sort_values("volume", ascending=False).reset_index(drop=True)

        total_volume = grouped["volume"].sum()
        grouped["cumulative"] = grouped["volume"].cumsum()
        grouped["cumulative_pct"] = grouped["cumulative"] / total_volume

        poc_bin = grouped.iloc[0]["price_bin"]

        vah_bin = grouped[grouped["cumulative_pct"] <= 0.7]["price_bin"].max()  
        val_bin = grouped[grouped["cumulative_pct"] <= 0.3]["price_bin"].min() 

        poc_price = poc_bin.mid
        vah_price = vah_bin.mid
        val_price = val_bin.mid

        df_statistic["poc"] = poc_price
        df_statistic["vah"] = vah_price
        df_statistic["val"] = val_price

        # Lấy kết quả mới nhất
        latest = df_statistic.iloc[-1]
        print(f"ATR: {latest['atr']}")
        print(f"SMA 20: {latest['sma_20']}")
        print(f"EMA 12: {latest['ema_12']}")
        print(f"MACD: {latest['macd']}")
        print(f"Bollinger Bands Width %: {latest['bb']}")
        print(f"RSI 10: {latest['rsi10']}")
        print(f"POC: {latest['poc']}, VAH: {latest['vah']}, VAL: {latest['val']}")
        print(f"Checking poc, vah, val: {df_statistic['poc'].iloc[0]}, {df_statistic['vah'].iloc[0]}, {df_statistic['val'].iloc[0]}")

        df_statistic["event_id"] = df["event_id"]
        df_statistic["symbol"] = df["symbol"]
        df_statistic["endtime"] = df["endtime"]
        order_col = ["event_id", "symbol", "sma_20", "ema_12", "rsi10", "macd", "bb", "atr", "vah", "val", "poc"]
        values = df_statistic[order_col].values.tolist()
        cursor = conn.cursor()

        execute_values( cursor, query_load, values)

        return df_statistic
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
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol, symbol,))
        return df
    finally:
        pool.putconn(conn)

def fetch_min_max(symbol, query):
    pool = run_pool()
    conn = pool.getconn()
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol, ))
        return df
    finally:
        pool.putconn(conn)

def fetch_news(symbol, query):
    pool = run_pool()
    conn = pool.getconn()
    cursor = conn.cursor()
    try:
        #------------------- RUN -------------------
        df = pd.read_sql(query, conn, params=(symbol,))
        return df
    finally:
        pool.putconn(conn)

def draw_chart(df, coin_symbol):
    # df = df.sort_values("endtime")
    min_price = df["close"].min() - 10
    max_price = df["close"].max() + 10
    padding = (max_price - min_price)

    # Tạo selection hover
    hover = alt.selection_single(
        fields=["endtime"], nearest=True,
        on="mouseover", empty="none", clear="mouseout"
    )

    # Biểu đồ cơ bản
    base = alt.Chart(df).encode(
        x=alt.X("endtime:T", title="Thời gian", axis=alt.Axis(format="%H:%M:%S")),
        y=alt.Y("close:Q", title="Giá", scale=alt.Scale(domain=[min_price, max_price]))
    )

    # Vẽ đường giá
    line = base.mark_line().encode(
        tooltip=[
            alt.Tooltip("endtime:T", title="Thời gian"),
            alt.Tooltip("close:Q", title="Giá")
        ]
    )

    # Vẽ điểm (hover)
    points = base.mark_point(size=50, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    ).add_selection(hover)

    # Vẽ rule khi hover
    rule = base.mark_rule(color="gray").encode(
        opacity=alt.condition(hover, alt.value(0.3), alt.value(0))
    ).transform_filter(hover)


    # Gộp các thành phần
    return (line + points + rule).properties(
        width=1000,
        height=500,
        title=f"📈 Giá {coin_symbol.upper()} theo thời gian"
    )