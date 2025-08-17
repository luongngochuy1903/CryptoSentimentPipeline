import streamlit as st
import pandas as pd
import time
import altair as alt
from streamlit_autorefresh import st_autorefresh
from datetime import timezone, timedelta

from query import  query_get_coin_data, query_load_back_to_db, query_get_24h_min_max, query_get_raw_statistic, query_load_back_to_db_sen
from modules import fetch_min_max, fetch_coin_price, draw_chart, calculate_technical, label_detach

st.set_page_config(page_title="CRYPTO AI SENTIMENT SUPPORT", layout="wide")
st.title("Coin Market")

symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
# Refresh mỗi 3 giây
st_autorefresh(interval=3000, key="data_refresh")

all_prices = {sym: fetch_coin_price(sym, query_get_coin_data) for sym in symbols}
all_technical = {sym: calculate_technical(sym, query_get_raw_statistic, query_load_back_to_db) for sym in symbols}
all_min_max = {sym: fetch_min_max(sym, query_get_24h_min_max) for sym in symbols}
all_sentiment = {sym: label_detach(all_technical[sym], query_load_back_to_db_sen) for sym in symbols}

# Chọn coin
coin = st.selectbox(
    "Coin >", 
    options=symbols, 
    index=0
)

# Lấy dữ liệu
df_realtime_price_24h = all_prices[coin]
print(f"checking date: {df_realtime_price_24h['endtime'].iloc[0]}")
df_technical = all_technical[coin]
df_min_max = all_min_max[coin]
df_sentiment = all_sentiment[coin]

# Hiển thị biểu đồ
if not df_realtime_price_24h.empty:
    chart = draw_chart(df_realtime_price_24h, coin)
    price_diff = df_realtime_price_24h['price_diff'].iloc[0]
    price_diff_str = f"{price_diff:.2f}" if pd.notna(price_diff) else "null"
    if pd.isna(price_diff):
        diff_color = "gray"
    elif price_diff > 0:
        diff_color = "green"
    elif price_diff < 0:
        diff_color = "red"
    else:
        diff_color = "gray"
    st.markdown(f"""
            <style>
            [data-testid="stHorizontalBlock"] > div:nth-of-type(2) [data-testid="stMetricValue"] {{
                color: {diff_color} !important;
            }}
            [data-testid="stMetricLabel"] {{
                font-size: 14px !important;
            }}
            [data-testid="stMetricDelta"] {{
                font-size: 12px !important;
            }}
            [data-testid="stMetricValue"] {{
                font-size: 18px !important;
            }}
            </style>
        """, unsafe_allow_html=True)
    
    col_main1, col_main2, col_main3 = st.columns(3)

    with col_main1:
        st.metric(label="Price", value=f"{df_realtime_price_24h['close'].iloc[0]:.2f}", delta="Neutral", delta_color="inverse", border=False)
    with col_main2:
        st.metric(label="▲", value=price_diff_str, delta="Neutral", delta_color="inverse", border=False)
    with col_main3:
        percent_diff = df_realtime_price_24h['percent_change'].iloc[0]
        percent_diff_str = f"{percent_diff:.2f}" if pd.notna(percent_diff) else "null"
        st.metric(label="%", value=percent_diff_str, delta="Neutral", delta_color="inverse", border=False)
        st.metric(label="Highest", value=f"{df_min_max['max_value'].iloc[0]}", delta="Neutral", delta_color="inverse", border=False)

    st.altair_chart(chart, use_container_width=True)
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    col7, col8, col9 = st.columns(3)
    with col1:
        st.metric(label="RSI (14)", value=f"{df_technical['rsi10'].iloc[0]:.2f}", delta=f"{df_sentiment['rsi_sen'].iloc[0]}", delta_color="inverse", border=True)
    with col2:
        st.metric(label="SMA (20)", value=f"{df_technical['sma20'].iloc[0]:.2f}", delta=f"{df_sentiment['sma_sen'].iloc[0]}", delta_color="inverse", border=True)
    with col3:
        st.metric(label="EMA (12)", value=f"{df_technical['ema12'].iloc[0]:.2f}", delta=f"{df_sentiment['ema_sen'].iloc[0]}", delta_color="inverse", border=True)

    with col4:
        st.metric(label="MACD", value=f"{df_technical['macd'].iloc[0]:.2f}", delta=f"{df_sentiment['macd_sen'].iloc[0]}", delta_color="inverse", border=True)
    with col5:
        st.metric(label="Bolling bands", value=f"{df_technical['bb'].iloc[0]:.2f}", delta=f"{df_sentiment['bb_sen'].iloc[0]}", delta_color="inverse", border=True)
    with col6:
        st.metric(label="ATR", value=f"{df_technical['atr'].iloc[0]:.2f}", delta=f"{df_sentiment['atr_sen'].iloc[0]}", delta_color="inverse", border=True)
    
    with col7:
        st.metric(label="va_high", value=f"{df_technical['va_high'].iloc[0]:.2f}", delta="Neutral", delta_color="inverse", border=True)
    with col8:
        st.metric(label="va_low", value=f"{df_technical['va_low'].iloc[0]:.2f}", delta="Neutral", delta_color="inverse", border=True)
    with col9:
        st.metric(label="POC", value=f"{df_technical['poc'].iloc[0]:.2f}", delta="Neutral", delta_color="inverse", border=True)
    

    st.markdown(f"🕒 Cập nhật: {pd.to_datetime(df_realtime_price_24h['endtime'].max(), utc=True).astimezone(timezone(timedelta(hours=7)))}")
else:
    st.warning("Không có dữ liệu giá.")
