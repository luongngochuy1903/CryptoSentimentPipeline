import websocket
import json
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'kafka:9092'
})

# Cấu hình danh sách coin muốn sub
symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]
symbol_name_map = {
    "BTCUSDT": "Bitcoin",
    "ETHUSDT": "Ethereum",
    "BNBUSDT": "BNB",
    "SOLUSDT": "Solana",
    "XRPUSDT": "XRP"
}
streams = [f"{symbol}@kline_1m" for symbol in symbols]

#Checking lỗi
def acked(err, msg):
    if err is not None:
        print(f'Data is not delivered')
    else:
        print(f'Data is delivered to {msg.topic()} [msg.partition()]')

# Xử lý khi nhận message
def on_message(ws, message):
    data = json.loads(message)
    if "result" in data:
        print(f"Đã SUBSCRIBE thành công - ID: {data['id']}")
        return
    if data.get("e") == "kline":
        symbol = data['s']
        candle = data['k']
        if candle['x']:
            starttime_ms = candle['t']
            endtime_ms = candle['T']
            interval = candle['i']
            open = candle['o']
            close = candle['c']
            highest = candle['h']
            lowest = candle['l']
            volume = candle['v']
            value = candle['q']
            # Chuyển sang giờ VN
            tradetime = datetime.fromtimestamp(starttime_ms / 1000, tz=timezone.utc).astimezone(timezone(timedelta(hours=7)))
            eventtime = datetime.fromtimestamp(endtime_ms / 1000, tz=timezone.utc).astimezone(timezone(timedelta(hours=7)))
            
            message_data = {
                "symbol": symbol,
                "name": symbol_name_map.get(symbol),
                "interval": interval,
                "starttime": starttime_ms,
                "endtime": endtime_ms,
                "volume": volume,
                "quotevolume": value,
                "open": open,
                "close":close,
                "highest":highest,
                "lowest": lowest,
                "tag": "realtime"
            }
            producer.produce("real-time", value=json.dumps(message_data).encode('utf-8'), callback=acked)
            print(f"\n[📈 {symbol}]")
            print(f"🕒 Kline-Start: {tradetime} | End: {eventtime} ---- Interval: {interval}")
            print(f"💰 Giá open: {open} | 🔢 Số lượng coin: {volume}")
            print(f"💰 Giá close: {close} | 🔢 Tổng giá trị coin (BTC): {value}")
            print("-" * 50)

# Gửi yêu cầu SUBSCRIBE khi kết nối
def on_open(ws):
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }))

# Kết nối tới raw socket
socket_url = "wss://stream.binance.com:9443/ws"
ws = websocket.WebSocketApp(socket_url, on_message=on_message, on_open=on_open)
ws.run_forever()
