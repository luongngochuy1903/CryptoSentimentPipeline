import time
from datetime import datetime, timezone
from pathlib import Path
import argparse
import requests
import pandas as pd

def fetch_prices(symbol: str, minutes: int) -> pd.DataFrame:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - minutes * 60_000
    url = "https://api.binance.com/api/v3/klines"
    rows = []
    cur = start_ms
    while cur < end_ms:
        r = requests.get(url, params={"symbol": symbol, "interval": "1m", "startTime": cur, "endTime": end_ms, "limit": 1000}, timeout=15)
        r.raise_for_status()
        kl = r.json()
        if not kl: break
        for k in kl:
            rows.append({"symbol": symbol, "endtime": datetime.fromtimestamp(int(k[6]) / 1000, tz=timezone.utc), "close": float(k[4])})
        cur = int(kl[-1][6]) + 1
        if len(kl) < 1000: break
    return pd.DataFrame(rows).drop_duplicates(subset=["endtime"])

def build_prices(out_dir: Path, minutes: int, symbols: list[str]) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for s in symbols:
        try:
            df = fetch_prices(s, minutes)
            if df.empty:
                print(f"No prices for {s}"); continue
            f = out_dir / f"price_{s}.parquet"
            df.to_parquet(f, index=False)
            total += len(df)
            print(f"Saved {len(df)} -> {f}")
        except Exception as e:
            print(f"Failed {s}: {e}")
    return total

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=1440)
    ap.add_argument("--symbols", nargs="*", default=["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT"])
    ap.add_argument("--out", default=str(Path("data") / "silver" / "realtime" / "data"))
    args = ap.parse_args()
    total = build_prices(Path(args.out), args.minutes, args.symbols)
    print("Total rows:", total)