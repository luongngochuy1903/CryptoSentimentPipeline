import glob, re
from pathlib import Path
import pandas as pd

TICKER_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
    "SOL": "SOLUSDT", "XRP": "XRPUSDT",
    "BITCOIN": "BTCUSDT", "ETHEREUM": "ETHUSDT", "SOLANA": "SOLUSDT"
}
SYMBOLS = {"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT"}

def _read_any(path: str) -> pd.DataFrame:
    try:
        p = path.lower()
        if p.endswith((".parquet", ".pq")):
            return pd.read_parquet(path, engine="pyarrow")
        if p.endswith(".jsonl"):
            return pd.read_json(path, lines=True)
        if p.endswith(".json"):
            return pd.read_json(path)
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def _ensure_dt_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)

def _infer_symbol_from_text(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    t = text.upper()
    for k, v in TICKER_MAP.items():
        if re.search(rf"(\$|\b){re.escape(k)}\b", t):
            return v
    return None

def _infer_symbol_from_filename(path: str) -> str | None:
    name = Path(path).name.lower()
    if "bitcoin" in name or "_btc" in name or "btc_" in name: return "BTCUSDT"
    if "ethereum" in name or "_eth" in name or "eth_" in name: return "ETHUSDT"
    if "solana" in name or "_sol" in name or "sol_" in name: return "SOLUSDT"
    if "bnb" in name: return "BNBUSDT"
    if "xrp" in name: return "XRPUSDT"
    return None

def _collect_files(*dirs: Path) -> list[str]:
    files = []
    for d in dirs:
        if d and Path(d).exists():
            files.extend(glob.glob(str(Path(d) / "**" / "*.*"), recursive=True))
    return files

def load_texts(silver_dir: str) -> pd.DataFrame:
    base = Path(silver_dir)
    raw_base = base.parent / "raw"
    gold_base = base.parent / "gold"
    repo_root = Path(__file__).resolve().parents[2]

    news_dirs = [base / "news" / "data", raw_base / "news", gold_base / "news" / "data"]
    cmt_dirs  = [base / "comments" / "data", raw_base / "comments", gold_base / "comments" / "data"]
    files = _collect_files(*news_dirs, *cmt_dirs)

    # repo-root fallbacks
    for pat in ["*_news_gnews.jsonl","rss_*_filtered_*.jsonl","reddit_crypto_cmt.jsonl","coin_news_debug.json"]:
        files.extend([str(p) for p in repo_root.glob(pat)])

    frames = []
    for f in files:
        df = _read_any(f)
        if df.empty:
            continue
        # text
        text = None
        for cols in [["text"], ["title","description"], ["title","summary"], ["content"], ["body"], ["selftext"]]:
            if all(c in df.columns for c in cols):
                text = df[cols[0]].astype(str) if len(cols) == 1 else (df[cols[0]].astype(str) + " " + df[cols[1]].astype(str))
                break
        if text is None:
            continue
        # ts
        ts = None
        for col in ["created_at","published","publishedAt","published_at","time","timestamp","date","created_utc"]:
            if col in df.columns:
                ts = _ensure_dt_utc(df[col]); break
        if ts is None:
            continue
        # symbol
        sym = None
        for col in ["symbol","pair","coin","ticker"]:
            if col in df.columns:
                sym = df[col].astype(str).str.upper().replace(TICKER_MAP); break
        if sym is None:
            fs = _infer_symbol_from_filename(f)
            if fs: sym = pd.Series([fs]*len(df))
        if sym is None:
            sym = text.apply(_infer_symbol_from_text)

        src = "comments" if ("comment" in f.lower() or "reddit" in f.lower()) else "news"
        frames.append(pd.DataFrame({"text": text, "created_at": ts, "symbol": sym, "source": src}))

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["text","created_at","symbol","source"])
    out = out.dropna(subset=["text","created_at","symbol"])
    out["symbol"] = out["symbol"].astype(str).str.upper().replace(TICKER_MAP)
    out = out[out["symbol"].isin(SYMBOLS)].sort_values("created_at").reset_index(drop=True)
    return out

def load_prices(silver_dir: str) -> pd.DataFrame:
    base = Path(silver_dir)
    gold_base = base.parent / "gold"
    iceberg_base = base.parent / "iceberg"
    raw_base = base.parent / "raw"
    search_dirs = [
        base / "realtime" / "data",
        gold_base / "realtime" / "data",
        iceberg_base / "realtime",
        raw_base / "realtimecoin",
    ]
    files = _collect_files(*search_dirs)
    if not files and (base / "realtime").exists():
        files = glob.glob(str((base / "realtime") / "**" / "*.*"), recursive=True)

    frames = []
    for f in files:
        df = _read_any(f)
        if df.empty:
            continue
        cols = {c.lower(): c for c in df.columns}
        end_col = cols.get("endtime") or cols.get("timestamp") or cols.get("time")
        close_col = cols.get("close") or cols.get("price") or cols.get("last")
        symbol_col = cols.get("symbol") or cols.get("pair") or cols.get("ticker")
        if not (end_col and close_col and symbol_col):
            continue
        tmp = pd.DataFrame({
            "symbol": df[symbol_col].astype(str).str.upper().replace(TICKER_MAP),
            "endtime": _ensure_dt_utc(df[end_col]),
            "close": pd.to_numeric(df[close_col], errors="coerce")
        }).dropna()
        frames.append(tmp)
    if not frames:
        return pd.DataFrame(columns=["symbol","endtime","close"])
    out = pd.concat(frames, ignore_index=True)
    out = out[out["symbol"].isin(SYMBOLS)].sort_values(["symbol","endtime"]).reset_index(drop=True)
    return out

def _future_returns_by_symbol(prices: pd.DataFrame, horizon_min: int) -> pd.DataFrame:
    outs = []
    for sym, grp in prices.groupby("symbol", sort=False):
        grp = grp.sort_values("endtime").drop_duplicates(subset=["endtime"])
        left = grp[["endtime", "close"]].copy()
        left["target_time"] = left["endtime"] + pd.Timedelta(minutes=horizon_min)
        right = grp[["endtime", "close"]].rename(columns={"endtime": "future_time", "close": "future_close"})
        m = pd.merge_asof(
            left.sort_values("target_time"),
            right.sort_values("future_time"),
            left_on="target_time",
            right_on="future_time",
            direction="forward",
            allow_exact_matches=True,
        )
        m["symbol"] = sym
        m = m.rename(columns={"endtime": "bar_time"})
        m["fwd_ret"] = (m["future_close"] - m["close"]) / m["close"]
        outs.append(m[["symbol", "bar_time", "fwd_ret"]])
    return pd.concat(outs, ignore_index=True) if outs else pd.DataFrame(columns=["symbol","bar_time","fwd_ret"])

def _align_texts_to_bars(texts: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    prices_small = prices[["symbol", "endtime", "close"]].rename(columns={"endtime": "bar_time"})
    outs = []
    for sym, tgrp in texts.groupby("symbol", sort=False):
        pgrp = prices_small[prices_small["symbol"] == sym].sort_values("bar_time")
        if pgrp.empty:
            continue
        tgrp = tgrp.sort_values("created_at")
        pgrp = pgrp[["bar_time", "close"]]
        a = pd.merge_asof(
            tgrp,
            pgrp,
            left_on="created_at",
            right_on="bar_time",
            direction="backward",
            allow_exact_matches=True,
        )
        outs.append(a)
    if not outs:
        return pd.DataFrame(columns=list(texts.columns) + ["bar_time", "close"])
    return pd.concat(outs, ignore_index=True).dropna(subset=["bar_time"])

def build_training_table(silver_dir: str, horizon_min: int = 30, up: float = 0.002, down: float = -0.002, max_samples: int | None = 150_000) -> pd.DataFrame:
    texts = load_texts(silver_dir)
    prices = load_prices(silver_dir)
    if texts.empty or prices.empty:
        return pd.DataFrame(columns=["text","created_at","symbol","label_id"])

    prices = prices.sort_values(["symbol","endtime"]).dropna(subset=["endtime","close"])
    merged = _align_texts_to_bars(texts, prices)
    if merged.empty:
        return pd.DataFrame(columns=["text","created_at","symbol","label_id"])

    fut = _future_returns_by_symbol(prices, horizon_min)
    if fut.empty:
        return pd.DataFrame(columns=["text","created_at","symbol","label_id"])

    merged = pd.merge(
        merged,
        fut,
        on=["symbol","bar_time"],
        how="left",
    ).dropna(subset=["fwd_ret"])

    def to_label_id(r):
        if r >= up: return 2
        if r <= down: return 0
        return 1

    merged["label_id"] = merged["fwd_ret"].apply(to_label_id)
    merged["text"] = merged["text"].astype(str).str.slice(0, 512)
    if max_samples and len(merged) > max_samples:
        merged = merged.sample(n=max_samples, random_state=42)
    return merged[["text","created_at","symbol","label_id"]].reset_index(drop=True)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default=str(Path(__file__).resolve().parents[2] / "data" / "silver"))
    args = ap.parse_args()
    txt = load_texts(args.silver_dir)
    prc = load_prices(args.silver_dir)
    print(f"texts: {len(txt)}, prices: {len(prc)}")
    if not txt.empty and not prc.empty:
        df = build_training_table(args.silver_dir, horizon_min=15, up=0.001, down=-0.001)
        if not df.empty:
            print(df["label_id"].value_counts(dropna=False))
        else:
            print("Built 0 labeled rows")