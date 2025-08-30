from __future__ import annotations

import pandas as pd
import numpy as np


def _per_symbol(df: pd.DataFrame, func):
    return df.groupby("symbol", group_keys=False).apply(func)


def make_price_features(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Build simple per-bar price features per symbol.

    Expects columns: ['symbol','endtime','close'] (optionally 'open','high','low','volume').
    Returns DataFrame with ['symbol','bar_time', <features...>].
    """
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["symbol", "bar_time"])  # empty

    df = prices.copy()
    df = df.dropna(subset=["symbol", "endtime", "close"]).copy()
    df["bar_time"] = pd.to_datetime(df["endtime"], utc=True)
    df = df.sort_values(["symbol", "bar_time"]).reset_index(drop=True)

    def add_feats(g: pd.DataFrame):
        g = g.copy()
        # 1-bar log return (approx)
        g["ret_1"] = np.log(g["close"]).diff()
        # rolling features (assuming 1-row ~ 5 minutes in your pipeline; adjust if needed)
        # Use windows ~ 1, 3, 6 bars (5, 15, 30 minutes)
        g["ret_3"] = g["close"].pct_change(3)
        g["ret_6"] = g["close"].pct_change(6)
        # rolling volatility (std of 1-bar returns) over 12/24 bars (~1h/2h)
        g["vol_12"] = g["ret_1"].rolling(12, min_periods=3).std()
        g["vol_24"] = g["ret_1"].rolling(24, min_periods=6).std()
        # simple momentum indicator: z-score of 1-bar return over 24 bars
        r = g["ret_1"]
        m = r.rolling(24, min_periods=6).mean()
        s = r.rolling(24, min_periods=6).std()
        g["mom_z24"] = (r - m) / (s.replace(0, np.nan))
        # normalize missing
        return g

    df = _per_symbol(df, add_feats)

    feat_cols = [
        "ret_1", "ret_3", "ret_6", "vol_12", "vol_24", "mom_z24",
    ]
    out = df[["symbol", "bar_time", *feat_cols]].copy()
    return out


def latest_features_for_symbol(feats: pd.DataFrame, symbol: str) -> pd.Series | None:
    """Return latest feature row for a symbol as a Series (or None)."""
    if feats is None or feats.empty:
        return None
    sub = feats[feats["symbol"] == symbol]
    if sub.empty:
        return None
    return sub.sort_values("bar_time").iloc[-1]
