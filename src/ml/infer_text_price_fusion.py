#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
import argparse
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import joblib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transformers import AutoTokenizer, AutoModel
from src.ml.dataset_builder import load_prices
from src.ml.price_features import make_price_features, latest_features_for_symbol


ID2LABEL = {0: "NEG", 1: "NEU", 2: "POS"}
LABEL2ID = {v: k for k, v in ID2LABEL.items()}


class FusionClassifier(nn.Module):
    def __init__(self, text_model_name: str, price_dim: int, hidden: int = 256, freeze_text: bool = True):
        super().__init__()
        self.text_model = AutoModel.from_pretrained(text_model_name)
        self.hidden_size = self.text_model.config.hidden_size
        if freeze_text:
            for p in self.text_model.parameters():
                p.requires_grad = False
        self.proj = nn.Linear(self.hidden_size + price_dim, hidden)
        self.act = nn.ReLU()
        self.dropout = nn.Dropout(0.2)
        self.head = nn.Linear(hidden, 3)

    def forward(self, input_ids=None, attention_mask=None, price=None):
        out = self.text_model(input_ids=input_ids, attention_mask=attention_mask)
        last = out.last_hidden_state
        mask = attention_mask.unsqueeze(-1).float()
        text_vec = (last * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1e-6)
        x = torch.cat([text_vec, price], dim=1)
        logits = self.head(self.dropout(self.act(self.proj(x))))
        return logits


class FusionPredictor:
    def __init__(self, model_dir: Path | str | None = None, device: str | None = None):
        self.model_dir = Path(model_dir) if model_dir else (ROOT / "models" / "sentiment_text_price_fusion")
        if not self.model_dir.exists():
            raise FileNotFoundError(f"Fusion model dir not found: {self.model_dir}")
        payload = torch.load(self.model_dir / "fusion_model.pt", map_location="cpu")
        self.model_name = payload.get("model_name", "distilbert-base-uncased")
        self.price_feat_cols = payload.get("price_feat_cols", ["ret_1","ret_3","ret_6","vol_12","vol_24","mom_z24"])
        freeze_text = bool(payload.get("freeze_text", True))
        self.tok = AutoTokenizer.from_pretrained(self.model_dir if (self.model_dir / "tokenizer_config.json").exists() else self.model_name)
        self.device = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
        self.model = FusionClassifier(self.model_name, price_dim=len(self.price_feat_cols), freeze_text=freeze_text).to(self.device)
        self.model.load_state_dict(payload["state_dict"])
        self.model.eval()
        self.scaler = joblib.load(self.model_dir / "price_scaler.pkl")

    @torch.no_grad()
    def predict(self, text: str, symbol: str, silver_dir: Path | str | None = None) -> dict:
        # Build price vector from latest features of the symbol
        sdir = Path(silver_dir) if silver_dir else (ROOT / "data" / "silver")
        prices = load_prices(str(sdir))
        feats = make_price_features(prices)
        row = latest_features_for_symbol(feats, symbol)
        if row is None:
            raise ValueError(f"No price features available for symbol: {symbol}")
        Xp = row[self.price_feat_cols].fillna(0.0).values.astype(np.float32)[None, :]
        Xp = self.scaler.transform(Xp)
        Xp_t = torch.from_numpy(Xp).to(self.device)

        enc = self.tok(text, padding="max_length", truncation=True, max_length=128, return_tensors="pt")
        enc = {k: v.to(self.device) for k, v in enc.items()}
        logits = self.model(price=Xp_t, **enc)
        probs = torch.softmax(logits, dim=-1).cpu().numpy()[0]

        # Map to labels in order [NEG, NEU, POS]
        out = {"NEG": float(probs[0]), "NEU": float(probs[1]), "POS": float(probs[2])}
        best_idx = int(np.argmax(probs))
        best_label = ID2LABEL[best_idx]
        human = {"NEG": "bearish", "NEU": "neutral", "POS": "bullish"}[best_label]
        return {
            "sentiment": human,
            "label": best_label,
            "label_id": best_idx,
            "probs": out,
        }

    @torch.no_grad()
    def predict_with_price_df(self, text: str, symbol: str, price_df: pd.DataFrame) -> dict:
        """
        Predict using a provided realtime price DataFrame for a single symbol.
        Expects columns: ['endtime','close'] (UTC or naive). Uses the latest row.
        """
        if price_df is None or price_df.empty:
            raise ValueError("price_df is empty")
        # Normalize frame to expected format
        df = price_df[["endtime", "close"]].copy()
        df["symbol"] = symbol
        df.rename(columns={"endtime": "endtime", "close": "close"}, inplace=True)
        df["endtime"] = pd.to_datetime(df["endtime"], utc=True)
        # Build features on-the-fly
        feats = make_price_features(df.rename(columns={"endtime": "bar_time"}).rename(columns={"bar_time": "endtime"}).rename(columns={"endtime": "bar_time"}))
        # The above ensures a 'bar_time' exists; simpler: construct expected columns directly
        if feats.empty:
            # Fallback: try direct feature build from minimal DF
            df2 = df.copy()
            df2 = df2.rename(columns={"endtime": "bar_time"})
            feats = make_price_features(df2.rename(columns={"bar_time": "endtime"}))  # reuse path
        row = latest_features_for_symbol(feats, symbol)
        if row is None:
            raise ValueError(f"Could not compute price features from provided price_df for {symbol}")
        Xp = row[self.price_feat_cols].fillna(0.0).values.astype(np.float32)[None, :]
        Xp = self.scaler.transform(Xp)
        Xp_t = torch.from_numpy(Xp).to(self.device)

        enc = self.tok(text, padding="max_length", truncation=True, max_length=128, return_tensors="pt")
        enc = {k: v.to(self.device) for k, v in enc.items()}
        logits = self.model(price=Xp_t, **enc)
        probs = torch.softmax(logits, dim=-1).cpu().numpy()[0]
        out = {"NEG": float(probs[0]), "NEU": float(probs[1]), "POS": float(probs[2])}
        best_idx = int(np.argmax(probs))
        best_label = ID2LABEL[best_idx]
        human = {"NEG": "bearish", "NEU": "neutral", "POS": "bullish"}[best_label]
        return {
            "sentiment": human,
            "label": best_label,
            "label_id": best_idx,
            "probs": out,
        }


def main():
    ap = argparse.ArgumentParser(description="Infer with text+price fusion model")
    ap.add_argument("--text", required=True, help="Input text")
    ap.add_argument("--symbol", required=True, choices=["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT"])
    ap.add_argument("--silver-dir", default=str(ROOT / "data" / "silver"))
    ap.add_argument("--model-dir", default=str(ROOT / "models" / "sentiment_text_price_fusion"))
    args = ap.parse_args()

    predictor = FusionPredictor(args.model_dir)
    res = predictor.predict(args.text, args.symbol, args.silver_dir)
    probs_str = ", ".join(f"{k}:{v:.3f}" for k, v in res["probs"].items())
    print(f"Sentiment: {res['sentiment']} (label={res['label']} id={res['label_id']})\nProbabilities: {probs_str}")


if __name__ == "__main__":
    main()
