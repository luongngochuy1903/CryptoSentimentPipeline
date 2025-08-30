#!/usr/bin/env python3
"""
Train a simple text + price fusion classifier:
- Text -> frozen transformer -> mean pooled embeddings
- Price -> engineered features (returns/volatility/momentum)
- Concatenate -> MLP classifier (trained)

This improves robustness by letting price context inform the label.
"""
from __future__ import annotations

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_class_weight

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ml.dataset_builder import build_training_table, load_prices
from src.ml.price_features import make_price_features
from transformers import AutoTokenizer, AutoModel


ID2LABEL = {0: "NEG", 1: "NEU", 2: "POS"}


class TextPriceDataset(Dataset):
    def __init__(self, df: pd.DataFrame, tok, price_feats: pd.DataFrame, scaler: StandardScaler | None, max_len: int = 128):
        self.df = df.reset_index(drop=True)
        self.tok = tok
        self.max_len = max_len
        # join price feats by (symbol, bar_time ~= created_at floored to 5min)
        feats = price_feats.copy()
        feats = feats.rename(columns={"bar_time": "created_at"})
        feats["created_at"] = pd.to_datetime(feats["created_at"], utc=True)
        dfx = self.df.copy()
        dfx["created_at"] = pd.to_datetime(dfx["created_at"], utc=True)
        # nearest join within same timestamp (assumes aligned bars)
        m = pd.merge_asof(
            dfx.sort_values("created_at"),
            feats.sort_values("created_at"),
            by="symbol",
            on="created_at",
            direction="nearest",
            tolerance=pd.Timedelta("10min"),
        )
        self.price_cols = [c for c in m.columns if c in ("ret_1", "ret_3", "ret_6", "vol_12", "vol_24", "mom_z24")]
        self.Xp = m[self.price_cols].fillna(0.0).values.astype(np.float32)
        # scale if provided
        if scaler is not None:
            self.Xp = scaler.transform(self.Xp)
        self.texts = self.df["text"].astype(str).tolist()
        self.labels = self.df["label_id"].astype(int).tolist()

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx: int):
        enc = self.tok(
            self.texts[idx],
            padding="max_length",
            truncation=True,
            max_length=self.max_len,
            return_tensors="pt",
        )
        item = {k: v.squeeze(0) for k, v in enc.items()}
        item["price"] = torch.from_numpy(self.Xp[idx])
        item["labels"] = torch.tensor(self.labels[idx], dtype=torch.long)
        return item


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

    def forward(self, input_ids=None, attention_mask=None, price=None, labels=None):
        out = self.text_model(input_ids=input_ids, attention_mask=attention_mask)
        # mean pool last_hidden_state
        last = out.last_hidden_state  # [B, T, H]
        mask = attention_mask.unsqueeze(-1).float()
        summed = (last * mask).sum(dim=1)
        denom = mask.sum(dim=1).clamp(min=1e-6)
        text_vec = summed / denom
        x = torch.cat([text_vec, price], dim=1)
        x = self.head(self.dropout(self.act(self.proj(x))))
        loss = None
        if labels is not None:
            loss = nn.CrossEntropyLoss()(x, labels)
        return {"loss": loss, "logits": x}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default=str(ROOT / "data" / "silver"))
    ap.add_argument("--model-name", default="distilbert-base-uncased")
    ap.add_argument("--epochs", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--lr", type=float, default=2e-4)
    ap.add_argument("--freeze-text", action="store_true")
    ap.add_argument("--out-dir", default=str(ROOT / "models" / "sentiment_text_price_fusion"))
    args = ap.parse_args()

    # Build supervised table (labels from price), load price series, make features
    table = build_training_table(args.silver_dir, horizon_min=30, up=0.003, down=-0.003)
    if table.empty:
        raise SystemExit("No labeled rows.")
    prices = load_prices(args.silver_dir)
    feats = make_price_features(prices)

    # Train/val split
    train_df, val_df = train_test_split(table, test_size=0.2, random_state=42, stratify=table["label_id"])

    # Fit scaler on training price features
    feat_cols = ["ret_1", "ret_3", "ret_6", "vol_12", "vol_24", "mom_z24"]
    scaler = StandardScaler()
    # quick fit using the merge logic inside dataset
    tok = AutoTokenizer.from_pretrained(args.model_name)
    tmp_train = TextPriceDataset(train_df, tok, feats, scaler=None)
    if len(tmp_train) == 0:
        raise SystemExit("No training samples after joining with price features.")
    Xp_train = tmp_train.Xp
    scaler.fit(Xp_train)

    train_ds = TextPriceDataset(train_df, tok, feats, scaler)
    val_ds = TextPriceDataset(val_df, tok, feats, scaler)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = FusionClassifier(args.model_name, price_dim=len(feat_cols), freeze_text=args.freeze_text).to(device)

    # class weights
    y = train_df["label_id"].values
    cls_w = compute_class_weight("balanced", classes=np.unique(y), y=y)
    cls_w = torch.tensor(cls_w, dtype=torch.float32, device=device)
    criterion = nn.CrossEntropyLoss(weight=cls_w)
    optimizer = torch.optim.AdamW(filter(lambda p: p.requires_grad, model.parameters()), lr=args.lr)

    train_loader = DataLoader(train_ds, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=args.batch_size)

    def eval_loop():
        model.eval()
        correct, total, loss_sum = 0, 0, 0.0
        with torch.no_grad():
            for batch in val_loader:
                batch = {k: (v.to(device) if hasattr(v, "to") else v) for k, v in batch.items()}
                out = model(**{k: batch[k] for k in ["input_ids", "attention_mask", "price", "labels"]})
                logits = out["logits"]
                loss = criterion(logits, batch["labels"])  # ensure weighted loss
                preds = logits.argmax(dim=1)
                correct += (preds == batch["labels"]).sum().item()
                total += preds.numel()
                loss_sum += float(loss.item())
        return (correct / max(1, total)), (loss_sum / max(1, len(val_loader)))

    best_acc = 0.0
    for epoch in range(1, args.epochs + 1):
        model.train()
        for batch in train_loader:
            batch = {k: (v.to(device) if hasattr(v, "to") else v) for k, v in batch.items()}
            out = model(**{k: batch[k] for k in ["input_ids", "attention_mask", "price", "labels"]})
            logits = out["logits"]
            loss = criterion(logits, batch["labels"])  # use weighted loss
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        acc, vl = eval_loop()
        print(f"Epoch {epoch}: val_acc={acc:.3f} val_loss={vl:.3f}")
        if acc > best_acc:
            best_acc = acc

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # save torch module and scaler/tokenizer
    torch.save({
        "state_dict": model.state_dict(),
        "model_name": args.model_name,
        "price_feat_cols": feat_cols,
        "freeze_text": args.freeze_text,
    }, out_dir / "fusion_model.pt")
    import joblib
    joblib.dump(scaler, out_dir / "price_scaler.pkl")
    tok.save_pretrained(out_dir)
    print(f"Saved fusion model to: {out_dir}")


if __name__ == "__main__":
    main()
