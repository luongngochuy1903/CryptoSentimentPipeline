import argparse
from pathlib import Path
import numpy as np
from sklearn.model_selection import train_test_split
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer
import evaluate

from .dataset_builder import build_training_table

ID2LABEL = {0: "NEG", 1: "NEU", 2: "POS"}
LABEL2ID = {v: k for k, v in ID2LABEL.items()}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-dir", default=str(Path(__file__).resolve().parents[2] / "data" / "silver"))
    ap.add_argument("--model-name", dest="model_name", default="distilbert-base-uncased")
    ap.add_argument("--out-dir", dest="out_dir", default=str(Path(__file__).resolve().parents[2] / "models" / "sentiment_distilbert"))
    ap.add_argument("--horizon-min", type=int, default=30)
    ap.add_argument("--up", type=float, default=0.002)
    ap.add_argument("--down", type=float, default=-0.002)
    ap.add_argument("--epochs", type=int, default=2)
    ap.add_argument("--batch-size", type=int, default=16)
    args = ap.parse_args()

    df = build_training_table(args.silver_dir, args.horizon_min, args.up, args.down)
    if df.empty:
        raise SystemExit("No training rows built from silver data.")

    train_df, val_df = train_test_split(df, test_size=0.1, random_state=42, stratify=df["label_id"])

    train_df = train_df.rename(columns={"label_id": "labels"})
    val_df = val_df.rename(columns={"label_id": "labels"})
    train_df["labels"] = train_df["labels"].astype(int)
    val_df["labels"] = val_df["labels"].astype(int)

    tok = AutoTokenizer.from_pretrained(args.model_name)

    def tokenize(batch):
        enc = tok(batch["text"], padding="max_length", truncation=True, max_length=128)
        # use the renamed column
        enc["labels"] = batch["labels"]
        return enc

    # keep only model inputs after tokenization
    train_ds = Dataset.from_pandas(train_df, preserve_index=False)
    val_ds = Dataset.from_pandas(val_df, preserve_index=False)
    train_ds = train_ds.map(tokenize, batched=True, remove_columns=train_ds.column_names)
    val_ds = val_ds.map(tokenize, batched=True, remove_columns=val_ds.column_names)

    model = AutoModelForSequenceClassification.from_pretrained(
        args.model_name, num_labels=3, id2label=ID2LABEL, label2id=LABEL2ID
    )

    acc = evaluate.load("accuracy")
    f1 = evaluate.load("f1")

    def metrics(eval_pred):
        logits, labels = eval_pred
        preds = np.argmax(logits, axis=-1)
        return {
            "accuracy": acc.compute(predictions=preds, references=labels)["accuracy"],
            "f1_macro": f1.compute(predictions=preds, references=labels, average="macro")["f1"],
        }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    training_args = TrainingArguments(
        output_dir=str(out_dir),
        learning_rate=5e-5,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        num_train_epochs=args.epochs,
        weight_decay=0.01,
        logging_steps=50,
        save_steps=200,
        do_eval=True,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        tokenizer=tok,
        compute_metrics=metrics,
    )
    trainer.train()
    trainer.save_model(str(out_dir))
    tok.save_pretrained(str(out_dir))
    print(f"Saved model to: {out_dir}")

if __name__ == "__main__":
    main()