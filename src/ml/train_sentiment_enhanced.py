#!/usr/bin/env python3
"""
Enhanced sentiment model training script with accuracy improvements.
This script addresses common issues that cause poor model performance:
1. Class imbalance handling
2. Better hyperparameters
3. More robust data preprocessing
4. Evaluation metrics
5. Learning rate scheduling
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.utils.class_weight import compute_class_weight
from datasets import Dataset
from transformers import (
    AutoTokenizer, 
    AutoModelForSequenceClassification, 
    TrainingArguments, 
    Trainer,
    EarlyStoppingCallback,
    get_linear_schedule_with_warmup
)
import evaluate
import torch
from typing import Dict, Any
import warnings
warnings.filterwarnings("ignore")

from dataset_builder import build_training_table

ID2LABEL = {0: "NEG", 1: "NEU", 2: "POS"}
LABEL2ID = {v: k for k, v in ID2LABEL.items()}

class WeightedTrainer(Trainer):
    """Custom trainer with class weights for imbalanced datasets."""
    
    def __init__(self, *args, class_weights=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_weights = class_weights
    
    def compute_loss(self, model, inputs, return_outputs=False):
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get('logits')
        
        if self.class_weights is not None:
            # Apply class weights
            weight = torch.tensor(self.class_weights, dtype=torch.float32, device=labels.device)
            loss_fct = torch.nn.CrossEntropyLoss(weight=weight)
            loss = loss_fct(logits.view(-1, self.model.config.num_labels), labels.view(-1))
        else:
            loss = outputs.loss
            
        return (loss, outputs) if return_outputs else loss

def analyze_dataset(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze the training dataset for insights."""
    print(f"\n📊 DATASET ANALYSIS")
    print(f"=" * 50)
    
    print(f"📈 Total samples: {len(df):,}")
    print(f"📝 Unique texts: {df['text'].nunique():,}")
    print(f"🎯 Label distribution:")
    
    label_counts = df['label_id'].value_counts().sort_index()
    label_names = {0: "NEG (Bearish)", 1: "NEU (Neutral)", 2: "POS (Bullish)"}
    
    for label_id, count in label_counts.items():
        percentage = count / len(df) * 100
        print(f"   {label_names[label_id]}: {count:,} ({percentage:.1f}%)")
    
    # Check for class imbalance
    min_class = label_counts.min()
    max_class = label_counts.max()
    imbalance_ratio = max_class / min_class if min_class > 0 else float('inf')
    
    if imbalance_ratio > 3:
        print(f"⚠️  Class imbalance detected! Ratio: {imbalance_ratio:.1f}:1")
        print(f"   This may cause bias toward the majority class.")
    else:
        print(f"✅ Class balance is reasonable. Ratio: {imbalance_ratio:.1f}:1")
    
    # Text length analysis
    text_lengths = df['text'].str.len()
    print(f"\n📏 Text length statistics:")
    print(f"   Mean: {text_lengths.mean():.0f} chars")
    print(f"   Median: {text_lengths.median():.0f} chars")
    print(f"   Max: {text_lengths.max():.0f} chars")
    print(f"   Min: {text_lengths.min():.0f} chars")
    
    return {
        "total_samples": len(df),
        "label_counts": label_counts.to_dict(),
        "imbalance_ratio": imbalance_ratio,
        "text_length_stats": {
            "mean": text_lengths.mean(),
            "median": text_lengths.median(),
            "max": text_lengths.max(),
            "min": text_lengths.min()
        }
    }

def preprocess_text(text: str) -> str:
    """Enhanced text preprocessing for better model performance."""
    if not isinstance(text, str):
        return ""
    
    # Basic cleaning
    text = text.strip()
    
    # Remove excessive whitespace
    text = ' '.join(text.split())
    
    # Normalize common crypto abbreviations for consistency
    crypto_replacements = {
        r'\$BTC\b': 'Bitcoin',
        r'\$ETH\b': 'Ethereum', 
        r'\$BNB\b': 'BNB',
        r'\$SOL\b': 'Solana',
        r'\$XRP\b': 'XRP',
        r'\bcrypto\b': 'cryptocurrency',
        r'\bhodl\b': 'hold',
        r'\bfomo\b': 'fear of missing out',
        r'\bfud\b': 'fear uncertainty doubt',
        r'\bdefi\b': 'decentralized finance',
        r'\bnft\b': 'NFT',
        r'\bdapp\b': 'decentralized application',
    }
    
    import re
    for pattern, replacement in crypto_replacements.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    return text

def balance_dataset(df: pd.DataFrame, method: str = "oversample") -> pd.DataFrame:
    """Balance the dataset to improve model performance."""
    print(f"\n⚖️  BALANCING DATASET using {method}")
    print(f"=" * 50)
    
    label_counts = df['label_id'].value_counts()
    print(f"Before balancing: {dict(label_counts)}")
    
    if method == "oversample":
        # Oversample minority classes to match majority class
        max_count = label_counts.max()
        balanced_dfs = []
        
        for label in [0, 1, 2]:
            label_df = df[df['label_id'] == label]
            if len(label_df) < max_count:
                # Oversample with replacement
                label_df = label_df.sample(n=max_count, replace=True, random_state=42)
            balanced_dfs.append(label_df)
        
        balanced_df = pd.concat(balanced_dfs, ignore_index=True).sample(frac=1, random_state=42)
        
    elif method == "undersample":
        # Undersample majority classes to match minority class
        min_count = label_counts.min()
        balanced_dfs = []
        
        for label in [0, 1, 2]:
            label_df = df[df['label_id'] == label]
            if len(label_df) > min_count:
                # Undersample
                label_df = label_df.sample(n=min_count, random_state=42)
            balanced_dfs.append(label_df)
        
        balanced_df = pd.concat(balanced_dfs, ignore_index=True).sample(frac=1, random_state=42)
    
    else:  # weighted - no actual resampling, just return original
        balanced_df = df
    
    new_counts = balanced_df['label_id'].value_counts()
    print(f"After balancing: {dict(new_counts)}")
    
    return balanced_df

def create_advanced_metrics():
    """Create comprehensive evaluation metrics."""
    accuracy = evaluate.load("accuracy")
    f1 = evaluate.load("f1")
    precision = evaluate.load("precision")
    recall = evaluate.load("recall")
    
    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        
        return {
            "accuracy": accuracy.compute(predictions=predictions, references=labels)["accuracy"],
            "f1_macro": f1.compute(predictions=predictions, references=labels, average="macro")["f1"],
            "f1_weighted": f1.compute(predictions=predictions, references=labels, average="weighted")["f1"],
            "precision_macro": precision.compute(predictions=predictions, references=labels, average="macro")["precision"],
            "recall_macro": recall.compute(predictions=predictions, references=labels, average="macro")["recall"],
            "f1_per_class": f1.compute(predictions=predictions, references=labels, average=None)["f1"],
        }
    
    return compute_metrics

def main():
    ap = argparse.ArgumentParser(description="Enhanced sentiment model training")
    ap.add_argument("--silver-dir", default=str(Path(__file__).resolve().parents[2] / "data" / "silver"))
    ap.add_argument("--model-name", dest="model_name", default="distilbert-base-uncased")
    ap.add_argument("--out-dir", dest="out_dir", default=str(Path(__file__).resolve().parents[2] / "models" / "sentiment_distilbert_enhanced"))
    ap.add_argument("--horizon-min", type=int, default=30)
    ap.add_argument("--up", type=float, default=0.003, help="Threshold for positive sentiment (higher = more selective)")
    ap.add_argument("--down", type=float, default=-0.003, help="Threshold for negative sentiment (lower = more selective)")
    ap.add_argument("--epochs", type=int, default=5, help="Number of training epochs")
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--learning-rate", type=float, default=2e-5, help="Learning rate")
    ap.add_argument("--warmup-steps", type=int, default=500, help="Warmup steps for learning rate scheduler")
    ap.add_argument("--weight-decay", type=float, default=0.01)
    ap.add_argument("--balance-method", choices=["oversample", "undersample", "weighted", "none"], 
                    default="weighted", help="Method to handle class imbalance")
    ap.add_argument("--max-length", type=int, default=256, help="Maximum sequence length")
    ap.add_argument("--early-stopping", action="store_true", help="Use early stopping")
    ap.add_argument("--patience", type=int, default=3, help="Early stopping patience")
    args = ap.parse_args()

    print(f"🚀 ENHANCED SENTIMENT MODEL TRAINING")
    print(f"=" * 60)
    print(f"📂 Silver data directory: {args.silver_dir}")
    print(f"🤖 Base model: {args.model_name}")
    print(f"💾 Output directory: {args.out_dir}")
    print(f"⚖️  Balance method: {args.balance_method}")
    print(f"🎯 Thresholds - Up: {args.up}, Down: {args.down}")

    # Build training data
    print(f"\n📊 Building training dataset...")
    df = build_training_table(args.silver_dir, args.horizon_min, args.up, args.down)
    if df.empty:
        raise SystemExit("❌ No training rows built from silver data.")

    # Analyze dataset
    dataset_stats = analyze_dataset(df)
    
    # Preprocess text
    print(f"\n🔧 Preprocessing text...")
    df['text'] = df['text'].apply(preprocess_text)
    
    # Remove empty texts
    df = df[df['text'].str.len() > 0].reset_index(drop=True)
    print(f"✅ Kept {len(df):,} samples after preprocessing")

    # Balance dataset if needed
    if args.balance_method != "none":
        df = balance_dataset(df, args.balance_method)

    # Calculate class weights for weighted training
    class_weights = None
    if args.balance_method == "weighted":
        labels = df['label_id'].values
        class_weights = compute_class_weight('balanced', classes=np.unique(labels), y=labels)
        print(f"🎯 Class weights: {dict(zip(np.unique(labels), class_weights))}")

    # Split data with stratification
    train_df, val_df = train_test_split(
        df, 
        test_size=0.15,  # Slightly larger validation set
        random_state=42, 
        stratify=df["label_id"]
    )

    print(f"\n📈 Data splits:")
    print(f"   Training: {len(train_df):,} samples")
    print(f"   Validation: {len(val_df):,} samples")

    # Prepare datasets
    train_df = train_df.rename(columns={"label_id": "labels"})
    val_df = val_df.rename(columns={"label_id": "labels"})
    train_df["labels"] = train_df["labels"].astype(int)
    val_df["labels"] = val_df["labels"].astype(int)

    # Load tokenizer
    print(f"\n🔤 Loading tokenizer: {args.model_name}")
    tok = AutoTokenizer.from_pretrained(args.model_name)

    def tokenize(batch):
        enc = tok(
            batch["text"], 
            padding="max_length", 
            truncation=True, 
            max_length=args.max_length
        )
        enc["labels"] = batch["labels"]
        return enc

    # Create datasets
    train_ds = Dataset.from_pandas(train_df, preserve_index=False)
    val_ds = Dataset.from_pandas(val_df, preserve_index=False)
    train_ds = train_ds.map(tokenize, batched=True, remove_columns=train_ds.column_names)
    val_ds = val_ds.map(tokenize, batched=True, remove_columns=val_ds.column_names)

    # Load model
    print(f"\n🤖 Loading model: {args.model_name}")
    model = AutoModelForSequenceClassification.from_pretrained(
        args.model_name, 
        num_labels=3, 
        id2label=ID2LABEL, 
        label2id=LABEL2ID
    )

    # Set up metrics
    compute_metrics = create_advanced_metrics()

    # Create output directory
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Enhanced training arguments
    training_args = TrainingArguments(
        output_dir=str(out_dir),
        learning_rate=args.learning_rate,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        num_train_epochs=args.epochs,
        weight_decay=args.weight_decay,
        warmup_steps=args.warmup_steps,
        logging_steps=50,
        save_steps=500,
        eval_steps=500,
        do_eval=True,
        # Removed evaluation/save strategy and load_best_model_at_end for compatibility
        remove_unused_columns=True,
        push_to_hub=False,
        report_to=None,  # Disable wandb/tensorboard
        dataloader_num_workers=0,  # Avoid multiprocessing issues
    )

    # Create trainer
    if args.balance_method == "weighted" and class_weights is not None:
        trainer = WeightedTrainer(
            model=model,
            args=training_args,
            train_dataset=train_ds,
            eval_dataset=val_ds,
            tokenizer=tok,
            compute_metrics=compute_metrics,
            class_weights=class_weights,
        )
    else:
        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_ds,
            eval_dataset=val_ds,
            tokenizer=tok,
            compute_metrics=compute_metrics,
        )

    # Add early stopping if requested
    if args.early_stopping:
        trainer.add_callback(EarlyStoppingCallback(early_stopping_patience=args.patience))
        print(f"🛑 Early stopping enabled with patience: {args.patience}")

    # Train model
    print(f"\n🏋️  Training model...")
    print(f"=" * 60)
    
    trainer.train()

    # Evaluate model
    print(f"\n📊 Evaluating model...")
    eval_results = trainer.evaluate()
    
    print(f"\n🎯 FINAL EVALUATION RESULTS:")
    print(f"=" * 40)
    for metric, value in eval_results.items():
        if isinstance(value, (int, float)):
            if metric.startswith("eval_f1_per_class"):
                continue  # Skip per-class F1 for summary
            print(f"   {metric}: {value:.4f}")

    # Save model and tokenizer
    trainer.save_model(str(out_dir))
    tok.save_pretrained(str(out_dir))
    
    # Save training info
    training_info = {
        "dataset_stats": dataset_stats,
        "training_args": vars(args),
        "eval_results": eval_results,
        "class_weights": class_weights.tolist() if class_weights is not None else None,
    }
    
    import json
    with open(out_dir / "training_info.json", "w") as f:
        json.dump(training_info, f, indent=2, default=str)

    print(f"\n✅ Model saved to: {out_dir}")
    print(f"📋 Training info saved to: {out_dir / 'training_info.json'}")
    
    # Provide recommendations
    print(f"\n💡 RECOMMENDATIONS FOR FURTHER IMPROVEMENT:")
    print(f"=" * 50)
    
    if dataset_stats["imbalance_ratio"] > 3:
        print(f"📊 Consider collecting more balanced training data")
    
    if eval_results.get("eval_accuracy", 0) < 0.7:
        print(f"🎯 Try adjusting thresholds (--up/--down) to create clearer labels")
        print(f"📈 Consider more training epochs or different learning rate")
    
    if eval_results.get("eval_f1_macro", 0) < 0.6:
        print(f"⚖️  Try different balance methods (oversample/undersample)")
        print(f"🤖 Consider using a larger pre-trained model (roberta-base)")

    print(f"\n🎉 Training completed successfully!")

if __name__ == "__main__":
    main()
