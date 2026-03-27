# Use neural network to predict win/loss with deck data

#%%
import copy
from dataclasses import dataclass

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score

from pathlib import Path
import os
import sys
import gc

# Make clash_ML (root) the current directory and add it to path
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = Path("\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]]))
os.chdir(root_dir)
sys.path.append(os.getcwd())

from functions.load_data_from_parquet import load_data_from_parquet
from modeling.architectures import Logit_in_256_128_64_1

#%%
if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)
    
# ================================================================
#%%
# Model evaluation

def evaluate_model(model, loader, device):
    model.eval()

    all_probs = []
    all_preds = []
    all_targets = []
    total_loss = 0.0

    criterion = nn.BCEWithLogitsLoss()

    with torch.no_grad():
        for xb, yb in loader:
            xb = xb.to(device)
            yb = yb.to(device)

            logits = model(xb)
            loss = criterion(logits, yb)
            probs = torch.sigmoid(logits)
            preds = (probs >= 0.5).float()

            total_loss += loss.item() * xb.size(0)
            all_probs.append(probs.cpu().numpy())
            all_preds.append(preds.cpu().numpy())
            all_targets.append(yb.cpu().numpy())

    y_true = np.concatenate(all_targets)
    y_prob = np.concatenate(all_probs)
    y_pred = np.concatenate(all_preds)

    avg_loss = total_loss / len(loader.dataset)
    acc = accuracy_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    # AUC requires both classes present
    try:
        auc = roc_auc_score(y_true, y_prob)
    except ValueError:
        auc = np.nan

    return {
        "loss": avg_loss,
        "accuracy": acc,
        "auc": auc,
        "f1": f1,
    }

# ================================================================
#%%
# Training function with early stopping 

@dataclass 
class TrainConfig : 
    batch_size: int = 512
    lr: float = 1e-4 
    max_epochs: int = 100
    patience: int = 10

def train_model(model, train_loader, val_loader, config: TrainConfig, device) : 
    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr = config.lr)

    best_val_loss = float("inf")
    best_state = None
    epochs_without_improvement = 0
    history = []

    for epoch in range(1, config.max_epochs + 1) : 
        model.train()
        running_loss = 0.0

        for xb, yb in train_loader : 
            xb = xb.to(device)
            yb = yb.to(device)

            optimizer.zero_grad()
            logits = model(xb)
            loss = criterion(logits, yb)
            loss.backward()
            optimizer.step()

            running_loss += loss.item() * xb.size(0)

        train_loss = running_loss / len(train_loader.dataset)
        train_metrics = evaluate_model(model, train_loader, device)
        val_metrics = evaluate_model(model, val_loader, device)

        history.append({
                "epoch": epoch,
                "train_loss": train_metrics["loss"],
                "train_acc": train_metrics["accuracy"],
                "train_auc": train_metrics["auc"],
                "train_f1": train_metrics["f1"],
                "val_loss": val_metrics["loss"],
                "val_acc": val_metrics["accuracy"],
                "val_auc": val_metrics["auc"],
                "val_f1": val_metrics["f1"],
            })
        
        print(
                f"Epoch {epoch:03d} | "
                f"train_loss={train_metrics['loss']:.4f} "
                f"train_acc={train_metrics['accuracy']:.4f} "
                f"val_loss={val_metrics['loss']:.4f} "
                f"val_acc={val_metrics['accuracy']:.4f}"
            )

        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            best_state = copy.deepcopy(model.state_dict())
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1

        if epochs_without_improvement >= config.patience :
            print(f"Early stopping triggered at epoch {epoch}.")
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    return model, history

# ================================================================
#%% 
# Load data 

random_state = 42

num_batches_to_load = 10

X, y, feature_names = load_data_from_parquet(num_batches = num_batches_to_load, player_swap = True)

#%%
# 85 / 7.5 / 7.5 split

X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.15, random_state=random_state, stratify=y
)

X_val, X_test, y_val, y_test = train_test_split(
    X_temp, y_temp, test_size=0.50, random_state=random_state, stratify=y_temp
)

# Clear up some memory
del X_temp, y_temp, X, y
gc.collect()

print(f"train: {y_train.shape}, val: {y_val.shape}, test: {y_test.shape}")

#%%
# Tensor conversion

X_train_t = torch.tensor(X_train.to_numpy(), dtype=torch.float32)
y_train_t = torch.tensor(y_train.to_numpy(), dtype=torch.float32)

X_val_t = torch.tensor(X_val.to_numpy(), dtype=torch.float32)
y_val_t = torch.tensor(y_val.to_numpy(), dtype=torch.float32)

X_test_t = torch.tensor(X_test.to_numpy(), dtype=torch.float32)
y_test_t = torch.tensor(y_test.to_numpy(), dtype=torch.float32)

train_ds = TensorDataset(X_train_t, y_train_t)
val_ds = TensorDataset(X_val_t, y_val_t)
test_ds = TensorDataset(X_test_t, y_test_t)

# Clear up some memory
del X_train, X_val, y_train, y_val
gc.collect()

# ================================================================
#%%
# Train the model

config = TrainConfig()

train_loader = DataLoader(train_ds, batch_size = config.batch_size, shuffle = True)
val_loader = DataLoader(val_ds, batch_size = config.batch_size, shuffle = False)
test_loader = DataLoader(test_ds, batch_size = config.batch_size, shuffle = False)

model = Logit_in_256_128_64_1(input_dim = X_train_t.shape[1], dropout = 0.3).to(DEVICE)
model, history = train_model(model, train_loader, val_loader, config, DEVICE)

# ================================================================
#%% 
# Final evaluation 
train_metrics = evaluate_model(model, train_loader, DEVICE)
val_metrics = evaluate_model(model, val_loader, DEVICE)
test_metrics = evaluate_model(model, test_loader, DEVICE)

# %%
print("\nFinal metrics")
print("Train:", train_metrics)
print("Val:  ", val_metrics)
print("Test: ", test_metrics)

#%% 
# Save neural network state
# models_dir = root_dir / "modeling/models/"
# models_dir.mkdir(parents = True, exist_ok = True)
# save_path = Path(models_dir / "test.pth")

# torch.save(model.state_dict, save_path) 