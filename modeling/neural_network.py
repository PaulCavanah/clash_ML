# Use neural network to predict win/loss with deck data

#%%
import copy
import random
from dataclasses import dataclass

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score

from pathlib import Path
import os
import pyarrow.parquet as pq
import pandas as pd
import gc

#%%
if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)

# ================================================================
#%%
# Network architecture:

class ClashDeckNN(nn.Module) : 
    """
    340 -> 256 -> 128 -> 64 -> 1
    """

    def __init__(self, input_dim : int, dropout : float = 0.3) : 
        super().__init__()

        self.net = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1) # raw logit
        )

    def forward(self, x) : 
        return self.net(x).squeeze(1)
    
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
# Data loading 

def load_data(num_batches_to_load) : 
    # Load in X and Y data from the parquet files: 
    # Due to card updates, the schema evolves - parquet files may have different columns
    # The approach to merging these schemas is to load in each parquet file individually
    # with its unique one hot columns as a dataframe, add the dataframe to a list,
    # then concatenate the list of dataframes and fill the NaNs with false

    if os.getcwd().split("\\")[-1] == "modeling" : # in modeling directory (jupyter)
        parquet_dir = Path(os.getcwd() + "/../data/parquet")
    elif os.getcwd().split("\\")[-1] == "clash_ML" : # in root directory
        parquet_dir = Path(os.getcwd() + "/data/parquet")

    parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")][0:num_batches_to_load]

    dfs = []

    for filename in parquet_filenames : 
        pf = pq.ParquetFile(parquet_dir / filename)
        columns = pf.schema.names
        X_columns = [column for column in columns if column[0:3] in ("Plr", "Opp")]
        Y_columns = ["player_crowns", "opponent_crowns"]

        # only include ladder and ranked matches
        # filters = [[("gamemode", "==", "Ranked1v1_NewArena")],
        #            [("gamemode", "==", "Ladder")], 
        #            [("gamemode", "==", "Ranked1v1_NewArena2")]]
        filters = [[("gamemode", "==", "Ladder")]]

        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = Y_columns + X_columns, filters = filters)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index = True)

    # Could be a memory bottleneck
    del dfs
    gc.collect()

    df.fillna(0, inplace = True)

    # X and Y
    X = df.iloc[:, 2:]
    y = df["player_crowns"] > df["opponent_crowns"]
    print("Loaded Data with shape:", f"X:{X.shape}, Y:{y.shape}" )

    return X, y

# ================================================================
#%% 
# Load data using function above

random_state = 42

num_batches_to_load = 10

X, y = load_data(num_batches_to_load)

#%%
# 70 / 15 / 15 split

X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.30, random_state=random_state, stratify=y
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

#%%
# Train the model

@dataclass 
class TrainConfig : 
    batch_size: int = 512
    lr: float = 1e-4 
    max_epochs: int = 500
    patience: int = 50

config = TrainConfig()

train_loader = DataLoader(train_ds, batch_size = config.batch_size, shuffle = True)
val_loader = DataLoader(val_ds, batch_size = config.batch_size, shuffle = False)
test_loader = DataLoader(test_ds, batch_size = config.batch_size, shuffle = False)

model = ClashDeckNN(input_dim = X_train_t.shape[1], dropout = 0.3).to(DEVICE)
model, history = train_model(model, train_loader, val_loader, config, DEVICE)

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