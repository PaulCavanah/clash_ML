
import pandas as pd 
import copy
import numpy as np
import torch
import os
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score
import torch.nn as nn
from dataclasses import dataclass
from torch.utils.data import DataLoader, TensorDataset
from functions.load_data_from_parquet import stream_onehot

@dataclass 
class TrainConfig : 
    batch_size: int = 512
    lr: float = 1e-4 
    max_epochs: int = 100
    patience: int = 10

class DataStreamConfig() : 
    # Holds properties of data streaming
    def __init__(self, buffer_size = 1_000_000, filters = None, levels = False, base_only = False) : 
        self.buffer_size = buffer_size # the size of the loaded buffers (default = 1 million games at a time)
        self.filters = filters # parquet filters to apply during loading (None applies no filters)
        self.base_only = base_only # if True, only base cards are loaded
        self.levels = levels # if True, the onehot matrix has card levels in place of 1s 
        
        # Count total number of games in the dataset and get number of buffers that will be loaded 
        self.num_games = pd.read_parquet(path = f"{os.getcwd()}/data/parquet", engine = "pyarrow", columns = ["player_crowns"], filters = filters)["player_crowns"].shape[0]
        self.num_buffers = np.ceil(self.num_games / buffer_size).astype(np.int64)
        print(f"Number of available games: {self.num_games}, loading in {self.num_buffers} buffers")

class ObscureConfig() : 
    # Holds properties of obscuring data randomly during training
    def __init__(self, p_obscure = 0.5, p_partial = 0.5) : 
        self.p_obscure = p_obscure # probability that any given column will be obscured (set to 0 in onehot)
        self.p_partial = p_partial # proportion of rows that are partial data (therefore 1-p_row is the proportion of rows that are full data)

# Evaluates neural network model
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

# Training function with early stopping 
def train_model(model, train_loader, val_loader, save_state_path, config: TrainConfig, device) : 
    if os.path.isfile(save_state_path) : 
        state_dict = torch.load(save_state_path, weights_only = False)
        model.load_state_dict(state_dict())
        print("Previous model state loaded")

    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr = config.lr)

    best_val_loss = float("inf")
    best_state = None
    epochs_without_improvement = 0
    history = []

    for epoch in range(1, config.max_epochs + 1) : 
        model.train()
        #running_loss = 0.0

        for xb, yb in train_loader : 
            xb = xb.to(device)
            yb = yb.to(device)

            optimizer.zero_grad()
            logits = model(xb)
            loss = criterion(logits, yb)
            loss.backward()
            optimizer.step()

            #running_loss += loss.item() * xb.size(0)

        #train_loss = running_loss / len(train_loader.dataset)
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

        torch.save(model.state_dict, save_state_path) 


    if best_state is not None:
        model.load_state_dict(best_state)

    return model, history

# Training function with early stopping that is for streaming training data from a 
def stream_train_model(model, val_loader, save_state_path, train_config : TrainConfig, stream_config: DataStreamConfig, obscure_config: ObscureConfig = None, device = "cpu") : 

    if os.path.isfile(save_state_path) : 
        state_dict = torch.load(save_state_path, weights_only = False)
        model.load_state_dict(state_dict())
        print("Previous model state loaded")

    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr = train_config.lr)

    best_val_loss = float("inf")
    best_state = None
    epochs_without_improvement = 0
    history = []

    epoch = 0

    while epoch <= train_config.max_epochs : 
        if epochs_without_improvement >= train_config.patience :
            print(f"Early stopping triggered at epoch {epoch}.")
            break

        model.train()
        data_generator = stream_onehot(buffer_size = stream_config.buffer_size, filters = stream_config.filters, levels = stream_config.levels, base_only = stream_config.base_only)

        for buffer in range(1, stream_config.num_buffers + 1) :
            if epochs_without_improvement >= train_config.patience :
                break

            epoch += 1

            # Create train loader 
            X, y, _ = next(data_generator)

            if obscure_config : 
                mask = np.random.random((X.shape)) < obscure_config.p_obscure # Values across the entire matrix are flipped to 0s (obscured) with probability p_obscure
                mask[np.random.random((X.shape[0], )) > obscure_config.p_partial, :] = False # Rows are protected from this operation at frequency 1-p_partial 
                X[mask] = 0 

            train_ds = TensorDataset(torch.tensor(X, dtype = torch.float32), torch.tensor(y, dtype = torch.float32))
            train_loader = DataLoader(train_ds, batch_size = train_config.batch_size, shuffle = True)
            
            for xb, yb in train_loader : 
                xb = xb.to(device)
                yb = yb.to(device)

                optimizer.zero_grad()
                logits = model(xb)
                loss = criterion(logits, yb)
                loss.backward()
                optimizer.step()

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


            torch.save(model.state_dict, save_state_path) 

    if best_state is not None:
        model.load_state_dict(best_state)
        torch.save(model.state_dict, save_state_path) 

    return model, history