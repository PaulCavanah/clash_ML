
import copy
import numpy as np
import torch
import os
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score
import torch.nn as nn
from dataclasses import dataclass

@dataclass 
class TrainConfig : 
    batch_size: int = 512
    lr: float = 1e-4 
    max_epochs: int = 100
    patience: int = 10

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

        torch.save(model.state_dict, save_state_path) 


    if best_state is not None:
        model.load_state_dict(best_state)

    return model, history