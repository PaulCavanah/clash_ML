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

from functions.load_data_from_parquet import load_level_onehot
from modeling.architectures import LogitSymmetric_256_128_64_1
architecture = LogitSymmetric_256_128_64_1
from functions.model_training import train_model, evaluate_model, TrainConfig

if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)
    
# ================================================================
#%% 
# Load data 

random_state = 42

num_batches_to_load = 50

ladder_minimum = 5000
ladder_maximum = 10000
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">", ladder_minimum), ("player_trophies", "<", ladder_maximum)]]

X, y, feature_names = load_level_onehot(num_batches = num_batches_to_load, filters = filters)

#%%
# 90 / 5 / 5 split

X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.10, random_state=random_state, stratify=y
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

if type(X_train) == np.ndarray : # already converted to numpy
    X_train_t = torch.tensor(X_train, dtype=torch.float32)
    y_train_t = torch.tensor(y_train, dtype=torch.float32)

    X_val_t = torch.tensor(X_val, dtype=torch.float32)
    y_val_t = torch.tensor(y_val, dtype=torch.float32)

    X_test_t = torch.tensor(X_test, dtype=torch.float32)
    y_test_t = torch.tensor(y_test, dtype=torch.float32)

else : # A pandas type
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

network_name = "NNsym_2M_ladder5k10k_levels"

# For saving neural network state
models_dir = root_dir / "modeling/model_states/"
models_dir.mkdir(parents = True, exist_ok = True)
save_state_path = Path(models_dir / f"{network_name}.pth")

# Save neural network features (needed because new cards are periodically added to the game)
features_dir = root_dir / "modeling/model_features/"
features_dir.mkdir(parents = True, exist_ok = True)
features_path = Path(features_dir / f"features_{network_name}.pkl")

import pickle
with open(features_path, "wb") as file : 
    pickle.dump(list(feature_names), file) # convert to list so that there aren't pandas versioning issues

#%%
# Train the model

config = TrainConfig()

train_loader = DataLoader(train_ds, batch_size = config.batch_size, shuffle = True)
val_loader = DataLoader(val_ds, batch_size = config.batch_size, shuffle = False)
test_loader = DataLoader(test_ds, batch_size = config.batch_size, shuffle = False)

model = architecture(input_dim = X_train_t.shape[1]).to(DEVICE)
model, history = train_model(model, train_loader, val_loader, save_state_path, config, DEVICE)

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

# %%
torch.save(model.state_dict, save_state_path) 
