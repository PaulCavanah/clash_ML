
# Take a model that's been pretrained and fine tune it

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
from modeling.architectures import LogitSymmetric_256_128_64_1
architecture = LogitSymmetric_256_128_64_1
from functions.model_training import train_model, evaluate_model, TrainConfig

if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)

#%% 
# Load data to fine tune with

random_state = 42

num_batches_to_load = 115

ladder_minimum = 8500
ladder_maximum = 9000
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">", ladder_minimum), ("player_trophies", "<", ladder_maximum)]]

X, y, feature_names = load_data_from_parquet(num_batches = num_batches_to_load, player_mirror = False, filters = filters)

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

pretrained_network_name = "NNsym_20M_ladder5k10k_pretrain"
finetuned_network_name = "NNsym_20M_ladder9k_finetuned"

models_dir = root_dir / "modeling/model_states/"
models_dir.mkdir(parents = True, exist_ok = True)

# Load pre-trained model 
model = architecture(input_dim = X_train_t.shape[1]).to(DEVICE)
pretrained_state_path = Path(models_dir / f"{pretrained_network_name}.pth")
pretrained_state_dict = torch.load(pretrained_state_path, weights_only = False)
model.load_state_dict(pretrained_state_dict())

# For saving neural network state (fine-tuned)
save_state_path = Path(models_dir / f"{finetuned_network_name}.pth")

# Save neural network features (needed because new cards are periodically added to the game)
features_dir = root_dir / "modeling/model_features/"
features_dir.mkdir(parents = True, exist_ok = True)
features_path = Path(features_dir / f"features_{finetuned_network_name}.pkl")

import pickle
with open(features_path, "wb") as file : 
    pickle.dump(list(feature_names), file) # convert to list so that there aren't pandas versioning issues

config = TrainConfig(
    max_epochs = 10
)

#%%
# Train the model

train_loader = DataLoader(train_ds, batch_size = config.batch_size, shuffle = True)
val_loader = DataLoader(val_ds, batch_size = config.batch_size, shuffle = False)
test_loader = DataLoader(test_ds, batch_size = config.batch_size, shuffle = False)

model, history = train_model(model, train_loader, val_loader, save_state_path, config, DEVICE)

#%% 
# Final evaluation 
model.load_state_dict(pretrained_state_dict())
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
