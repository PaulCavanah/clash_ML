# Train a neural network on whether decks are in-distribution vs. random by streaming data from the parquet database

#%%
# Imports
import torch
from torch.utils.data import DataLoader, TensorDataset

from pathlib import Path
import os
import sys
import gc
import numpy as np 

# Make clash_ML (root) the current directory and add it to path
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = Path("\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]]))
os.chdir(root_dir)
sys.path.append(os.getcwd())

from functions.load_data_from_parquet import load_decks
from functions.get_random_deck import random_deck
from modeling.architectures import Logit_in_256_128_64_1
architecture = Logit_in_256_128_64_1
from functions.model_training import TrainConfig, DataStreamConfig, ObscureConfig, stream_train_model_decks 

if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)

#%%
# Set filters for data

ladder_minimum = 13000
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", 0)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", 0)]]

#%%
# Create a static validation set by getting half random decks and half real decks
val_size = 500_000 # Half random decks and half real decks 

# Get real decks and randomly generated decks 
X_real, _, feature_names = load_decks(val_size//2, filters = filters, base_only = True, unique = True)
C = len(feature_names) // 2 
X_random = random_deck(range(C), feature_names, num_decks = val_size//2, output_format = "OH half")
X_val = np.concatenate([X_real, X_random])

# Generate labels: 
y_val = np.concatenate([np.ones(val_size//2), np.zeros(val_size//2)])

val_ds = TensorDataset(torch.tensor(X_val, dtype = torch.float32), torch.tensor(y_val, dtype = torch.float32))

del X_val # Free up a bit of space
gc.collect()

#%%
# Create model and configs for training as well as the validation loader
model = architecture(input_dim = C).to(DEVICE)

train_config = TrainConfig(batch_size = 512, max_epochs = 100, patience = 10)
stream_config = DataStreamConfig(buffer_size = 10_000_000, filters = filters, base_only = True, unique = True)
obscure_config = ObscureConfig(p_obscure = 0.5, p_partial = 0.5)

val_loader = DataLoader(val_ds, batch_size = train_config.batch_size, shuffle = True)

#%%
# Save NN details 
network_name = "NNindist_35Mobscbase_13kranked"

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
    pickle.dump(list(feature_names[:C]), file) # convert feature_names to list so that there aren't pandas versioning issues (e.g. with numpy or Pandas series)

#%%
# Train model: 
model, history = stream_train_model_decks(model, val_loader, save_state_path, train_config, stream_config, obscure_config, device = DEVICE)

# %%
