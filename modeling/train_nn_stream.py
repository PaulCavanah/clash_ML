# Train a neural network by streaming data from the parquet database

#%%
# Imports
import torch
from torch.utils.data import DataLoader, TensorDataset

from pathlib import Path
import os
import sys
import gc

# Make clash_ML (root) the current directory and add it to path
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = Path("\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]]))
os.chdir(root_dir)
sys.path.append(os.getcwd())

from functions.load_data_from_parquet import load_random_games
from modeling.architectures import LogitSymmetric_256_128_64_1
architecture = LogitSymmetric_256_128_64_1
from functions.model_training import TrainConfig, DataStreamConfig, ObscureConfig, stream_train_model 

if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)

#%%
# Set filters for data

ladder_minimum = 12000
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", 0)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", 0)]]

#%%
# Create a static validation set via random sampling across the streamed database 
val_size = 1_000_000
X_val, y_val, feature_names = load_random_games(val_size, filters = filters, base_only = True)
val_ds = TensorDataset(torch.tensor(X_val, dtype = torch.float32), torch.tensor(y_val, dtype = torch.float32))

del X_val # Free up a bit of space
gc.collect()

#%% 
# Create model and configs for training as well as the validation loader
model = architecture(input_dim = len(feature_names)).to(DEVICE)

train_config = TrainConfig(batch_size = 512, max_epochs = 100, patience = 10)
stream_config = DataStreamConfig(buffer_size = 10_000_000, filters = filters, base_only = True)
obscure_config = ObscureConfig(p_obscure = 0.5, p_partial = 0.5)

val_loader = DataLoader(val_ds, batch_size = train_config.batch_size, shuffle = True)

#%%
# Save NN details 
network_name = "NNsym_60Mobsc_base_12kranked"

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
    pickle.dump(list(feature_names), file) # convert feature_names to list so that there aren't pandas versioning issues (e.g. with numpy or Pandas series)

#%%
# Approach: 
model, history = stream_train_model(model, val_loader, save_state_path, train_config, stream_config, obscure_config, device = DEVICE)

# %%
