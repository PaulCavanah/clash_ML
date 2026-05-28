# Train a neural network by streaming data from the parquet database

#%%
# Imports
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

from functions.load_data_from_parquet import stream_onehot
from modeling.architectures import LogitSymmetric_256_128_64_1
architecture = LogitSymmetric_256_128_64_1
from functions.model_training import train_model, evaluate_model, TrainConfig

if torch.cuda.is_available() :
    DEVICE = torch.device("cuda")
else : 
    DEVICE = torch.device("cpu")

print("Using device: ", DEVICE)

#%%
# Create data generator, where next(generator) gets the next buffer of data

ladder_minimum = 12000
ranked_minimum = 0
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", ranked_minimum)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", ranked_minimum)]]

data_generator = stream_onehot(buffer_size = 100_000, filters = filters)
