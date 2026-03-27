# Use lightGBM algorithm to train a classifier to predict win/loss

#%% 
import lightgbm as lgb
from scipy import sparse
import gc
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score, classification_report

import pyarrow.parquet as pq
import pandas as pd

import sys
from pathlib import Path
import os

# Make clash_ML (root) the current directory and add it to path
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = Path("\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]]))
os.chdir(root_dir)
sys.path.append(os.getcwd())

from functions.load_data_from_parquet import load_data_from_parquet 

random_state = 42 # for splits and model 

#%% 
# Load data

num_batches_to_load = 1
X, y, feature_names = load_data_from_parquet(num_batches = num_batches_to_load, player_swap = True)

#%% 
# Create train and test splits
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size = 0.1, random_state = random_state)
#print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

del X # Major consumer of memory
del y # Minor consumer of memory
gc.collect()

#%%

# Create LightGBM dataset object 
train_data = lgb.Dataset(x_train, label=y_train)

#%%

# Set parameters for regression
params = {
    "objective": "binary",
    "metric": "binary_logloss",
    "verbose": -1
}

#%%
# Train the model 
model = lgb.train(params, train_data, num_boost_round = 500)

#%%

y_pred = model.predict(x_test)
y_pred_binary = y_pred > 0.5 
accuracy = accuracy_score(y_test, y_pred_binary)
MSE = mean_squared_error(y_test, y_pred)
print(accuracy, MSE)

#%%

feature_importances = model.feature_importance()
features = feature_names 
FI = {int(feature_importance) : feature for feature, feature_importance in zip(features, feature_importances)}
sorted_FI = {int(sorted_I) : FI[sorted_I] for sorted_I in sorted(feature_importances, reverse = True)}
print(sorted_FI)

# %%
