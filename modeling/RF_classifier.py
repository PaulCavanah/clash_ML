# Train an RF classifier on the data

#%% 
import os 
import pandas as pd
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV 
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report 
import pickle
from pathlib import Path

#%% 

parquet_raw_dir = Path(os.getcwd() + "/data/parquet_raw")
parquet_X_dir = Path(os.getcwd() + "/data/parquet_X")

random_state = 42 # for splits and model 

#%%
#1. Load in X and Y data
# X comes from parquet_X
# Y comes from parquet_raw 
# only include ladder and ranked matches

filters = [("gamemode", "=", "Ranked1v1_NewArena"), ("gamemode", "=", "Ladder")]

gamemode = 

# X : 
X = pd.read_parquet(path = parquet_X_dir, engine = "pyarrow", )

# Y :
y_columns = ["player_crowns", "opponent_crowns"]
crown_data = pd.read_parquet(path = parquet_raw_dir, engine = "pyarrow", columns = y_columns, filters = filters)
Y = crown_data["player_crowns"] > crown_data["opponent_crowns"]

#%% 
# Create train and test splits
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state = random_state)
print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

# %%
# Train classifier 
from sklearn.ensemble import RandomForestClassifier
rf = RandomForestClassifier(verbose = 2, max_depth = 30, min_samples_leaf = 5, random_state = random_state)
rf.fit(x_train, y_train)

# %%
# Investigate model results
rf_test_acc = rf.score(x_test, y_test)

y_pred = rf.predict(x_test)
from sklearn.metrics import classification_report 
print(classification_report(y_test, y_pred))

# Save (pickle) the model 
import pickle
pkl_path = "models\\RF_1.pkl"
with open(pkl_path, 'wb') as file : 
    pickle.dump(rf, file) 