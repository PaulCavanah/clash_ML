# Use lightGBM algorithm to train a classifier to predict win/loss

#%% 
import lightgbm as lgb
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score, classification_report
from pathlib import Path
import os
import pyarrow.parquet as pq
import pandas as pd

#%% 

parquet_dir = Path(os.getcwd() + "/../data/parquet")

train_limit = 10000
num_batches_to_load = 1

random_state = 42 # for splits and model 

#%% 
# Load in X and Y data from the parquet files: 
# Due to card updates, the schema evolves - parquet files may have different columns
# The approach to merging these schemas is to load in each parquet file individually
# with its unique one hot columns as a dataframe, add the dataframe to a list,
# then concatenate the list of dataframes and fill the NaNs with false

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
print("Loaded DataFrame:", df.shape)

#del dfs #Major consumer of memory

#%%
# Get X and Y vectors 
X = df.iloc[:, 2:]
Y = df["player_crowns"] > df["opponent_crowns"]

#del df # Major consumer of memory

#%% 
# Create train and test splits
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.1, random_state = random_state)
#print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

x_train = x_train[0:train_limit]
y_train = y_train[0:train_limit]

del X # Major consumer of memory
del Y # Minor consumer of memory

# Create LightGBM dataset object 
train_data = lgb.Dataset(x_train, label=y_train)

# Set parameters for regression
params = {
    "objective": "regression",
    "verbose": -1
}

#%%
# Train the model 
model = lgb.train(params, train_data, num_boost_round = 500)

#%%

y_pred = model.predict(x_test) > 0.5
accuracy = accuracy_score(y_test, y_pred)
print(accuracy)

# %%
print(x_train.shape)
# %%
