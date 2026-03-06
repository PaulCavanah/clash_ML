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
from functions.get_API_token import get_API_token
from functions.get_card_onehot_columns import get_card_onehot_columns
import pyarrow.parquet as pq
import datetime 

#%% 

parquet_dir = Path(os.getcwd() + "/data/parquet")
models_dir = Path(os.getcwd() + "/modeling/models")

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
    filters = [[("gamemode", "==", "Ranked1v1_NewArena")], [("gamemode", "==", "Ladder")], [("gamemode", "==", "Ranked1v1_NewArena2")]]

    df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = Y_columns + X_columns, filters = filters)
    dfs.append(df)

df = pd.concat(dfs, ignore_index = True)
print("Loaded DataFrame:", df.shape)

#%%
# Get X and Y vectors 
X = df.iloc[:, 2:]
Y = df["player_crowns"] > df["opponent_crowns"]

#%% 
# Create train and test splits
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state = random_state)
print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

# %%
# Train classifier 
rf = RandomForestClassifier(verbose = 2, max_depth = 30, min_samples_leaf = 5, random_state = random_state)
rf.fit(x_train, y_train)

# %%
# Investigate model results
rf_test_acc = rf.score(x_test, y_test)
print(rf_test_acc)

#%%

y_pred = rf.predict(x_test)
print(classification_report(y_test, y_pred))

#%%
# Save (pickle) the model using datetime as name
dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
pkl_path = models_dir / f"RFC_{dt}.pkl"
with open(pkl_path, 'wb') as file : 
    pickle.dump(rf, file) 

# %%
