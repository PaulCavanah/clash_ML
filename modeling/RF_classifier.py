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

#%% 
TOKEN = get_API_token()

parquet_dir = Path(os.getcwd() + "/data/parquet")

random_state = 42 # for splits and model 

#%% 
# Load in X and Y data from the parquet files: 
# Due to card updates, the schema evolves - parquet files may have different columns
# The approach to merging these schemas is to load in each parquet file individually
# with its unique one hot columns as a dataframe, add the dataframe to a list,
# then concatenate the list of dataframes and fill the NaNs with false

parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")]
dfs = []
for filename in parquet_filenames : 
    #%% 
    filename = parquet_filenames[1]
    pf = pq.ParquetFile(parquet_dir / filename)
    columns = pf.schema.names
    X_columns = [column for column in columns if column[0:3] in ("Plr", "Opp")]
    Y_columns = ["player_crowns", "opponent_crowns"]
    df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = Y_columns + X_columns)

    #%%
    print(df.iloc[1, :])

    #%%

#%%
parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")]


#%%

pd.read_parquet(path = parquet_dir, columns = ["Plr Hero Magic Archer"])

#%%
# Load in X and Y data

# only include ladder and ranked matches
filters = [("gamemode", "=", "Ranked1v1_NewArena"), ("gamemode", "=", "Ladder")]


#%%

# Y :
y_columns = ["player_crowns", "opponent_crowns"]
crown_data = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = y_columns, filters = filters)
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
