#%%
import os 
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import gc
import numpy as np

#%%
num_batches = 1
#%%

def load_data_from_parquet(num_batches, player_mirror = True) : 
    # Load in X and Y data from the parquet files: 
    # Due to card updates, the schema evolves - parquet files may have different columns
    # The approach to merging these schemas is to load in each parquet file individually
    # with its unique one hot columns as a dataframe, add the dataframe to a list,
    # then concatenate the list of dataframes and fill the NaNs with false
    
    # Player swap option - mirrors the data across the player/opponent dimension and
    # concatenates it to the dataframe

    #%%
    parquet_dir = Path(os.getcwd() + "/data/parquet")

    parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")][0:num_batches]

    dfs = []

    for filename in parquet_filenames : 
        pf = pq.ParquetFile(parquet_dir / filename)
        columns = pf.schema.names
        X_columns = [column for column in columns if column[0:3] in ("Plr", "Opp")]
        Y_columns = ["player_crowns", "opponent_crowns"]

        # only include ladder and ranked matches
        # filters = [[("gamemode", "==", "Ranked1v1_NewArena")],
        #             [("gamemode", "==", "Ladder")], 
        #              [("gamemode", "==", "Ranked1v1_NewArena2")]]
        #filters = [[("gamemode", "==", "Ladder")]]
        
        filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , 10000)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", 1000)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", 1000)]]


        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = Y_columns + X_columns, filters = filters)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index = True)

    #%%

    feature_names = df.columns[2:]

    # Could expand memory bottleneck for systems with low RAM
    del dfs
    gc.collect()

    df.fillna(0, inplace = True)

    # X and Y
    X = df.iloc[:, 2:]
    y = df["player_crowns"] > df["opponent_crowns"]

    #%%

    # Could expand memory bottleneck for systems with low RAM
    del df
    gc.collect() 

    # Mirrors player and opponent data so that model does not form player/opponent biases 
    if player_mirror : 
        half = X.shape[1] // 2 #swap around half-way point (player is first half / opponent is second half)

        # Swap player columns to opponent columns and opponent columns to player columns 
        X_swap = X.copy()
        X_swap.iloc[:, half:] = X.iloc[:, :half] # Set player deck data to opponent deck data
        X_swap.iloc[:, :half] = X.iloc[:, half:] # Set opponent deck data to player deck data

        # Do "Not" operation on y to flip wins/lossess
        y_swap = y.copy()
        y_swap = ~y

        X = pd.concat([X, X_swap], ignore_index = True)
        y = pd.concat([y, y_swap], ignore_index = True)
        
    print("Loaded Data with shape:", f"X:{X.shape}, y:{y.shape}, mirroring = {player_mirror}" )

    return X, y, feature_names
