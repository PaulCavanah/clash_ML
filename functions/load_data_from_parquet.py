#%%
import os 
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import gc
import numpy as np
from functions.get_card_onehot_columns import get_card_onehot_columns
from functions.get_API_token import get_API_token

#%%
num_batches = 1
#%%

def load_level_onehot(num_batches, filters = None, normalize_levels = False) : 
    
    parquet_dir = Path(os.getcwd() + "/data/parquet")

    # Get up-to-date card types and one-hot column names using logic on data from API   
    TOKEN = get_API_token() 
    card_types, OH_columns = get_card_onehot_columns(TOKEN)  

    # Make it easy to get column index from the name of the column (for the vectorization below)
    OH_name_to_idx = {column : i for i, column in enumerate(OH_columns)}

    # Get card_key : column_idx mappings for vectorization 
    cardkey_to_colnum = dict()
    for (card_id, evohero), card_name in card_types.items() : 
        # Encodings for player = card_id * + (evohero+1)*1000 + 10000 (puts evohero info in 1000ths place, and player info in 10000ths place, both where there are always a 0 otherwise)
        card_key_plr = (card_id + (evohero+1)*1000 + 10000) 
        column_idx_plr = OH_name_to_idx[f"Plr {card_name}"]
        cardkey_to_colnum[card_key_plr] = column_idx_plr
        # Encodings for opponent = card_id + (evohero+1)*1000 + 20000 (puts evohero info in 1000ths place, and opponent info in 10000ths place, both where there are always a 0 otherwise)
        card_key_opp = (card_id + (evohero+1)*1000 + 20000)
        column_idx_opp = OH_name_to_idx[f"Opp {card_name}"] 
        cardkey_to_colnum[card_key_opp] = column_idx_opp

    # Make giant sparse array where the values are the column numbers and the indices are cardkeys: 
    cardkeys = np.array(list(cardkey_to_colnum.keys()), dtype = np.uint32)
    cardkey_to_colnum_lookup = np.zeros((np.max(cardkeys)+1,), dtype = np.uint16)
    for cardkey in cardkeys : 
        cardkey_to_colnum_lookup[cardkey] = cardkey_to_colnum[cardkey] #translate dict-based to numpy vectorizable lookup

    # Columns for ids, evo/hero status, levels, and crowns 
    num_cards = 8 
    id_columns = [f"p_card_{i+1}" for i in range(num_cards)] + [f"o_card_{i+1}" for i in range(num_cards)]
    eh_columns = [f"{id_col}_evohero" for id_col in id_columns]
    lvl_columns = [f"{id_col}_level" for id_col in id_columns]
    crown_columns = ["player_crowns", "opponent_crowns"]

    card_data_cols = id_columns + eh_columns + lvl_columns + crown_columns 

    # Load 
    parquet_dir = Path(os.getcwd() + "/data/parquet")

    parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")][0:num_batches]

    dfs = []

    for filename in parquet_filenames :         
        
        if filters == None :
            # Default
            filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , 10000)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", 1000)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", 1000)]]

        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = card_data_cols, filters = filters)
        dfs.append(df)

    pqt_df = pd.concat(dfs, ignore_index = True)

    # Could expand memory bottleneck for systems with low RAM
    del dfs
    gc.collect()

    num_rows = pqt_df.shape[0]

    card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(pqt_df[f"p_card_{i+1}_evohero"].astype(np.uint32)+1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(pqt_df[f"o_card_{i+1}_evohero"].astype(np.uint32)+1) + 20000 for i in range(8)])
    card_lvls = np.transpose(np.array(pqt_df[lvl_columns].astype(np.uint8))) # transpose is to make same shape as card_keys

    # Get corresponding row numbers for each card key 
    row_range = np.arange(num_rows)
    row_idx = np.broadcast_to(row_range[np.newaxis, ], card_keys.shape)

    # Card keys that are less than 100000 are due to empty card id - remove these
    valid = card_keys > 100000
    card_keys = card_keys[valid] 
    card_lvls = card_lvls[valid]
    row_idx = row_idx[valid] 

    # Get one-hot column indices that correspond to card keys, using sparse array lookup 
    col_idx = cardkey_to_colnum_lookup[card_keys]

    # Create one-hot matrix and fill with levels at card row/column indices
    X = np.zeros(shape = (num_rows, len(OH_columns)), dtype = np.uint8)
    X[row_idx, col_idx] = card_lvls

    # y: player wins
    y = np.array(pqt_df["player_crowns"] > pqt_df["opponent_crowns"])

    # Feature names: One-hot column names
    feature_names = OH_columns

    print("Loaded Data with shape:", f"X:{X.shape}, y:{y.shape}")

    return X, y, feature_names
    
    

def load_plain_onehot(num_batches, player_mirror = False, filters = None) : 
    # Load in X and Y data from the parquet files: 
    # Due to card updates, the schema evolves - parquet files may have different columns
    # The approach to merging these schemas is to load in each parquet file individually
    # with its unique one hot columns as a dataframe, add the dataframe to a list,
    # then concatenate the list of dataframes and fill the NaNs with false
    
    # Player mirror option - mirrors the data across the player/opponent dimension and
    # concatenates it to the dataframe (for asymmetric models)

    #%%
    parquet_dir = Path(os.getcwd() + "/data/parquet")

    parquet_filenames = [filepath.name for filepath in parquet_dir.glob("*.parquet")][0:num_batches]

    dfs = []

    for filename in parquet_filenames : 
        pf = pq.ParquetFile(parquet_dir / filename)
        columns = pf.schema.names
        X_columns = [column for column in columns if column[0:3] in ("Plr", "Opp")]
        Y_columns = ["player_crowns", "opponent_crowns"]
        
        if filters == None :
            # Default
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
    X = df.iloc[:, 2:].astype(bool)
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
