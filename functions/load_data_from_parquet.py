#%%
import os 
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import gc
import numpy as np
from functions.get_cards import get_all_cards
from functions.get_API_token import get_API_token

def load_onehot(game_lim = 1000000, levels = False, base_only = False, filters = None, include_tags = False) : 
    # kwargs: 
    #   game_lim = int or None (maximum number of games to load, default = 1,000,000)
    #   levels = bool (True = levels in place of 1/0 one-hot labels, default = False)
    #   base_only = bool (True = only base cards, no evo/hero, default = False)
    #   filters = list (parquet filter format to apply to each loaded file, default = None)
    #   include_tags = bool (True = returns an extra argument after feature_names, with player/opponent tag info for each game)
    # Returns: 
    #   X = ndarray with shape (num_games, num_features) (whether card was present or not, as 1/0 or level/0)
    #   y = ndarray with shape (num_games, 1) (win/loss)
    #   feature_names = list (name of each feature)
    #   (if include_tags) tags = ndarray of shape (num_games, 2) (player_tag, opponent_tag for each game) 

    # Get up-to-date card types and one-hot column names using logic on data from API   
    TOKEN = get_API_token() 
    card_types, OH_columns = get_all_cards(TOKEN, base_only = base_only)  

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
    tag_columns = ["player_tag", "opponent_tag"]

    card_data_cols = id_columns + eh_columns + lvl_columns + crown_columns
    cols_w_tags = card_data_cols + tag_columns

    # Load data
    parquet_dir = Path(os.getcwd() + "/data/parquet")
    parquet_filenames = list([filepath.name for filepath in parquet_dir.glob("*.parquet")])

    dfs = []
    row_count = 0 
    for filename in parquet_filenames : 
        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = cols_w_tags, filters = filters)
        dfs.append(df)
        row_count += df.shape[0]
        if game_lim and row_count > game_lim : 
            break 

    if game_lim == None : 
        game_lim = row_count

    pqt_df = pd.concat(dfs, ignore_index = True)
    pqt_df = pqt_df.iloc[:game_lim, :]
    num_games = pqt_df.shape[0]

    # Do garbage collection for systems with low RAM
    del dfs
    gc.collect()

    if base_only : # Sets evohero to be 1 to treat all cards as base cards
        card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(1) + 20000 for i in range(8)])
    else : # Uses evohero info
        card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(pqt_df[f"p_card_{i+1}_evohero"].astype(np.uint32)+1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(pqt_df[f"o_card_{i+1}_evohero"].astype(np.uint32)+1) + 20000 for i in range(8)])

    card_lvls = np.transpose(np.array(pqt_df[lvl_columns].astype(np.uint8))) # transpose is to make same shape as card_keys

    # Get corresponding row numbers for each card key 
    row_range = np.arange(num_games)
    row_idx = np.broadcast_to(row_range[np.newaxis, ], card_keys.shape)

    # Card keys that are less than 100000 are due to empty card id - remove these
    valid = card_keys > 100000
    card_keys = card_keys[valid] 
    card_lvls = card_lvls[valid]
    row_idx = row_idx[valid] 

    # Get one-hot column indices that correspond to card keys, using sparse array lookup 
    col_idx = cardkey_to_colnum_lookup[card_keys]

    # Create one-hot matrix and fill with 1s/levels at card row/column indices
    X = np.zeros(shape = (num_games, len(OH_columns)), dtype = np.uint8)
    if levels : 
        X[row_idx, col_idx] = card_lvls
    else : 
        X[row_idx, col_idx] = 1 

    # y: player wins
    y = np.array(pqt_df["player_crowns"] > pqt_df["opponent_crowns"])

    # Feature names: One-hot column names
    feature_names = OH_columns

    # Tags: player_tag, opponent_tag
    tags = np.array(pqt_df.loc[:, ["player_tag", "opponent_tag"]])

    print("Loaded Data with shape:", f"X:{X.shape}, y:{y.shape}")

    if not include_tags : 
        return X, y, feature_names
    else : 
        return X, y, feature_names, tags

def load_player_unique_decks(game_lim = 1000000, levels = False, base_only = False, filters = None) : 
    # Extension of load_onehot that transforms X to only have decks that are unique to each player and to not have an opponent side
    # 
    # kwargs: 
    #   game_lim = int or None (maximum number of games to load, default = 1,000,000)
    #   levels = bool (True = levels in place of 1/0 one-hot labels, default = False)
    #   base_only = bool (True = only base cards, no evo/hero, default = False)
    #   filters = list (parquet filter format to apply to each loaded file, default = None)
    # Returns: 
    #   X = ndarray with shape (num_player_unique_decks, num_features//2) (whether card was present in deck or not, as 1/0 or level/0)
    #   y = ndarray with shape (num_games, 1) (win/loss)
    #   feature_names = list (name of each feature) - here, all original feature_names are returned

    X, y, feature_names, tags = load_onehot(game_lim = game_lim, levels = levels, base_only = base_only, filters = filters, include_tags = True)

    C = len(feature_names) // 2 # number of cards available
    N = X.shape[0] # number of original games
    X_cat = np.concatenate([X[:, :C], X[:, C:]], axis = 0) # Concatenate player and opponent
    y_cat = np.concatenate([y, np.logical_not(y)], axis = 0) # Flip opponent wins 
    tags_cat = np.concatenate([tags[:, 0], tags[:, 1]], axis = 0) # Concatenate player and opponent tags
    tags_with_decks = np.concatenate([np.reshape(tags_cat, (N*2, 1)), X_cat], axis = 1) # Concatenate tags with deck data
    nonunique_df = pd.DataFrame(tags_with_decks) 
    unique_df = nonunique_df.drop_duplicates() # eliminate rows in which the player has the same deck (eliminates sampling bias)
    X_out = unique_df.iloc[:, 1:] # Take tags out of the data (so it's only card data)
    y_out = y_cat[unique_df.index] # get wins/losses that correspond to unique deck games

    print(f"Loaded {X_out.shape[0]} decks")

    return X_out, y_out, feature_names

def load_level_onehot(num_batches = "all", filters = None) : 
    
    parquet_dir = Path(os.getcwd() + "/data/parquet")

    # Get up-to-date card types and one-hot column names using logic on data from API   
    TOKEN = get_API_token() 
    card_types, OH_columns = get_all_cards(TOKEN)  

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
    parquet_filenames = list([filepath.name for filepath in parquet_dir.glob("*.parquet")])
    if num_batches != "all" : 
        parquet_filenames = parquet_filenames[-num_batches:] 

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
    
    

def load_plain_onehot(num_batches = "all", player_mirror = False, filters = None) : 
    # Load in X and Y data from the parquet files: 
    # Due to card updates, the schema evolves - parquet files may have different columns
    # The approach to merging these schemas is to load in each parquet file individually
    # with its unique one hot columns as a dataframe, add the dataframe to a list,
    # then concatenate the list of dataframes and fill the NaNs with false
    
    # Player mirror option - mirrors the data across the player/opponent dimension and
    # concatenates it to the dataframe (for asymmetric models)

    # Load data
    parquet_dir = Path(os.getcwd() + "/data/parquet")
    parquet_filenames = list([filepath.name for filepath in parquet_dir.glob("*.parquet")])
    if num_batches != "all" : 
        parquet_filenames = parquet_filenames[-num_batches:] 

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

    return np.array(X), np.array(y), feature_names
