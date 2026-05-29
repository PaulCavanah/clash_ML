#%%
import os 
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import gc
import numpy as np
from functions.get_cards import get_all_cards
from functions.get_API_token import get_API_token

def load_onehot(game_lim = 1_000_000, levels = False, base_only = False, filters = None, include_tags = False, omit_features = [], previous_features = None) : 
    # kwargs: 
    #   game_lim = int or None (maximum number of games to load, default = 1,000,000)
    #   levels = bool (True = levels in place of 1/0 one-hot labels, default = False)
    #   base_only = bool (True = only base cards, no evo/hero, default = False)
    #   filters = list (parquet filter format to apply to each loaded file, default = None)
    #   include_tags = bool (True = returns an extra argument after feature_names, with player/opponent tag info for each game)
    #   omit_features = a list of features to omit from the data, will be removed from X
    #   previous_features = if provided, will force the output X into the columnar order of this list
    # Returns: 
    #   X = ndarray with shape (num_games, num_features) (whether card was present or not, as 1/0 or level/0)
    #   y = ndarray with shape (num_games, 1) (win/loss)
    #   feature_names = list (name of each feature)
    #   (if include_tags) tags = ndarray of shape (num_games, 2) (player_tag, opponent_tag for each game) 

    vectorizer = OneHotVectorizer(levels = levels, base_only = base_only)

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

    # Do garbage collection for systems with low RAM
    del dfs
    gc.collect()

    X = vectorizer.to_onehot(pqt_df)

    # y: player wins
    y = np.array(pqt_df["player_crowns"] > pqt_df["opponent_crowns"])

    # Tags: player_tag, opponent_tag
    tags = np.array(pqt_df.loc[:, ["player_tag", "opponent_tag"]])

    # Do garbage collection for systems with low RAM
    del pqt_df
    gc.collect()

    # Feature names: One-hot column names
    feature_names = vectorizer.OH_columns

    # If applicable, omit features by not including them in data matrix and feature_names
    if len(omit_features) > 0 :
        X = omit(X, omitted_features = omit_features, feature_names = feature_names)

    # If applicable, use a previous list of features to re-order/re-select the columns 
    if previous_features : 
        X = reset_features(X, previous_features = previous_features, current_features = feature_names)
        feature_names = previous_features

    print("Loaded Data with shape:", f"X:{X.shape}, y:{y.shape}")

    if not include_tags : 
        return X, y, feature_names
    else : 
        return X, y, feature_names, tags

def stream_onehot(buffer_size = 100_000, levels = False, base_only = False, filters = None) : 
    
    # Get location of data and filenames
    parquet_dir = Path(os.getcwd() + "/data/parquet")
    parquet_filenames = list([filepath.name for filepath in parquet_dir.glob("*.parquet")])

    # Randomize file order
    num_files = len(parquet_filenames)
    rng = np.random.default_rng()
    shuffled_filenames = [parquet_filenames[i] for i in rng.choice(num_files, size = num_files, replace = False)] 

    # Get column names for data
    num_cards = 8 
    id_columns = [f"p_card_{i+1}" for i in range(num_cards)] + [f"o_card_{i+1}" for i in range(num_cards)]
    eh_columns = [f"{id_col}_evohero" for id_col in id_columns]
    lvl_columns = [f"{id_col}_level" for id_col in id_columns]
    crown_columns = ["player_crowns", "opponent_crowns"]
    tag_columns = ["player_tag", "opponent_tag"]

    card_data_cols = id_columns + eh_columns + lvl_columns + crown_columns
    cols_w_tags = card_data_cols + tag_columns

    # Vectorizer for converting ids into one-hot columns
    vectorizer = OneHotVectorizer(levels = levels, base_only = base_only) 
    feature_names = vectorizer.OH_columns

    # Yield buffers of data iteratively
    df_buffer_list = [] # list of dfs that contain data for the current buffer

    buffer_row_count = 0 # Current buffer game count

    for filename in shuffled_filenames : 
        df = pd.read_parquet(path = parquet_dir / filename, engine = "pyarrow", columns = cols_w_tags, filters = filters)
        remaining_file_size = df.shape[0] # this decreases as buffers are filled with the file's data

        size_needed_for_fill = buffer_size - buffer_row_count # games needed to COMPLETELY fill the current buffer

        while remaining_file_size > size_needed_for_fill : # While there is data in this file to fill a buffer COMPLETELY
            # Fill the buffer and yield 
            needed_df = df.iloc[:size_needed_for_fill, :] # Data that is needed to finish the buffer
            df_buffer_list.append(needed_df)
            df_buffer = pd.concat(df_buffer_list, ignore_index = True) # Includes buffer data from this file and the previous file(s)
            
            # X: card data
            X = vectorizer.to_onehot(df_buffer)

            # y: player wins
            y = np.array(df_buffer["player_crowns"] > df_buffer["opponent_crowns"])

            yield X, y, feature_names # yields a generator that next() gives each successive buffer's data

            # Prepare for next buffer
            buffer_row_count = 0  
            df = df.iloc[size_needed_for_fill:, :] # The file data now becomes only what is needed for the next buffer(s)
            remaining_file_size = df.shape[0]
            df_buffer_list = []
            size_needed_for_fill = buffer_size - buffer_row_count # games needed to completely fill the buffer
        
        # Whatever's left of the file data is added to the df_buffer_list
        buffer_row_count += remaining_file_size 
        df_buffer_list.append(df)

    # Yield what's left after loading all the files:
    if len(df_buffer_list) > 0 : 
        df_buffer = pd.concat(df_buffer_list, ignore_index = True) # Includes buffer data from this file and the previous file(s)
        
        # X: card data
        X = vectorizer.to_onehot(df_buffer)

        # y: player wins
        y = np.array(df_buffer["player_crowns"] > df_buffer["opponent_crowns"])

        yield X, y, feature_names

def load_random_games(load_size, filters = None, buffer_size = 500_000, levels = False, base_only = False) : 
    # Get a sample of games from the entire dataset by streaming 
    num_games = pd.read_parquet(path = f"{os.getcwd()}/data/parquet", engine = "pyarrow", columns = ["player_crowns"], filters = filters)["player_crowns"].shape[0] # load a lightweight column to get the game count
    data_generator = stream_onehot(buffer_size = buffer_size, filters = filters, levels = levels, base_only = base_only)
    _, _, feature_names = next(data_generator)
    num_ss = num_games // buffer_size - 1 # number of buffers to subsample
    buffer_ss_size = int(round(load_size/num_ss)) # subsample size to reach the goal load_size
    load_size_actual = buffer_ss_size*num_ss # actual num games (might be very slightly different from num_games if num_games is not divisible by num_ss)
    ss = np.arange(0, num_ss) * buffer_ss_size # index edges of subsamplings across buffers
    X_ss = np.zeros((load_size_actual, len(feature_names)), dtype = np.uint8)
    y_ss = np.zeros(load_size_actual, dtype = np.uint8)
    for i in range(num_ss) : # buffer iterator
        (X_buffer, y_buffer, feature_names) = next(data_generator, [])        
        # Randomly subsample games 
        rng = np.random.default_rng()
        ss_games = rng.choice(X_buffer.shape[0], size = buffer_ss_size, replace = False) 
        X_ss[ss[i]:(ss[i]+buffer_ss_size), :] = X_buffer[ss_games, :]
        y_ss[ss[i]:(ss[i]+buffer_ss_size)] = y_buffer[ss_games]

    return X_ss, y_ss, feature_names

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

class OneHotVectorizer() :
    # Class used to vectorize raw card ids to onehot columns
    # Primarily useful in streaming cases where the to_onehot method is applied before every yield 
    #
    # levels = bool (True = levels in place of 1/0 one-hot labels, default = False)
    # base_only = bool (True = only base cards, no evo/hero, default = False)

    def __init__(self, levels = False, base_only = False) : 
        self.base_only = base_only
        self.levels = levels

        card_data = get_all_cards(base_only = base_only)  
        self.OH_columns = card_data["feature_names"]
        card_types = card_data["card_types"]

        num_cards = 8
        id_columns = [f"p_card_{i+1}" for i in range(num_cards)] + [f"o_card_{i+1}" for i in range(num_cards)]
        self.lvl_columns = [f"{id_col}_level" for id_col in id_columns]

        # Make it easy to get column index from the name of the column (for the vectorization below)
        OH_name_to_idx = {column : i for i, column in enumerate(self.OH_columns)}

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
        self.cardkeys = np.array(list(cardkey_to_colnum.keys()), dtype = np.uint32)
        self.cardkey_to_colnum_lookup = np.zeros((np.max(self.cardkeys)+1,), dtype = np.uint16)
        for cardkey in self.cardkeys : 
            self.cardkey_to_colnum_lookup[cardkey] = cardkey_to_colnum[cardkey] #translate dict-based to numpy vectorizable lookup

    def to_onehot(self, pqt_df) : 
        num_games = pqt_df.shape[0]

        if self.base_only : # Sets evohero to be 1 to treat all cards as base cards
            card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(1) + 20000 for i in range(8)])
        else : # Uses evohero info
            card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(pqt_df[f"p_card_{i+1}_evohero"].astype(np.uint32)+1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(pqt_df[f"o_card_{i+1}_evohero"].astype(np.uint32)+1) + 20000 for i in range(8)])

        card_lvls = np.transpose(np.array(pqt_df[self.lvl_columns].astype(np.uint8))) # transpose is to make same shape as card_keys

        # Get corresponding row numbers for each card key 
        row_range = np.arange(num_games)
        row_idx = np.broadcast_to(row_range[np.newaxis, ], card_keys.shape)

        # Card keys that are less than 100000 are due to empty card id - remove these
        valid = card_keys > 100000
        card_keys = card_keys[valid] 
        card_lvls = card_lvls[valid]
        row_idx = row_idx[valid] 

        # Get one-hot column indices that correspond to card keys, using sparse array lookup 
        col_idx = self.cardkey_to_colnum_lookup[card_keys]

        # Create one-hot matrix and fill with 1s/levels at card row/column indices
        X = np.zeros(shape = (num_games, len(self.OH_columns)), dtype = np.uint8)
        if self.levels : 
            X[row_idx, col_idx] = card_lvls
        else : 
            X[row_idx, col_idx] = 1 

        return X 
    
def omit(X, omitted_features, feature_names) : 
    # Omit features from a matrix X of game data
    found = [f for f in omitted_features if f in feature_names]; not_found = [f for f in omitted_features if f not in feature_names]
    print(f"Omitting {found} from data", f"Not omitted: {not_found}")
    non_omitted = [i for i, feature in enumerate(feature_names) if feature not in omitted_features]
    feature_names = [feature for feature in feature_names if feature not in omitted_features]
    return X[:, non_omitted]

def reset_features(X, previous_features, current_features) : 
    new_order = []
    for pf in previous_features : 
        new_order.append(current_features.index(pf))
    return X[:, new_order]

