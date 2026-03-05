# Script to batch-create parquet files from raw csv data

# There are several advantages of performing this batch-conversion and using parquet files instead of csv:
# 1. Data is stored in an ML-friendly format: 
#   - A. Explicit types (e.g. bool for one-hot columns)
#   - B. Related to above, memory is efficient compared to CSV (parquet files also use compression e.g. for sparse columns while CSV does not)
#   - C. One-hot columns are ready out-of-the-box and named consistently
# 2. Reading from parquet files is much faster than CSV
# 3. Only the data of interest needs to be read (column-based storage)

# The setup is admittedly a lot more work than CSV (which is as simple as it gets), 
# but when properly implemented, a .parquet database should be more efficient and agile
# for ML training and testing than a .csv database 

# Some implementation details: 
# In order to efficiently convert the raw information in the .csv files (most importantly, 
# card IDs and evo/hero levels) to the appropriately typed and formatted columns in the .parquet files, 
# a vectorization-based conversion is used. The original version of this script used loops to iteratively
# go through each file and then lookup one-hot columns with the card ID/evo-hero level and set it to True.
# The vectorization involves 1) mapping the card_id and evohero (transformed to a single integer) of each card
# to an integer column index (i.e. for the one-hot column), 2) creating an (Nrows,16) matrix of column indices and
# (Nrows,16) matrix of corresponding row numbers, and 3) a single-line selection of all the one-hot column indices and
# their corresponding rows and setting them to True 
# The original looping approach (row-by-row search and index) took about 6.5 hours for a batch of 500 CSV files, 
# The vectorized approach took just under a minute 

#%%
from pathlib import Path
import shutil 
import pandas as pd 
import os
import glob
import sys
import datetime
from tqdm import tqdm
import requests
from functions.get_API_token import get_API_token
from functions.get_card_onehot_columns import get_card_onehot_columns
import numpy as np 

#%% 

TOKEN = get_API_token() 

#%%
# Define paths
raw_dir = Path(os.getcwd() + "/data/raw_data")
done_dir = Path(os.getcwd() + "/data/raw_data_converted")
parquet_dir = Path(os.getcwd() + "/data/parquet")

raw_dir.mkdir(parents = True, exist_ok = True)
parquet_dir.mkdir(parents = True, exist_ok = True)

csv_batch_size = 100 # number of csv files (batch size) to convert to a single parquet file at a time

#%% 
# Get up-to-date card types and one-hot column names using logic on data from API   
card_types, OH_columns = get_card_onehot_columns(TOKEN)  

# Make it easy to get column index from the name of the column (for the vectorization below)
OH_name_to_idx = {column : i for i, column in enumerate(OH_columns)}

#%% 
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

#%%
# Make giant sparse array where the values are the column numbers and the indices are cardkeys: 
cardkeys = np.array(list(cardkey_to_colnum.keys()), dtype = np.uint32)
cardkey_to_colnum_lookup = np.zeros((np.max(cardkeys)+1,), dtype = np.uint16)
for cardkey in cardkeys : 
    cardkey_to_colnum_lookup[cardkey] = cardkey_to_colnum[cardkey] #translate dict-based to numpy vectorizable lookup

#%% 
# Define data types for each column in the .csv files
d_types = {
    "player_tag" : "object",
    "player_trophies" : "uint16",
    "player_crowns" : "uint8",
    "opponent_tag" : "object",
    "opponent_trophies" : "uint16",
    "opponent_crowns" : "uint8",
    "gamemode" : "object", 
    "game_time" : "object", 
    "p_king_health" : "uint16",
    "p_support_1_health" : "uint16",
    "p_support_2_health" : "uint16",
    "p_support_level" : "uint8",
    "o_king_health" : "uint16",
    "o_support_1_health" : "uint16",
    "o_support_2_health" : "uint16",
    "o_support_level" : "uint8",
    "p_tower" : "uint32",
    "o_tower" : "uint32"
} | \
{f"p_card_{i+1}" : "uint32" for i in range(8)} | \
{f"p_card_{i+1}_level" : "uint8" for i in range(8)} | \
{f"p_card_{i+1}_evohero" : "uint8" for i in range(8)} | \
{f"o_card_{i+1}" : "uint32" for i in range(8)} | \
{f"o_card_{i+1}_level" : "uint8" for i in range(8)} | \
{f"o_card_{i+1}_evohero" : "uint8" for i in range(8)} 

#%%
# Get filenames of raw data 
csv_filenames = [filepath.name for filepath in raw_dir.glob("*.csv")]
num_files = len(csv_filenames)
num_batches = num_files // csv_batch_size

#%%
# If there are batches to convert, run them 
if num_batches > 0 :
    for batch_i in range(num_batches) :
        # Use timestamp to uniquely identify the parquet batch file
        dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        batch_filepath = parquet_dir / (dt + ".parquet")
        
        df_list = []
        batch_csv_files = csv_filenames[(batch_i)*csv_batch_size : (batch_i+1)*csv_batch_size]

        for csv_file in tqdm(batch_csv_files, desc = f"Batch {batch_i+1}/{num_batches}") : 
            # Load in dataframe for single .csv file
            single_df = pd.read_csv(raw_dir / csv_file)
            single_df.fillna(0, inplace = True) #replace NaN values with 0 (e.g. in 4 card games, there are empty values for last 4 cards in decks)
            single_df = single_df.astype(dtype = d_types) #convert to appropriate datatypes 
            num_rows = single_df.shape[0]

            # Perform matrix operations to get cardkeys 
            card_keys = np.array([single_df[f"p_card_{i+1}"] + 1000*(single_df[f"p_card_{i+1}_evohero"].astype(np.uint32)+1) + 10000 for i in range(8)] + [single_df[f"o_card_{i+1}"] + 1000*(single_df[f"o_card_{i+1}_evohero"].astype(np.uint32)+1) + 20000 for i in range(8)])

            # Get corresponding row numbers for each card key 
            row_range = np.arange(num_rows)
            row_idx = np.broadcast_to(row_range[np.newaxis, ], card_keys.shape)

            # Card keys that are less than 100000 are due to empty card id - remove these
            valid = card_keys > 100000
            card_keys = card_keys[valid] 
            row_idx = row_idx[valid] 

            # Get one-hot column indices that correspond to card keys, using sparse array lookup 
            col_idx = cardkey_to_colnum_lookup[card_keys]

            # Create one-hot matrix and fill with trues at card row/column indices
            OH_mat = np.zeros(shape = (num_rows, len(OH_columns)), dtype = bool)
            OH_mat[row_idx, col_idx] = True 

            # Concatenate main dataframe with one-hot matrix
            single_df = pd.concat([single_df, pd.DataFrame(data = OH_mat, columns = OH_columns)], axis = 1) # Axis must be set to 1 here

            # Append dataframe to batch df list
            df_list.append(single_df)

        df = pd.concat(df_list, ignore_index = True, axis = 1) #Contains the data from all of the batch CSVs

        print("Saving parquet...")
        df.to_parquet(batch_filepath, engine = "pyarrow", compression = "zstd", index = False)

        # Move all CSV files in the batch to converted
        for csv_file in batch_csv_files : 
            shutil.move(raw_dir / csv_file, done_dir / csv_file) 

print("Done converting CSV to parquet")



# # %%
# # Do a quick test: 
# lk = {0: "A", 1 : "B"}

# test_data = np.zeros((3, 3))
# idx = np.array([[0, 0, 1], [1, 2, 1]])
# test_data[idx[0], idx[1]] = 1

# vals = np.vectorize(lk.get)
# print(vals(test_data))

# print(vals)

# #%%

# # %%


# card_ids = np.array([27000012, 26000064, 28000005, 28000006, 26000051, 26000052, 28000024, 28000000]) + 1000*(np.array([1, 1, 0, 0, 0, 0, 0, 0])+1) + 10000

# card_cols = cardkey_to_colnum_lookup[card_ids]

# col_names = [OH_columns[col] for col in card_cols]

# print(card_cols, col_names)
# # %%
