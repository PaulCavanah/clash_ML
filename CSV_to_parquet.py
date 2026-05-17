# Script to batch-create parquet files from raw csv data (a key preprocessing step)

# There are several advantages of performing this batch-conversion and using parquet files instead of csv:
# 1. Data is stored in an ML-friendly format: 
#   - A. Explicit types (e.g. bool for one-hot columns)
#   - B. Related to above, memory is efficient compared to CSV (parquet files also use compression e.g. for sparse columns while CSV does not)
#   - C. One-hot columns are ready out-of-the-box and named consistently
# 2. Reading from parquet files is much faster than CSV
# 3. Only the data of interest needs to be read (column-based storage)

# The setup is more work than CSV (which is as simple as it gets setup-wise), 
# but when properly implemented, a .parquet database is more efficient and agile
# for ML training and testing than a .csv database

# Take an example (from experience) - getting a "player list" with only players above 12,000 trophies and/or ranked games.
# At the time of typing, I have 70 million games of data. 
# 1. parquet method - From start to finish, it's about ten seconds of run time and a few lines of code
# 2. CSV method - Can't even be performed on my current computer due to memory constraints. But even
# if I could, it would take 2 minutes to load 70,000 CSV files into memory. Then I would have to 
# select the data from memory only after I loaded it, which takes more code, more time, and more memory.  

# Some implementation details: 
# - Vectorization: In order to efficiently convert the raw information in the .csv files (most importantly, 
# card IDs and evo/hero levels) to the appropriately typed and formatted columns in the .parquet files, 
# a vectorization-based conversion is used. The original version of this script used loops to iteratively
# go through each file and then lookup one-hot columns with the card ID/evo-hero level and set it to True.
# The vectorization involves 1) mapping the card_id and evohero (transformed to a single integer) of each card
# to an integer column index (i.e. for the one-hot column), 2) creating an (Nrows,16) matrix of column indices and
# (Nrows,16) matrix of corresponding row numbers, and 3) a single-line selection of all the one-hot column indices and
# their corresponding rows and setting them to True 
# The original looping approach (row-by-row search and index) took about 6.5 hours for a batch of 500 CSV files, 
# The vectorized approach took just under a minute for those 500 files.
# - No duplicates (using hashing): To prevent duplicates in the parquet database, and to efficiently check for duplicates, 
# a hashing system is used. Three pieces of information are used from each battle to create a unique "battle id"
# for each battle to ensure 100% that there are no duplicates: the player's tag, the opponent's tag,
# and the battle datetime (in the format originally retrieved from the API). Just the datetime is not
# precise enough to distinguish between battles (it's only precise to a second), and
# both the player and opponent tag (sorted alphabetically when concatenated with the datetime) are needed
# to distinguish the game from all other games in the database. Importantly, the battle id is stored as a
# column in the parquet database for easy access 

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
import numpy as np 

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

from functions.get_API_token import get_API_token
from functions.get_cards import get_all_cards

#%% 

TOKEN = get_API_token() 

#%%
# Define paths
raw_dir = Path(os.getcwd() + "/data/raw_data")
done_dir = Path(os.getcwd() + "/data/raw_data_converted")
parquet_dir = Path(os.getcwd() + "/data/parquet")

raw_dir.mkdir(parents = True, exist_ok = True)
done_dir.mkdir(parents = True, exist_ok = True)
parquet_dir.mkdir(parents = True, exist_ok = True)

csv_batch_size = 100 # number of csv files (batch size) to convert to a single parquet file at a time

#%% 
# Get up-to-date card types and one-hot column names using logic on data from API   
card_types, OH_columns = get_all_cards(TOKEN)  

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
# Load battle ids from currently existing dataset
battle_ids = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = ["battle_id"])["battle_id"]
print("Previous dataset battle ids loading...")
print("Unique battle ids: ", pd.unique(battle_ids).shape[0], "Num battles: ", battle_ids.shape[0])

#%%
# If there are batches to convert, run them 
if num_batches > 0 :
    for batch_i in tqdm(range(num_batches)) :

        # Use timestamp to uniquely identify the parquet batch file
        dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        batch_filepath = parquet_dir / (dt + ".parquet")
        
        batch_csv_files = csv_filenames[(batch_i)*csv_batch_size : (batch_i+1)*csv_batch_size]
        # Load all csvs at once  
        batch_df_raw = pd.concat([single_df for single_df in [pd.read_csv(raw_dir / csv_file, index_col = 0) for csv_file in batch_csv_files]], axis = 0, ignore_index = True)
        batch_df_raw.fillna(0, inplace = True) #replace NaN values with 0 (e.g. in 4 card games, there are empty values for last 4 cards in decks)
        batch_df_raw = batch_df_raw.astype(dtype = d_types) #convert to appropriate datatypes 

        # Get battle ids (hashes) from csv data and make it the first column of the dataframe
        batch_iddf = batch_df_raw.loc[:, ["player_tag", "opponent_tag", "game_time"]] # Data used to make hash
        batch_iddf.iloc[:, :2] = np.sort(batch_iddf.iloc[:, :2], axis = 1) # Sort player tags and opponent tags alphabetically 
        str_concat = batch_iddf.iloc[:, 0] + batch_iddf.iloc[:, 1] + batch_iddf.iloc[:, 2] # Create concatenated string input to hash 
        batch_hashes = pd.util.hash_pandas_object(str_concat, index = False) # Use pandas built in hash function to generate battle id
        batch_hashes_df = batch_hashes.to_frame(name = "battle_id") # convert to dataframe (for concatenation later)
        # If there are any duplicate battle id rows within the CSV, or from the previous database, don't include them
        duplicates = batch_hashes_df.duplicated() | batch_hashes.isin(battle_ids)
        batch_df = batch_df_raw.loc[~duplicates, :]
        batch_hashes_df = batch_hashes_df.loc[~duplicates, :]
        print("Removed ", np.sum(duplicates), " duplicate games")
        num_rows = batch_df.shape[0]

        battle_ids = pd.concat([battle_ids, batch_hashes_df.loc[:, "battle_id"]])

        pqt_df = pd.concat([batch_hashes_df, batch_df], axis = 1) # Concatenate hashes and data along columns
        pqt_df = pqt_df.reset_index(drop = True) # Necessary to concatenate with OH columns later (which have default index)

        # Perform matrix operations to get cardkeys 
        card_keys = np.array([pqt_df[f"p_card_{i+1}"] + 1000*(pqt_df[f"p_card_{i+1}_evohero"].astype(np.uint32)+1) + 10000 for i in range(8)] + [pqt_df[f"o_card_{i+1}"] + 1000*(pqt_df[f"o_card_{i+1}_evohero"].astype(np.uint32)+1) + 20000 for i in range(8)])

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
        pqt_df = pd.concat([pqt_df, pd.DataFrame(data = OH_mat, columns = OH_columns)], axis = 1) # Axis must be set to 1 here

        print("Saving parquet...")
        pqt_df.to_parquet(batch_filepath, engine = "pyarrow", compression = "zstd", index = False)

        # Move all CSV files in the batch to converted
        print("Moving raw data...")
        for csv_file in batch_csv_files : 
            shutil.move(raw_dir / csv_file, done_dir / csv_file) 


print("Done converting CSV to parquet")
battle_ids = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = ["battle_id"])["battle_id"]
print("Num battles in new dataset: ", battle_ids.shape[0])
