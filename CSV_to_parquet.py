# Script to batch-create parquet files from raw csv data (a key preprocessing step)

# There are several advantages of performing this batch-conversion and using parquet files instead of csv:
# 1. Explicit types (e.g. bool for one-hot columns)
# 2. Related to above, memory is efficient compared to CSV (parquet files also use compression e.g. for sparse columns while CSV does not)
# 3. Reading from parquet files is much faster than CSV, for the same amount of data
# 4. Only the data of interest needs to be read (column-based storage)
# 5. Parquet library has filtering operations at load time (e.g. can load games only after a certain date and in a certain trophy range)

# The setup is more work than CSV, but when properly implemented, a .parquet database is more efficient and agile
# for ML training and testing than a .csv database

# Take an example (from experience) - getting a "player list" with only players above 12,000 trophies and/or ranked games.
# At the time of typing, I have 70 million games of data. 
# 1. parquet method - From start to finish, it's about ten seconds and a few lines of code. Filtering
# is done at load time. 
# 2. CSV method - Takes several minutes to load. Filtering is done after all the data is loaded. Many more lines of code.
# Some implementation details: 
# - No duplicates (using hashing): To prevent duplicates in the parquet database, and to efficiently check for duplicates, 
# a hashing system is used. Three pieces of information are used from each battle to create a unique "battle id"
# for each battle to ensure 100% that there are no duplicates: the player's tag, the opponent's tag,
# and the battle datetime (in the format originally retrieved from the API). Just the datetime is not
# precise enough to distinguish between battles (it's only precise to a second), and
# both the player and opponent tag (sorted alphabetically when concatenated with the datetime) are needed
# to distinguish the game from all other games in the database. Importantly, the battle id is stored as a
# column in the parquet database to load and check against every time new files are created. 

#%%
from pathlib import Path
import shutil 
import pandas as pd 
import os
import datetime
from tqdm import tqdm
import numpy as np 

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

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
# Define data types for each column in the .csv files
d_types = {
    "player_tag" : "object",
    "player_trophies" : "uint16",
    "player_crowns" : "uint8",
    "opponent_tag" : "object",
    "opponent_trophies" : "uint16",
    "opponent_crowns" : "uint8",
    "gamemode" : "object", 
    "game_time" : "int64", # Game time will be converted from str in csv to int64 in parquet
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

        #replace NaN values with 0 to allow for int types (e.g. in 4 card games, there are empty values for last 4 cards in decks)
        batch_df_raw.fillna(0, inplace = True) 
        
        # Reformat time to int64: 
        ex_time = "20260222T224801" #YYYYMMDDTHHMMSS
        index = list(range(0, 8)) + list(range(9, len(ex_time))) #Omit "T" and the extra stuff at the end (e.g. .000Z)
        batch_df_raw["game_time"] = pd.Series([int("".join([char for i, char in enumerate(str_time) if i in index])) for str_time in batch_df_raw["game_time"]])

        # Convert to explicit datatypes (important step)
        batch_df_raw = batch_df_raw.astype(dtype = d_types) 

        # Get battle ids (hashes) from csv data and make it the first column of the dataframe
        batch_iddf = batch_df_raw.loc[:, ["player_tag", "opponent_tag", "game_time"]] # Data used to make hash
        batch_iddf.iloc[:, :2] = np.sort(batch_iddf.iloc[:, :2], axis = 1) # Sort player tags and opponent tags alphabetically 
        str_concat = batch_iddf.iloc[:, 0] + batch_iddf.iloc[:, 1] + batch_iddf.iloc[:, 2].astype("string") # Create concatenated string input to hash 
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

        print("Saving parquet...")
        pqt_df.to_parquet(batch_filepath, engine = "pyarrow", compression = "zstd", index = False)

        # Move all CSV files in the batch to converted
        print("Moving raw data...")
        for csv_file in batch_csv_files : 
            shutil.move(raw_dir / csv_file, done_dir / csv_file) 


print("Done converting CSV to parquet")
#battle_ids = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = ["battle_id"])["battle_id"]
print("Num battles in new dataset: ", battle_ids.shape[0])
