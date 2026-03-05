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
# This took several hours per file. 
# The vectorization involves 1) mapping the card_id and evohero (combined into a single integer, C_idx) of each card
# to an integer column index (i.e. for the one-hot column), 2) constructing a numpy matrix of (row, C_idx), 

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

csv_batch_size = 500 # number of csv files to convert to parquet at a time

#%% 
# Get one-hot card column names using logic on data from API   

card_types, OH_columns = get_card_onehot_columns(TOKEN)  

#%% 
# Define data types for each column
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
{f"o_card_{i+1}_evohero" : "uint8" for i in range(8)} | \
{one_hot_column : "bool" for one_hot_column in OH_columns}

#%%
# Get filenames of raw data 
raw_data_names = [filepath.name for filepath in raw_dir.glob("*.csv")]
num_files = len(raw_data_names)
num_batches = num_files // csv_batch_size

#%%
# If there are batches to convert, run them 
if num_batches > 0 :
    for batch_i in range(num_batches) : 
        batch_file_range = range((batch_i)*csv_batch_size, (batch_i+1)*csv_batch_size)
        df_list = []
        for file_ii in tqdm(batch_file_range) : 
            single_df = pd.read_csv(raw_dir / raw_data_names[file_ii])
            single_df.fillna(0, inplace = True) #replace NaN values with 0 (e.g. in 4 card games, there are empty values for last 4 cards in decks)
            num_rows = single_df.shape[0]
            OH_df = pd.DataFrame(data = np.zeros(shape = (num_rows, len(OH_columns)), dtype = bool), columns = OH_columns)
            single_df = pd.concat([single_df, OH_df], axis = 1) #Add one-hot columns (AXIS MUST BE SET TO 1 HERE)
            for col, dt in d_types.items() : # Convert column data types 
                single_df[col] = single_df[col].astype(dt)
            # Fill in the one-hot columns by processing each row of the data: 
            player_types = {"p" : "Plr", "o" : "Opp"}
            for row in range(num_rows) : 
                for pt in player_types : 
                    for card_i in range(8) : 
                        lookup = (single_df.loc[row, f"{pt}_card_{card_i+1}"], single_df.loc[row, f"{pt}_card_{card_i+1}_evohero"])
                        if lookup not in card_types: #card not available (e.g. 4-card games, a card that was very recently introduced, or a limited-time card)
                            continue
                        single_df.loc[row, f"{player_types[pt]} {card_types[lookup]}"] = True 

            df_list.append(single_df)

        df = pd.concat(df_list, ignore_index = True) #Contains the re-typed data from all of the batch CSVs

        # Use timestamp to uniquely identify the parquet batch file
        dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        batch_filepath = parquet_dir / (dt + ".parquet")
        df.to_parquet(batch_filepath, engine = "pyarrow", compression = "zstd")

        # Move all CSV files in the batch to converted
        for file_ii in batch_file_range : 
            shutil.move(raw_dir / raw_data_names[file_ii], done_dir / raw_data_names[file_ii]) 

print("Done converting CSV to parquet")



# %%
# Do a quick test: 
test_data = np.zeros((3, 3))
idx = np.array([[0, 0, 1], [1, 2, 1]])
test_data[idx[0], idx[1]] = 1
print(test_data)

test_df = pd.DataFrame(data = test_data)


# %%
# 1. 

