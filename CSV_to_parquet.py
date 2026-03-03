# Script to batch-create parquet files from raw csv data

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
            single_df.fillna(0, inplace = True) #replace NaN values with 0 
            single_df = pd.concat([single_df, pd.DataFrame(columns = OH_columns)]) #Add one-hot columns
            for col, dt in d_types.items() : # Convert column data types 
                single_df[col] = single_df[col].astype(dt)
            num_rows = single_df.shape[0]
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

test_data = {"A" : [1, 1], "B" : [2, 2]}
test = pd.DataFrame(test_data)


test = pd.concat([test, pd.DataFrame(columns = ["C"])])
print(test)

# %%
