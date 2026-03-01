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

#%%
# Define paths
raw_dir = Path(os.getcwd() + "/data/raw_data")
done_dir = Path(os.getcwd() + "/data/raw_data_converted")
parquet_raw_dir = Path(os.getcwd() + "/data/parquet_raw")

raw_dir.mkdir(parents = True, exist_ok = True)
parquet_raw_dir.mkdir(parents = True, exist_ok = True)

csv_batch_size = 500 # number of csv files to convert to parquet at a time

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
{f"o_card_{i+1}_evohero" : "uint8" for i in range(8)} 

# fyi : sys.getsizeof(retyped_df) is ~0.5 sys.getsizeof(raw_df)

#%%
# Get filenames of raw data and establish batch size
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
            for col, dt in d_types.items() : 
                single_df[col] = single_df[col].astype(dt)
            df_list.append(single_df)
        df = pd.concat(df_list, ignore_index = True) #Contains the re-typed data from all of the batch CSVs

        # Use timestamp to uniquely identify the parquet batch file
        dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        batch_filepath_tmp = parquet_raw_dir / (dt + ".parquet.tmp")
        batch_filepath = parquet_raw_dir / (dt + ".parquet")
        df.to_parquet(batch_filepath_tmp, engine = "pyarrow", compression = "zstd", index = False)
        # After saving successfully, convert .tmp to .parquet (.tmp makes crashing during saves not a problem)
        batch_filepath_tmp.replace(batch_filepath)

        # Move all CSV files in the batch to converted
        for file_ii in batch_file_range : 
            shutil.move(raw_dir / raw_data_names[file_ii], done_dir / raw_data_names[file_ii]) 

print("Done converting CSV to parquet")


# %%
