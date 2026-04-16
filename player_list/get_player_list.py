
# Get the list of players for ladder (above a certain trophy level) and ranked gamemodes

from pathlib import Path
import pandas as pd
import os 
import numpy as np

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

parquet_dir = Path(os.getcwd() + "/data/parquet")

ladder_minimum = 13000
ranked_minimum = 0

# Get tags from parquet dataset games that meet criteria
filters = [[("gamemode", "==", "Ladder"), ("player_trophies", ">" , ladder_minimum)], [("gamemode", "==", "Ranked1v1_NewArena"), ("player_trophies", ">", ranked_minimum)], [("gamemode", "==", "Ranked1v1_NewArena2"), ("player_trophies", ">", ranked_minimum)]]
tags_highskill = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = ["player_tag", "opponent_tag"], filters = filters)
tags_highskill_cat = pd.concat([tags_highskill["player_tag"], tags_highskill["opponent_tag"]])

unique_tags_highskill = pd.unique(tags_highskill_cat)
print(len(unique_tags_highskill))

#%%
# for a certain number of collectors, split data up 
import pickle 
num_collectors = 2
list_len = len(unique_tags_highskill)
split_indices = [split * list_len // num_collectors for split in range(num_collectors)] + [list_len]

# Save data splits allocated for each collector 
save_dir = Path(root_dir + "/player_list/lists/")
save_dir.mkdir(exist_ok = True, parents = True)

for split in range(num_collectors) : 
    split_name = f"collector_{split}_20260415_ladder13k_ranked.pkl"
    save_path = f"{save_dir}/{split_name}"
    split_data = list(unique_tags_highskill[split_indices[split]:split_indices[split+1]])
    print(f"Collector {split}: {len(split_data)} players")
    with open(save_path, "wb") as file : 
        pickle.dump(split_data, file) 

# %%
