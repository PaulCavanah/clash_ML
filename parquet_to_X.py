# For model training, there is X and Y data required 
# In this case, X is a one-hot matrix of all cards, including evo/hero versions
# This function converts the relevant columns from the parquet file to X


from pathlib import Path
import pandas as pd 
import os 
import sys
import glob
import requests
import json
from tqdm import trange
import numpy as np
from functions.get_API_token import get_API_token

TOKEN = get_API_token()

parquet_raw_dir = Path(os.getcwd() + "/data/parquet_raw/")
parquet_X_dir = Path(os.getcwd() + "/data/parquet_X")
parquet_X_dir.mkdir(parents = True, exist_ok = True)

parquet_raw_files = [filepath.name for filepath in parquet_raw_dir.glob("*.parquet")]

# Data = card information (id and evo/hero status) for each game
data_columns = [f"p_card_{i+1}" for i in range(8)] \
+ [f"o_card_{i+1}" for i in range(8)] \
+ [f"p_card_{i+1}_evohero" for i in range(8)] \
+ [f"o_card_{i+1}_evohero" for i in range(8)] 


for pqt_filename in parquet_raw_files : 
    file_date = pqt_filename.split(".")[0]
    X_path = parquet_X_dir / f"{file_date}_X.parquet "

    if os.path.isfile(X_path) : # Don't convert files that have already been converted
        continue

    print("Converting ", file_date)

    # Read only data required for X 
    df = pd.read_parquet(path = parquet_raw_dir / pqt_filename, engine = "pyarrow", columns = data_columns)

    # Load card names from cards json or API call (cards json can be used for debugging/testing in certain situations whereas API gives an up-to-date version of all the cards)
    card_get_method = "API" # "JSON" or "API"
    if card_get_method == "API" : 
        url = f"https://api.clashroyale.com/v1/cards"
        headers = {"Authorization": f"Bearer {TOKEN}"}
        r = requests.get(url, headers = headers)
        card_data = r.json()
    elif card_get_method == "JSON": 
        json_path = os.getcwd() + "/json/" + "Cards.json"
        with open(json_path, 'r') as file : 
            card_data = json.load(file) 

    # What is needed? 
    # A player/opponent OH column for every type of card, including default, evo, and hero variants 

    # To identify these types from the data, I need a dict, where the key is a tuple (id, evo level) and
    # the value is the column name (e.g. "Evo Knight") 

    card_types = dict()

    for card in card_data["items"] : 
        name = card["name"]
        id = card["id"]
        if "maxEvolutionLevel" in card : 
            evo_type = card["maxEvolutionLevel"]
        else : 
            evo_type = 0 #default
        
        card_types[(id, 0)] = f"{name}"  #Add default no matter what

        if evo_type == 1 : # Evo available (but no hero)
            card_types[(id, 1)] = f"Evo {name}"
        elif evo_type == 2 : # Hero available (but no evo)
            card_types[(id, 2)] = f"Hero {name}"
        elif evo_type == 3 : # Both evo and hero available
            card_types[(id, 1)] = f"Evo {name}" 
            card_types[(id, 2)] = f"Hero {name}"

    # Create one-hot columns from the card types
    OH_columns = ["Plr " + card_name for card_name in card_types.values()] + ["Opp " + card_name for card_name in card_types.values()]

    num_rows = df.shape[0]

    # Create one-hot dataframe (datatype as boolean)
    df_X = pd.DataFrame(
        data = np.zeros((num_rows, len(OH_columns)), dtype = bool),
        columns = OH_columns
        )
    # Brute force fill in the one-hot dataframe by going through each row of the data: 
    player_types = {"p" : "Plr", "o" : "Opp"}
    for row in trange(num_rows) : 
        for pt in player_types : 
            for card_i in range(8) : 
                lookup = (df.loc[row, f"{pt}_card_{card_i+1}"], df.loc[row, f"{pt}_card_{card_i+1}_evohero"])
                if lookup not in card_types: #card not available (e.g. 4-card games, a card that was very recently introduced, or a limited-time card)
                    continue
                df_X.loc[row, f"{player_types[pt]} {card_types[lookup]}"] = True 

    # Save the X dataframe as a parquet file 
    file_date = parquet_raw_files[0].split(".")[0]
    X_path = parquet_X_dir / f"{file_date}_X.parquet "
    df_X.to_parquet(path = X_path, engine = "pyarrow", compression = "zstd", index = False)

print("Finished conversion")


# %%
