
# Produces CSV files with battle data in them

# The approach is very simple - query a predetermined list of high ladder and ranked players (compiled from previously collected data).
# Importantly, the index in the list needs to be kept track of and saved every time a CSV file is saved. 

# Importantly, unlike previous versions, this collector system does not keep track of previous battles,
# since doing so does not scale as well as simply making sure they do not collide at the
# CSV -> parquet stage. Thus, there will be some redundant data in the CSV files produced by this collector.

# Data that are included :
# Player tag 
# Player starting trophies
# Player crowns 
# Player king tower health remaining
# Player support towers 1 and 2 health remaining (separate columns)
# Player support tower level
# Opponent tag 
# Opponent starting trophies
# Opponent crowns
# Opponent support towers 1 and 2 health remaining (separate columns)
# Opponent support tower level 
# Gamemode 
# Datetime for game
# Player card IDs (8 columns)
# Player card levels (8 columns)
# Player card evo/hero status (8 columns) (0 = default, 1 = evo, or 2 = hero)
# Player support tower ID
# Opponent card IDs (8 columns)
# Opponent card levels (8 columns)
# Opponent card evo/hero status (8 columns) (0 default, 1 = evo, or 2 = hero)
# Opponent support tower ID 

#%%
# Imports 
import os 
import pandas as pd 
import datetime
import requests
from pathlib import Path
from functions.get_API_token import get_API_token
import sys
import pickle 

# Get API token
TOKEN = get_API_token()
if TOKEN == None : 
    sys.exit()

# Make clash_ML (root) the current directory and add it to path
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = Path("\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]]))
os.chdir(root_dir)
sys.path.append(os.getcwd())

# Directory where csv will be saved
data_dir = root_dir / "data/raw_data/"
data_dir.mkdir(parents = True, exist_ok = True)

num_battle_limit = 10000 #number of battles to collect for each cycle before saving as CSV

# Load collector number
collector_num_path = Path(root_dir / "data/collector.txt")
if os.path.isfile(collector_num_path) : 
    with open(collector_num_path, "r") as file : # collector.txt contains one value - the number of collector on this machine (e.g. "0")
        collector = file.read()
else : 
    raise(Exception("No collector.txt file found"))
print("Collector: ", collector)

# Load the list for this collector
list_name = "20260609_highskill"
list_path = root_dir / f"player_list/lists/collector_{collector}_{list_name}.pkl"
if os.path.isfile(list_path) : 
    with open(list_path, "rb") as file : 
        player_list = pickle.load(file)
else : 
    raise(Exception("No list found at list path"))
print("Loaded list of ", len(player_list), " players")
    
# Load the last saved index for the list
last_index_path = root_dir / "data/last_index.txt"
if os.path.isfile(last_index_path) : 
    with open(last_index_path, "r") as file :
        last_index = int(file.read())
else : 
    with open(last_index_path, "w") as file : 
        last_index = 0 
        file.write(str(last_index))
print("Starting position in queue: ", last_index)

#%%
#lambda function to reformat raw tags to be queried by API
tag_reformat = lambda raw_id: "%23" + raw_id[1:]

row_list = [] # list of games, formatted as dictionaries of data

while True : 

    try : 
        # Establish naming for this data collection cycle: 
        collection_cycle_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S") #later = always greater

        # Initiate data collection cycle
        while len(row_list) <= num_battle_limit : 
            
            current_player_tag = tag_reformat(player_list[last_index])
            print(f"current player: {current_player_tag}", f"index: {last_index}")

            # API call - get all battle log data from player
            url = f"https://api.clashroyale.com/v1/players/{current_player_tag}/battlelog" 
            headers = {"Authorization": f"Bearer {TOKEN}"}
            r = requests.get(url, headers = headers)
            battle_data  = r.json() # json -> dict
            
            # Process the data into the columnar format 
            for battle in battle_data : 

                # Get support tower health from json output: 
                p_supports = battle["team"][0]["princessTowersHitPoints"] # comes as None if empty, len 1 if one tower destroyed, or len 2 if both towers up
                p_support_1, p_support_2 = ((p_supports or []) + [0, 0])[:2] # cool padding + unpacking trick
                o_supports = battle["opponent"][0]["princessTowersHitPoints"] # comes as None if empty, len 1 if one tower destroyed, or len 2 if both towers up 
                o_support_1, o_support_2 = ((o_supports or []) + [0, 0])[:2] # cool padding + unpacking trick

                # Get support tower levels from json, and convert to max level 16 (from whatever rarity level)
                try : 
                    p_support_level = battle["team"][0]["supportCards"][0]["level"] + (16 - battle["team"][0]["supportCards"][0]["maxLevel"])
                    o_support_level = battle["opponent"][0]["supportCards"][0]["level"] + (16 - battle["opponent"][0]["supportCards"][0]["maxLevel"])
                except IndexError: #occurs when support towers are not listed - definitely not a ranked or ladder match
                    print(f"Skipping battle of type {battle["gameMode"]["name"]}")
                    continue

                # Setup columns: 
                new_row = {
                    "player_tag" : battle["team"][0]["tag"],
                    "player_trophies" : 0,
                    "player_crowns" : battle["team"][0]["crowns"],
                    "opponent_tag" : battle["opponent"][0]["tag"],
                    "opponent_trophies" : 0,
                    "opponent_crowns" : battle["opponent"][0]["crowns"],
                    "gamemode" : battle["gameMode"]["name"],
                    "game_time" : battle["battleTime"],
                    "p_king_health" : battle["team"][0]["kingTowerHitPoints"],
                    "p_support_1_health" : p_support_1,
                    "p_support_2_health" : p_support_2, 
                    "p_support_level" : p_support_level,
                    "o_king_health" : battle["opponent"][0]["kingTowerHitPoints"],
                    "o_support_1_health" : o_support_1, 
                    "o_support_2_health" : o_support_2,
                    "o_support_level" : o_support_level,
                } \
                | {f"p_card_{i+1}" : "" for i in range(8)} \
                | {f"p_card_{i+1}_level" : 0 for i in range(8)} \
                | {f"p_card_{i+1}_evohero" : 0 for i in range(8)} \
                | {f"p_tower" : battle["team"][0]["supportCards"][0]["id"]} \
                | {f"o_card_{i+1}" : "" for i in range(8)} \
                | {f"o_card_{i+1}_level" : 0 for i in range(8)} \
                | {f"o_card_{i+1}_evohero" : 0 for i in range(8)} \
                | {f"o_tower" : battle["opponent"][0]["supportCards"][0]["id"]}

                # Get card information for player and opponent decks 
                player_deck = [card for card in battle["team"][0]["cards"]]
                opponent_deck = [card for card in battle["opponent"][0]["cards"]]
                if len(player_deck) > 8 or len(opponent_deck) > 8 : #don't process the battle if decks are > 8 cards for whatever reason
                    continue 

                if "startingTrophies" in battle["team"][0] : 
                    new_row["player_trophies"] = battle["team"][0]["startingTrophies"]
                if "startingTrophies" in battle["opponent"][0] : 
                    new_row["player_trophies"] = battle["opponent"][0]["startingTrophies"]
                
                # Assign deck information to columns (card id, card level, and whether it's default, evo, or hero): 
                for i in range(len(player_deck)) : 
                    # Player : 
                    new_row[f"p_card_{i+1}"] = player_deck[i]["id"]
                    new_row[f"p_card_{i+1}_level"] = player_deck[i]["level"] + (16 - player_deck[i]["maxLevel"])
                    if "maxEvolutionLevel" in player_deck[i] and "evolutionLevel" in player_deck[i]:
                        new_row[f"p_card_{i+1}_evohero"] = player_deck[i]["evolutionLevel"]
                    else : 
                        new_row[f"p_card_{i+1}_evohero"] = 0 
                    # Opponent : 
                    new_row[f"o_card_{i+1}"] =  opponent_deck[i]["id"]
                    new_row[f"o_card_{i+1}_level"] = opponent_deck[i]["level"] + (16 - opponent_deck[i]["maxLevel"])
                    if "maxEvolutionLevel" in opponent_deck[i] and "evolutionLevel" in opponent_deck[i]:
                        new_row[f"o_card_{i+1}_evohero"] = opponent_deck[i]["evolutionLevel"]
                    else : 
                        new_row[f"o_card_{i+1}_evohero"] = 0 

                # Append to rows list
                row_list.append(new_row)

                print("battle time: " + new_row["game_time"], "player id: " + new_row["player_tag"], "gamemode: " + new_row["gamemode"], "row num: " + str(len(row_list)))

            # Move forward one in the player queue
            last_index += 1 
            if last_index > len(player_list) : 
                last_index = 0 

        # Save the rows as a csv with a unique timestamp: 
        df = pd.DataFrame(row_list)
        df.to_csv(data_dir / f"{collection_cycle_timestamp}.csv")

        # Save the last index for the player queue 
        with open(last_index_path, "w") as file : 
            file.write(str(last_index))

        row_list = []
        local_hash_set = set()

    except Exception as e: 
        print(e) 
        TOKEN = get_API_token()


# %%
