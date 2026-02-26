
# Produces CSV files with battle data in them

# How this script keeps track of previous battles/players: 
# - Load in player queue, player set, and battle set (if they exist)
#   - If they don't exist, start with Mohamed Light's player tag
#   - Else, start with first player tag in the player queue 
# - For each tag in the queue, query API for its battle log. Do the following for each game: 
#   - If opponent is not in player set: 
#       - Add opponent tag to player set
#       - Add opponent tag to player queue 
#   - If battle is not in battle set: 
#       - Add battle to battle set 
#       - Record some metadata and the player/opponent deck information (see below) in a dict 
#       - Append that dict to a growing list (row list)
# - Every certain number of battles: 
#   - Make df from the row list, then save to disk (csv) and flush the row list 
#   - Save the player queue, player set, and battle set 
#   - The above (csv, player queue, sets ...) will be saved with same datetime/timestamp
#   - This makes it so that the "state" of the data collection is saved at every cycle along with the data

# All relevant data are included. 

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
# Player card evo/hero status (0 = default, 1 = evo, or 2 = hero)
# Player support tower name
# Opponent card IDs (8 columns)
# Opponent card levels (8 columns)
# Opponent card evo/hero status (0 default, 1 = evo, or 2 = hero)
# Opponent support tower name 

#Apt
#TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjNiZjBiMzZiLTVlMzQtNGFhZi05MTNkLWUwZmJhMzFiYjJkYyIsImlhdCI6MTc2OTMxNjAwOSwic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyI3Ni4zNy4yNDkuMTU2Il0sInR5cGUiOiJjbGllbnQifV19.c_Z8GSEGA6i8iwWTV6ZUrOVZAVTkGn6vmzPafa6GJRqEMEgyQSlEjzSJnkuPz689Udd2JZUAih0FbgzcmAGRhg"

#K apt 
#TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjY4MmNjOGNmLTcxMzktNDNmOS1hZmM5LWM1NzgyYjAwMGJlNSIsImlhdCI6MTc3MTAyNjgwMywic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyI3Ni4zNy4yNTAuMTgiXSwidHlwZSI6ImNsaWVudCJ9XX0.8-Fwy4Unu3TQFOn4opxt_A9r2bOxu1VOOUY6jvqV1QcciJdjNCjK6IOPVm0T7vKvqvWYjfR7GhnYJoGtSri3fQ"

#Hospital
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjBiODkwMjBkLTY4YWMtNDhkYS05NTcwLTEyZTllNzFhM2NlOCIsImlhdCI6MTc2OTM2NjQwNCwic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyIxMjguMTUxLjcxLjIzIl0sInR5cGUiOiJjbGllbnQifV19._7in6Y-a-cSSGzzXBjMZB0HZaRO-3qIjxCs9cphtEJ0weFyuCXinSyxqcgtgP7LFrCQMJc4dIC1ncAJXq1ynXA"

#%% 
import os 
import pandas as pd 
import datetime
import json 
import pickle
import requests

num_battle_limit = 10000 #number of battles to collect for each cycle before saving

import pickle
# Load data collection state information (player queue, player set, battle set) from pickle files
states_path = os.getcwd() + "/data/states/"
if not os.path.isdir(states_path) : 
    os.makedirs(states_path)

default_date_limit = [20260223, int(datetime.datetime.now().strftime("%Y%m%d"))] #only take battles between (including) these dates (alternative to building a battle set across every day, which would consume so much memory)

prev_states = list([name[0] for name in [file.split(".") for file in os.listdir(states_path)] if name[1] == "pkl"])
if len(prev_states) > 0 : #Load most recent previous state
    most_recent_state = max(prev_states)
    pkl_path = f"{states_path}{most_recent_state}.pkl"
    with open(pkl_path, "rb") as file : 
        state = pickle.load(file)
else : # No previous states saved 
    seed_tag = "#G9YV9GR8R" #Mo Light - start at the top 
    state = {
        "player_queue" : [seed_tag], # Player tag queue (type list, until I see a reason to use queue type in this situation) 
        "player_set" : {seed_tag}, # Set of players, to prevent duplication of player tags
        "battle_set" : set(), # Set of battles, which consists of sets of player tag, opponent tag, and game time (to prevent duplication of games)
        "date_limit" : default_date_limit # Date limits for when data were collected
    }

# Directory where csv will be saved
data_path = os.getcwd() + "/data/raw_data/"
if not os.path.isdir(data_path) : 
    os.makedirs(data_path)

#%%
#lambda function to reformat raw tags to be queried by API
reformat_id = lambda raw_id: "%23" + raw_id[1:]

row_list = [] 

while True : 

    try : 
        # Establish naming for this data collection cycle: 
        dt = datetime.datetime.now()
        collection_cycle_timestamp = dt.strftime("%Y%m%d%H%M%S") #later = always greater

        # Initiate data collection cycle
        while len(row_list) <= num_battle_limit : 

            # Get player who is first in queue
            current_player_tag = reformat_id(state["player_queue"].pop(0))

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
                    #print(f"Skipping battle of type {battle["gameMode"]["name"]}")
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

                # Don't process battle if it is outside the date limit
                if int(new_row["game_time"][0:8]) < state["date_limit"][0] or int(new_row["game_time"][0:8]) > state["date_limit"][1] : 
                    continue

                # Get card information for player and opponent decks 
                player_deck = [card for card in battle["team"][0]["cards"]]
                opponent_deck = [card for card in battle["opponent"][0]["cards"]]
                if len(player_deck) > 8 or len(opponent_deck) > 8 : #don't process the battle if decks are > 8 cards for whatever reason
                    continue 

                if "startingTrophies" in battle["team"][0] : 
                    new_row["player_trophies"] = battle["team"][0]["startingTrophies"]
                if "startingTrophies" in battle["opponent"][0] : 
                    new_row["player_trophies"] = battle["opponent"][0]["startingTrophies"]

                # Test whether this game occurred before using sets, and then update sets
                battle_metadata = frozenset({new_row["player_tag"], new_row["opponent_tag"], new_row["game_time"]})
                if battle_metadata in state["battle_set"] : #don't process the battle if it's already been collected before
                    continue 
                else : #if it's not in there, add it and keep going (the battle has been accepted)
                    state["battle_set"].add(battle_metadata)

                if new_row["opponent_tag"] not in state["player_set"] : #If opponent is new, add to queue
                    state["player_queue"].append(new_row["opponent_tag"])
                    state["player_set"].add(new_row["opponent_tag"])
                
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

                print(battle_metadata, len(row_list))

        # Save the rows as a dataframe: 
        df = pd.DataFrame(row_list)
        df.to_csv(f"{data_path}{collection_cycle_timestamp}.csv")

        # Save the state: 
        pkl_path = f"{states_path}{collection_cycle_timestamp}.pkl"
        with open(pkl_path, 'wb') as file : 
            pickle.dump(state, file) 

        row_list = []

    except Exception as e: 
        print(e) 

# %%
