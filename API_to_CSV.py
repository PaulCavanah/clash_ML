
# Produces CSV files with battle data in them

# How the collector keeps track of previous battles/players: 
# - Insert description about postgresql database 

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
# Player support tower ID
# Opponent card IDs (8 columns)
# Opponent card levels (8 columns)
# Opponent card evo/hero status (0 default, 1 = evo, or 2 = hero)
# Opponent support tower ID 

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
import requests
import psycopg2 
from pathlib import Path
import hashlib
import base64

# Directory where csv will be saved
data_dir = Path(os.getcwd() + "/data/raw_data/")
data_dir.mkdir(parents = True, exist_ok = True)

num_battle_limit = 10000 #number of battles to collect for each cycle before saving

num_hash_bytes = 12 #this always needs to be 12, or else the hash will be different

# Connect to hash db (for keeping track of battles and getting queue): 
conn = psycopg2.connect(
    host = "localhost",
    database = "hash_db",
    user = "postgres",
    password = "Onetwothree123!",
    port = "5432"
)

cur = conn.cursor()

# Create battles table if it doesn't exist:
create_table_query = """
CREATE TABLE IF NOT EXISTS battles (
    battle_id TEXT PRIMARY KEY,
    battle_time TEXT NOT NULL,
    player_tag TEXT NOT NULL,
    opponent_tag TEXT NOT NULL,
    player_win BOOLEAN
);
"""

cur.execute(create_table_query)
conn.commit()

# The below query inserts the hash of the battle (battle_id, as primary key), along with
# some other info. If the primary key is already in there, it doesn't execute the insert
insert_query = """
INSERT INTO battles (
    battle_id,
    battle_time,
    player_tag,
    opponent_tag,
    player_win
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (battle_id) DO NOTHING;
"""

# The below query checks if the hash of the battle (battle_id) exists
battle_exist_query = """
    SELECT EXISTS (
        SELECT 1
        FROM battles
        WHERE battle_id = %s
    );
"""

# The below query gets 10,000 unique opponent tags from the most recent battles
tag_select_query = """
SELECT DISTINCT battle_time, opponent_tag 
    FROM battles
    ORDER BY battle_time DESC
    LIMIT 10000;
"""

#%%
#lambda function to reformat raw tags to be queried by API
tag_reformat = lambda raw_id: "%23" + raw_id[1:]

tag_queue = [] # list of players to be queried on the API
row_list = [] # list of games, formatted as dictionaries of data

while True : 

    try : 
        # Establish naming for this data collection cycle: 
        collection_cycle_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S") #later = always greater

        # Initiate data collection cycle
        while len(row_list) <= num_battle_limit : 

            # Get player who is first in queue
            if len(tag_queue) > 0 :  
                current_player_tag = tag_reformat(tag_queue.pop(0))
            else : 
                print("Re-filling tag queue...")
                cur.execute(tag_select_query)
                tag_queue = [d[1] for d in cur.fetchall()]
                continue
            
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

                # Get battle_id (hash) 
                ordered_tags = sorted((new_row["player_tag"], new_row["opponent_tag"])) # this is crucial so that player/opponent swaps don't affect hash
                input_str = f"{new_row["game_time"]}|{ordered_tags[0]}|{ordered_tags[1]}".encode("utf-8")
                digest = hashlib.blake2b(input_str, digest_size = num_hash_bytes).digest()
                battle_id = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

                # Test whether this game occurred before using hash database
                cur.execute(battle_exist_query, (battle_id, ))
                battle_in_database = cur.fetchone()[0]
                if battle_in_database : # don't process this game if it's already present
                    print("Battle occurred before")
                    continue

                # Insert battle's data into db 
                db_insert_data = [battle_id, 
                                  new_row["game_time"],
                                  new_row["player_tag"],
                                  new_row["opponent_tag"], 
                                  bool(new_row["player_crowns"] >= new_row["opponent_crowns"])
                ]
                cur.execute(insert_query, db_insert_data)
                
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

        conn.commit() # Necessary so that the database insertions take place

        # Save the rows as a csv with a unique timestamp: 
        df = pd.DataFrame(row_list)
        df.to_csv(data_dir / f"{collection_cycle_timestamp}.csv")

        row_list = []

    except Exception as e: 
        print(e) 

# %%
