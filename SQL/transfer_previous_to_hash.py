# Before the hash db was implemented, there were a 
# few hundred CSVs that weren't put in the db (because it didn't exist).
# Now that it has been created, here is the place to put in that data.

# %%
import psycopg2 
import pandas as pd
from pathlib import Path
import glob
import os 
import tqdm
import hashlib
import base64

#%%
# Load in un-databased, raw CSV data
raw_dir = Path(os.getcwd() + "/../data/raw_data") #assuming this script is in /SQL

raw_data_names = [filepath.name for filepath in raw_dir.glob("*.csv")]
df_list = []
for data_filepath in raw_data_names : 
    df = pd.read_csv(raw_dir / data_filepath)
    df_list.append(df)

df = pd.concat(df_list, ignore_index = True)

#%%
# Creating table for battles : 

# Connect to hash db: 
conn = psycopg2.connect(
    host = "localhost",
    database = "hash_db",
    user = "postgres",
    password = "Onetwothree123!",
    port = "5432"
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())

# Create table if it doesn't exist:
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

cur.close()
conn.close()


#%% 

# Connect to hash db: 
conn = psycopg2.connect(
    host = "localhost",
    database = "hash_db",
    user = "postgres",
    password = "Onetwothree123!",
    port = "5432"
)

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
# The above query inserts the hash of the battle (battle_id, as primary key), along with
# other info, and if the primary key is already in there, it doesn't execute the insert

cur = conn.cursor()

num_bytes = 12 

num_rows = df.shape[0]
# Execute the insert query for every battle in the dataframe 
for row_ii in tqdm.trange(num_rows) : #tqdm = progress bar
    if row_ii % 10000 == 0 : #every 10,000 rows, commit the insertions
        conn.commit()
    # Get the row data
    player_tag, opponent_tag, game_time, player_crowns, opponent_crowns = df.loc[row_ii, ["player_tag", "opponent_tag", "game_time", "player_crowns", "opponent_crowns"]]
    # Get whether player won
    player_win = bool(player_crowns >= opponent_crowns)
    # Get hash 
    ordered_tags = sorted((player_tag, opponent_tag)) # this is crucial so that player/opponent swaps don't affect hash
    input_str = f"{game_time}|{ordered_tags[0]}|{ordered_tags[1]}".encode("utf-8")
    digest = hashlib.blake2b(input_str, digest_size = num_bytes).digest()
    battle_id = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    row_data = [
        battle_id,
        game_time,
        player_tag,
        opponent_tag,
        player_win
    ]

    cur.execute(insert_query, row_data)

conn.commit()

cur.close()
conn.close()

#%% 
# Run a test with a battle to get whether it exists in the database :

row_ii = 10000
# Get the row data
player_tag, opponent_tag, game_time, player_crowns, opponent_crowns = df.loc[row_ii, ["player_tag", "opponent_tag", "game_time", "player_crowns", "opponent_crowns"]]
# Get whether player won
player_win = bool(player_crowns >= opponent_crowns)
# Get hash 
ordered_tags = sorted((player_tag, opponent_tag)) # this is crucial so that player/opponent swaps don't affect hash
input_str = f"{game_time}|{ordered_tags[0]}|{ordered_tags[1]}".encode("utf-8")
digest = hashlib.blake2b(input_str, digest_size = num_bytes).digest()
battle_id = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

conn = psycopg2.connect(
    host = "localhost",
    database = "hash_db",
    user = "postgres",
    password = "Onetwothree123!",
    port = "5432"
)

cur = conn.cursor()

battle_exist_query = """
    SELECT EXISTS (
        SELECT 1
        FROM battles
        WHERE battle_id = %s
    );
"""

cur.execute(battle_exist_query, (battle_id, ))
battle_in_database = cur.fetchone()[0]
print(battle_in_database)

# %%
