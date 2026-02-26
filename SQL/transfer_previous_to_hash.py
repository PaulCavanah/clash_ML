# Before the hash db was implemented, there were a 
# few hundred CSVs that weren't put in the db (because it didn't exist).
# Now that it has been created, here is the place to put in that data.

# %%
import psycopg2 

# Try to connect: 
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

# Create hash table if it doesn't exist:
create_table_query = """
CREATE TABLE IF NOT EXISTS battles (
    battle_id TEXT PRIMARY KEY,
    battle_time TIMESTAMP NOT NULL,
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