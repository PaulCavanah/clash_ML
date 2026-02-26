# For model training, there is X and Y data required 
# In this case, X is a one-hot matrix of all cards, including evo/hero versions
# This function converts the relevant columns from the parquet file to X

#%%
from pathlib import Path
import pandas as pd 
import os 
import sys
import requests
import json

#Apt
#TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjNiZjBiMzZiLTVlMzQtNGFhZi05MTNkLWUwZmJhMzFiYjJkYyIsImlhdCI6MTc2OTMxNjAwOSwic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyI3Ni4zNy4yNDkuMTU2Il0sInR5cGUiOiJjbGllbnQifV19.c_Z8GSEGA6i8iwWTV6ZUrOVZAVTkGn6vmzPafa6GJRqEMEgyQSlEjzSJnkuPz689Udd2JZUAih0FbgzcmAGRhg"

#K apt 
#TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjY4MmNjOGNmLTcxMzktNDNmOS1hZmM5LWM1NzgyYjAwMGJlNSIsImlhdCI6MTc3MTAyNjgwMywic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyI3Ni4zNy4yNTAuMTgiXSwidHlwZSI6ImNsaWVudCJ9XX0.8-Fwy4Unu3TQFOn4opxt_A9r2bOxu1VOOUY6jvqV1QcciJdjNCjK6IOPVm0T7vKvqvWYjfR7GhnYJoGtSri3fQ"

#Hospital
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjBiODkwMjBkLTY4YWMtNDhkYS05NTcwLTEyZTllNzFhM2NlOCIsImlhdCI6MTc2OTM2NjQwNCwic3ViIjoiZGV2ZWxvcGVyLzhjNzUyNDE0LWIzNzItYjc4Ny0zOTk0LTZlMGExZWEzN2RmZCIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyIxMjguMTUxLjcxLjIzIl0sInR5cGUiOiJjbGllbnQifV19._7in6Y-a-cSSGzzXBjMZB0HZaRO-3qIjxCs9cphtEJ0weFyuCXinSyxqcgtgP7LFrCQMJc4dIC1ncAJXq1ynXA"

parquet_dir = Path(os.getcwd() + "/data/parquet")

# Data = card information (id and evo/hero status) for each game
data_columns = [f"p_card_{i+1}" for i in range(8)] \
+ [f"o_card_{i+1}" for i in range(8)] \
+ [f"p_card_{i+1}_evohero" for i in range(8)] \
+ [f"o_card_{i+1}_evohero" for i in range(8)] 

# %%

# Read only data required for X 
df = pd.read_parquet(path = parquet_dir, engine = "pyarrow", columns = data_columns)
#print(df.dtypes, sys.getsizeof(df))

# %%

# Load card names from cards json or API call (the same thing, but cards json can be used for debugging/testing in certain situations where API can't)
card_get_method = "JSON" # "JSON" or "API"
if card_get_method == "API" : 
    url = f"https://api.clashroyale.com/v1/cards"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers = headers)
    card_data = r.json()
elif card_get_method == "JSON": 
    json_path = os.getcwd() + "/json/" + "Cards.json"
    with open(json_path, 'r') as file : 
        card_data = json.load(file) 

# %%
cards = card_data["items"]
# Three types of cards: default, evolution, and hero
card_columns = card_data["items"]



# %%
