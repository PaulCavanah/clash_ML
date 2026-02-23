#%% 
import pickle
import os 
import shutil
import datetime

date_reset = True 

custom_date_limit = False

state_archive_path = os.getcwd() + "/data/search_test_3/state_archive/"
if not os.path.isdir(state_archive_path) : 
    os.makedirs(state_archive_path)

states_path = os.getcwd() + "/data/search_test_3/states/"

prev_states = list([name[0] for name in [file.split(".") for file in os.listdir(states_path)] if name[1] == "pkl"])
if len(prev_states) > 0 : #Load most recent previous state
    most_recent_state = max(prev_states)
    pkl_path = f"{states_path}{most_recent_state}.pkl"
    with open(pkl_path, "rb") as file : 
        state = pickle.load(file)
    # Move most recent to state archive : 
    shutil.move(src = pkl_path, dst = f"{state_archive_path}{most_recent_state}.pkl")

# Remove all players in queue after first 100 
state["player_queue"] = state["player_queue"][0:100]
# Refresh player set 
state["player_set"] = set()
# Battle set should remain the same UNLESS date rule is enforced

if date_reset : 
    # reset battle set (old battles cannot possibly be included again) 
    old_dl = state["battle_set"] = set()

    old_dl = state["date_limit"]
    state["date_limit"][0] = old_dl[1] + 1
    dt = datetime.datetime.now()
    dt = int(dt.strftime("%Y%m%d"))
    state["date_limit"][1] = dt 

    #E.g. if today is 20260213, [20260208, 20260210] -> [20260211, 20260213]

if custom_date_limit : 
    state["date_limit"] = custom_date_limit 

print(f"new state date limit: {state['date_limit']}")

# Save 
dt = datetime.datetime.now()
timestamp = dt.strftime("%Y%m%d%H%M%S") #later = always greater
pkl_path = f"{states_path}{timestamp}.pkl"
with open(pkl_path, 'wb') as file : 
    pickle.dump(state, file) 

#%%

