
# Given an input deck, find a deck that counters it pretty well
# Initial approach: brute force search iterating over deck slots and cards, with multiple passes

#%% 
# Imports and finding root dir
import torch
import torch.nn as nn
import torch.nn.functional as F
import os 
from pathlib import Path
import pandas as pd
import numpy as np

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

from modeling.architectures import *
architecture = LogitSymmetric_256_128_64_1

#%%
# Get feature labels and parse opponent card types (base/evo/hero)
import pickle 
model_name = "NNsym_28M_rankedladder"

features_path = root_dir + f"/modeling/model_features/features_{model_name}.pkl"
with open(features_path, "rb") as file :
   feature_names = pickle.load(file)
print(f"features ({len(feature_names)}): ", feature_names)

# column indices of cards 
base = [] 
evos = [] 
hero = [] # includes heros and champions
feature_splits = []
for i, feature in enumerate(feature_names) : 
    feature_split = feature.split(" ")
    if "Opp" in feature_split : 
        if "Evo" in feature_split : 
            evos.append(i)
        elif "Hero" in feature_split or " ".join(feature_split[1:]) in ["Skeleton King", "Archer Queen", "Goblinstein", "Golden Knight", "Little Prince", "Mighty Miner", "Monk", "Boss Bandit"]: 
            hero.append(i)
        else : 
            base.append(i)
    feature_splits.append(feature_split)

#%%
# get dictionary of for which feature index, other features indices collide (e.g. Knight collides with Evo Knight and Hero Knight)
feature_name_collisions = dict()
for i, feature_split_i in enumerate(feature_splits) : 
    if i not in feature_name_collisions : 
        feature_name_collisions[i] = []
    for j, feature_split_j in enumerate(feature_splits) :
        if i == j : 
            continue 

        if ("Plr" in feature_split_i and "Plr" in feature_split_j) or ("Opp" in feature_split_i and "Opp" in feature_split_j): 
            if "Evo" in feature_split_i or "Hero" in feature_split_i : 
                name_i = " ".join(feature_split_i[2:])
            else : 
                name_i = " ".join(feature_split_i[1:])

            if "Evo" in feature_split_j or "Hero" in feature_split_j : 
                name_j = " ".join(feature_split_j[2:])
            else : 
                name_j = " ".join(feature_split_j[1:])

            if name_i == name_j : 
                feature_name_collisions[i].append(j)

print(feature_name_collisions)

#%% 
# load model from architecture/state
state_path = Path(os.getcwd() + f"/modeling/model_states/{model_name}.pth")
model = architecture(input_dim = 340).to("cpu")
state_dict = torch.load(state_path, weights_only = False)
model.load_state_dict(state_dict())
model.eval()
print("num parameters: ", sum(param.numel() for param in model.parameters()))

#%%
# Set player input deck
input_deck = [
"Plr Battle Ram",
"Plr Wizard",
"Plr Lumberjack",
"Plr Barbarian Barrel",
"Plr Dark Prince",
"Plr Fireball",
"Plr Zappies",
"Plr Giant Skeleton"
""
]
# Get column indices of player cards
player_ind = [ind for ind, feature in enumerate(feature_names) if feature in input_deck]

#%% 
# Set parameters for algorithm

num_cycles = 3
num_slots = 8 

#%% 
# Run algorithm

# Construct tensor for model input
input_size = len(feature_names)
tens_in = torch.tensor(np.zeros((1, input_size)), dtype = torch.float32)
tens_in[0, player_ind] = 1 # tensor needs to be 2D to match architecture (which is trained on batches of data)

cards_in_deck = np.zeros([num_slots], dtype = np.float32) # indices of cards currently in the deck after iteration and selection 
cards_in_deck[:] = np.nan 

name_collisions = [] # e.g. hero knight collides with evo and base knight

slot_logit = np.zeros((num_slots, input_size), dtype = np.float32) # updated every slot/pass iteration
slot_logit[:] = np.nan 

for cycle in range(num_cycles) : 
    for slot in range(num_slots) : 
        if not np.isnan(cards_in_deck[slot]) : 
            tens_in[0, int(cards_in_deck[slot])] = 0 # current card removed in tensor during search

        # Getting cards to search for the slot
        cards_to_search = [b for b in base if b not in cards_in_deck]

        # if slot == 0 or slot == 2 : # evo slot 
        #     cards_to_search = [evo for evo in evos if evo not in cards_in_deck]
        # elif slot == 1 : # hero/champion slot
        #     cards_to_search = [h for h in hero if h not in cards_in_deck]
        # else : # base card slot
        #     cards_to_search = [b for b in base if b not in cards_in_deck]

        for card in cards_to_search :
            tens_in[0, card] = 1 # set slot to card 
            slot_logit[slot, card] = model(tens_in) # evaluate
            tens_in[0, card] = 0 # empty slot when done
            
        cards_in_deck[slot] = np.nanargmin(slot_logit[slot, :])
        #name_collisions = [collision for collision in [feature_name_collisions[card] for card in cards_in_deck]]
        tens_in[0, int(cards_in_deck[slot])] = 1 # set current card in tensor to the minimum

        slot_logit[slot, :] = np.nan # reset logits for next cycle


#%% 
# Look at deck output
features_np = np.array(feature_names)
print(features_np[cards_in_deck.astype(np.int16)])

# PROBLEM - handle duplicate cards (e.g. Evo Wizard, base wizard in same deck)

# %%
# Look at estimated win probability
test_tens = torch.tensor(np.zeros((1, input_size)), dtype = torch.float32)
test_tens[0, player_ind] = 1
test_tens[0, cards_in_deck.astype(np.int16)] = 1
print(F.sigmoid(model(test_tens)))

# %%
print(cards_in_deck)
# %%
print([collision for collisions in [feature_name_collisions[card] for card in cards_in_deck] for collision in collisions])

# %%
