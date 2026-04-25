
# Given an input deck, find a deck that counters it well
# Initial approach: brute force search iterating over deck slots and cards, with multiple passes

# What to do now: 
# In summary - CONDENSE and modularize this process for future 
# Components right now: 
# 1. Imports
# 2. Get feature names from model
# 3. Get base, evos, hero/champion column ids from feature names
# 4. Get collisions of column ids 
# 5. Load model
# 6. Set player deck / column indices of player cards
# 7. Run algorithm
# 8. Evaluate result

# All of this is too jumbled 

# Ultimately, I should have a script or process where I just put in a deck with
# levels, and I choose from some options:  
# 1. choose a player tag and these are the cards to choose from
# 2. choose any cards
# 3. choose only base cards
# 4. etc... 

#%% 
# Imports and finding root dir
import torch
import torch.nn as nn
import torch.nn.functional as F
import os 
import pandas as pd
import numpy as np

# Set root dir as cwd
enum = [(i, dir) for i, dir in enumerate(os.getcwd().split("\\"))]
root_dir = "\\".join([dir for i, dir in enum if i <= [i for i, dir in enum if dir == "clash_ML"][0]])
os.chdir(root_dir)

# Specify model architecture and name, then load model
from modeling.architectures import *
architecture = LogitSymmetric_256_128_64_1

model_name = "NNsym_22M_ladder5k10k_levels"

from functions.load_model import load_nn_model
model, feature_names = load_nn_model(architecture, model_name)

# Get available cards from a player tag
from functions.get_cards import get_player_cards

tag = "%2389QUL8YCQ" # Some random ladder player




#%%

# column indices of available cards (columns of feature_names, i.e. model input)
base = [] 
evos = [] 
hero = [] # includes heros and champions
opp_half = len(feature_names) // 2
feature_collisions = {i : [] for i in range(opp_half, len(feature_names))} # column id : [column ids that collide]

for i_, feature in enumerate(feature_names[opp_half:]) : # only opp columns matter here since player deck is constant for this algorithm
    i = i_ + opp_half
    feature_split = feature.split(" ") # e.g. ["Opp", "Hero", "Mega", "Minion"]
    split_len = len(feature_split) # number of words in the split (e.g. 4)

    if "Evo" in feature_split : 
        evos.append(i)
        name_start = 2 #index of name start 
    elif "Hero" in feature_split or " ".join(feature_split[1:]) in ["Skeleton King", "Archer Queen", "Goblinstein", "Golden Knight", "Little Prince", "Mighty Miner", "Monk", "Boss Bandit"]: 
        hero.append(i)
        name_start = 2
    else : 
        base.append(i)
        name_start = 1

    base_name = " ".join(feature_split[name_start:]) # E.g. "Mega Minion"

    # Get collisions for this feature
    for j_, feature_j in enumerate(feature_names[opp_half:]) : 
        j = j_ + opp_half
        feature_split_j = feature_j.split(" ")[1:] # Get rid of "Opp" 
        if "Evo" in feature_split_j or "Hero" in feature_split_j :
            feature_split_j.pop(0) # Get rid of "Evo" or "Hero"
        if base_name == " ".join(feature_split_j) : # Only the base name remains
            feature_collisions[i].append(j)


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
        #cards_to_search = [b for b in base if b not in cards_in_deck]

        if slot == 0 or slot == 2 : # evo slot 
            cards_to_search = [evo for evo in evos if evo not in cards_in_deck]
        elif slot == 1 : # hero/champion slot
            cards_to_search = [h for h in hero if h not in cards_in_deck]
        else : # base card slot
            cards_to_search = [b for b in base if b not in cards_in_deck]

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
